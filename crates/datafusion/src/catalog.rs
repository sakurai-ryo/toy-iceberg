use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::{DataFusionError, Result};
use toy_iceberg_core::Error;
use toy_iceberg_core::catalog::{Catalog, TableIdent};

use crate::inspect::metadata_table;
use crate::table::IcebergTable;
use crate::to_df;

/// Namespaces exist implicitly: a schema provider is handed out for any name, and the
/// namespace appears in the store once a table is created in it.
#[derive(Debug)]
pub struct IcebergCatalogProvider {
    catalog: Arc<Catalog>,
    schemas: Mutex<BTreeMap<String, Arc<IcebergSchemaProvider>>>,
}

impl IcebergCatalogProvider {
    /// DataFusion's `schema_names` and `table_exist` are synchronous, so the listing is read
    /// here. Commits that other processes make later only change what those two report.
    pub async fn load(catalog: Arc<Catalog>) -> Result<Self> {
        let mut schemas = BTreeMap::new();
        for namespace in catalog.list_namespaces().await.map_err(to_df)? {
            let tables = catalog.list_tables(&namespace).await.map_err(to_df)?;
            let provider = IcebergSchemaProvider::new(catalog.clone(), &namespace, tables);
            schemas.insert(namespace, Arc::new(provider));
        }
        Ok(IcebergCatalogProvider {
            catalog,
            schemas: Mutex::new(schemas),
        })
    }
}

impl IcebergCatalogProvider {
    pub(crate) fn forget(&self, ident: &TableIdent) {
        if let Some(schema) = self.schemas.lock().unwrap().get(&ident.namespace) {
            schema.tables.lock().unwrap().remove(&ident.name);
        }
    }
}

impl CatalogProvider for IcebergCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.lock().unwrap().keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let mut schemas = self.schemas.lock().unwrap();
        let provider = schemas.entry(name.to_owned()).or_insert_with(|| {
            Arc::new(IcebergSchemaProvider::new(
                self.catalog.clone(),
                name,
                vec![],
            ))
        });
        Some(provider.clone())
    }
}

#[derive(Debug)]
struct IcebergSchemaProvider {
    catalog: Arc<Catalog>,
    namespace: String,
    tables: Mutex<BTreeSet<String>>,
}

impl IcebergSchemaProvider {
    fn new(catalog: Arc<Catalog>, namespace: &str, tables: Vec<String>) -> Self {
        IcebergSchemaProvider {
            catalog,
            namespace: namespace.to_owned(),
            tables: Mutex::new(tables.into_iter().collect()),
        }
    }

    async fn load(&self, name: &str) -> Result<Option<toy_iceberg_core::table::Table>> {
        let ident = TableIdent::new(&self.namespace, name);
        match self.catalog.load_table(&ident).await {
            Ok(t) => Ok(Some(t)),
            Err(Error::TableNotFound(_)) => Ok(None),
            Err(e) => Err(to_df(e)),
        }
    }
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.tables.lock().unwrap().iter().cloned().collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some((base, kind)) = name.split_once('$') {
            let Some(table) = self.load(base).await? else {
                return Ok(None);
            };
            return metadata_table(&table, kind).await.map(Some);
        }
        if let Some((base, id)) = name.rsplit_once('@')
            && let Ok(id) = id.parse::<i64>()
        {
            let Some(table) = self.load(base).await? else {
                return Ok(None);
            };
            return Ok(Some(Arc::new(IcebergTable::at_snapshot(table, id))));
        }
        let table = self.load(name).await?;
        Ok(table.map(|t| Arc::new(IcebergTable::new(t)) as Arc<dyn TableProvider>))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.lock().unwrap().contains(name)
    }

    /// `CREATE EXTERNAL TABLE` calls this after the factory has already created the table in
    /// the catalog, so only the local listing changes. Anything else would be lost, because
    /// this catalog stores nothing but Iceberg tables.
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if !(table.as_ref() as &dyn Any).is::<IcebergTable>() {
            return Err(DataFusionError::NotImplemented(
                "only CREATE EXTERNAL TABLE ... STORED AS ICEBERG can create tables here".into(),
            ));
        }
        self.tables.lock().unwrap().insert(name);
        Ok(None)
    }

    /// Refuses, so that `CREATE OR REPLACE` fails before it can lose the existing table.
    /// See [`crate::IcebergSession`] for `DROP TABLE`.
    fn deregister_table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::NotImplemented(
            "replacing a table is not supported; run DROP TABLE through IcebergSession::sql".into(),
        ))
    }
}
