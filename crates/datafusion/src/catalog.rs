//! Iceberg の Catalog を DataFusion のカタログに対応づける。
//!
//! namespace が DataFusion のスキーマにあたる。テーブルの実体は毎回 core の Catalog から読むので、
//! 別のプロセスのコミットも次のクエリから見える。

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

/// namespace は暗黙に存在する。どの名前にもスキーマプロバイダを返し、その namespace は
/// テーブルを作ったときにストレージ上に現れる。
#[derive(Debug)]
pub struct IcebergCatalogProvider {
    catalog: Arc<Catalog>,
    schemas: Mutex<BTreeMap<String, Arc<IcebergSchemaProvider>>>,
}

impl IcebergCatalogProvider {
    /// DataFusion の `schema_names` と `table_exist` は同期関数なので、一覧はここで読んでおく。
    /// 他のプロセスがあとで行うコミットの影響は、この2つの結果に限られる。
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

    // 名前の接尾辞でテーブルの種類を決める。metadata はこの呼び出しのたびに読み直す。
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

    /// `CREATE EXTERNAL TABLE` は、ファクトリが Catalog にテーブルを作ったあとでこれを呼ぶので、
    /// 変わるのはローカルの一覧だけである。この Catalog は Iceberg のテーブルしか保存しないので、
    /// それ以外を受け入れると、登録したものが失われてしまう。
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

    /// 拒否する。`CREATE OR REPLACE` が既存のテーブルを失う前に失敗するようにするためである。
    /// `DROP TABLE` については [`crate::IcebergSession`] を参照。
    fn deregister_table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::NotImplemented(
            "replacing a table is not supported; run DROP TABLE through IcebergSession::sql".into(),
        ))
    }
}
