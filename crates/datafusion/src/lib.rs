//! DataFusion integration for `toy-iceberg-core`.
//!
//! Tables resolve through a catalog named `iceberg`. Besides plain table names, the schema
//! provider understands `"t@<snapshot-id>"` for time travel and `"t$snapshots"`,
//! `"t$manifests"` and `"t$files"` for metadata tables.

mod catalog;
mod filter;
mod inspect;
mod table;

use std::ops::Deref;
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{DdlStatement, DropTable, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use toy_iceberg_core::catalog::{Catalog, TableIdent};

pub use catalog::IcebergCatalogProvider;
pub use table::IcebergTableFactory;

pub const CATALOG_NAME: &str = "iceberg";
pub const DEFAULT_NAMESPACE: &str = "default";

/// A `SessionContext` whose `sql` also runs `DROP TABLE`.
///
/// DataFusion reaches `SchemaProvider::deregister_table` from both `DROP TABLE` and the first
/// step of `CREATE OR REPLACE`, so the provider cannot tell a drop from a replacement that may
/// still fail. The provider refuses to deregister, and drops are handled here instead.
pub struct IcebergSession {
    ctx: SessionContext,
    catalog: Arc<Catalog>,
    provider: Arc<IcebergCatalogProvider>,
}

impl Deref for IcebergSession {
    type Target = SessionContext;

    fn deref(&self) -> &SessionContext {
        &self.ctx
    }
}

impl IcebergSession {
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let plan = self.ctx.state().create_logical_plan(sql).await?;
        match &plan {
            LogicalPlan::Ddl(DdlStatement::DropTable(cmd)) => self.drop_table(cmd).await,
            // Namespaces exist implicitly, and DataFusion would report this one as a duplicate.
            LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(_)) => self.ctx.read_empty(),
            _ => self.ctx.execute_logical_plan(plan).await,
        }
    }

    async fn drop_table(&self, cmd: &DropTable) -> Result<DataFrame> {
        let state = self.ctx.state();
        let defaults = &state.config_options().catalog;
        let name = cmd
            .name
            .clone()
            .resolve(&defaults.default_catalog, &defaults.default_schema);
        let ident = TableIdent::new(name.schema.as_ref(), name.table.as_ref());
        match self.catalog.drop_table(&ident).await {
            Ok(()) => self.provider.forget(&ident),
            Err(toy_iceberg_core::Error::TableNotFound(_)) if cmd.if_exists => {}
            Err(e) => return Err(to_df(e)),
        }
        self.ctx.read_empty()
    }
}

pub async fn session(catalog: Arc<Catalog>) -> Result<IcebergSession> {
    let mut config = SessionConfig::new()
        .with_default_catalog_and_schema(CATALOG_NAME, DEFAULT_NAMESPACE)
        .with_create_default_catalog_and_schema(false);
    // SQL `TIMESTAMPTZ` plans as a zone-less timestamp unless the session has a time zone.
    config.options_mut().execution.time_zone = Some("UTC".to_owned());
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_table_factory(
            "ICEBERG".into(),
            Arc::new(IcebergTableFactory::new(catalog.clone())),
        )
        .build();
    let ctx = SessionContext::new_with_state(state);
    let provider = Arc::new(IcebergCatalogProvider::load(catalog.clone()).await?);
    ctx.register_catalog(CATALOG_NAME, provider.clone());
    Ok(IcebergSession {
        ctx,
        catalog,
        provider,
    })
}

pub(crate) fn to_df(e: toy_iceberg_core::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}
