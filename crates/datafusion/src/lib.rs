//! `toy-iceberg-core` と DataFusion の接続。
//!
//! Iceberg の仕様はクエリエンジンを定めていない。このクレートは core の API を
//! DataFusion の trait に対応づけるだけで、テーブルフォーマットの処理は core が行う。
//!
//! テーブルは `iceberg` という名前のカタログで解決する。通常のテーブル名のほか、次の名前を解決する。
//!
//! - `"t@<snapshot-id>"`: タイムトラベル
//! - `"t$snapshots"`、`"t$manifests"`、`"t$files"`: メタデータテーブル

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

/// `sql` で `DROP TABLE` も実行できる `SessionContext`。
///
/// DataFusion は、`DROP TABLE` と `CREATE OR REPLACE` の最初の手順の両方で
/// `SchemaProvider::deregister_table` を呼ぶ。プロバイダからは、削除と、まだ失敗しうる置き換えを
/// 区別できない。そこでプロバイダは登録解除を拒否し、削除はここで処理する。
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
            // namespace は暗黙に存在する。そのまま渡すと DataFusion が重複として報告する。
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

/// Iceberg のテーブルを SQL で扱えるセッションを作る。`STORED AS ICEBERG` はここで登録する
/// ファクトリに渡る。
pub async fn session(catalog: Arc<Catalog>) -> Result<IcebergSession> {
    let mut config = SessionConfig::new()
        .with_default_catalog_and_schema(CATALOG_NAME, DEFAULT_NAMESPACE)
        .with_create_default_catalog_and_schema(false);
    // セッションにタイムゾーンがないと、SQL の `TIMESTAMPTZ` はタイムゾーンなしの timestamp になる。
    // Iceberg の `timestamptz` は UTC で保存するので、セッションも UTC にする。
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
