use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::TableType;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{CreateExternalTable, Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};
use toy_iceberg_core::arrow_schema::{arrow_schema, schema_from_arrow};
use toy_iceberg_core::catalog::{Catalog, TableIdent};
use toy_iceberg_core::partition::PartitionSpec;
use toy_iceberg_core::scan::{FileScanTask, ScanPlan, read_task};
use toy_iceberg_core::table::Table;
use toy_iceberg_core::types::Field;

use crate::filter::to_predicate;
use crate::to_df;

#[derive(Debug)]
pub struct IcebergTableFactory {
    catalog: Arc<Catalog>,
}

impl IcebergTableFactory {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        IcebergTableFactory { catalog }
    }
}

#[async_trait]
impl TableProviderFactory for IcebergTableFactory {
    /// Recognised options are `partitioning` and `format-version`; the others become table
    /// properties. An empty `LOCATION` selects the default location.
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        if !cmd.table_partition_cols.is_empty() {
            return Err(DataFusionError::Plan(
                "use OPTIONS ('partitioning' '...') instead of PARTITIONED BY".into(),
            ));
        }
        if !cmd.column_defaults.is_empty()
            || !cmd.order_exprs.is_empty()
            || !cmd.constraints.is_empty()
        {
            return Err(DataFusionError::NotImplemented(
                "column defaults, WITH ORDER and constraints are not supported".into(),
            ));
        }
        let table_name = cmd.name.table();
        if table_name.contains(['$', '@']) {
            return Err(DataFusionError::Plan(format!(
                "{table_name:?} contains a character reserved for metadata tables and time travel"
            )));
        }
        let schema = schema_from_arrow(cmd.schema.as_arrow()).map_err(to_df)?;
        // DataFusion prefixes option keys that contain no dot with `format.`.
        let mut properties: BTreeMap<_, _> = cmd
            .options
            .iter()
            .map(|(k, v)| (k.strip_prefix("format.").unwrap_or(k).to_owned(), v.clone()))
            .collect();
        let spec = match properties.remove("partitioning") {
            Some(expr) => PartitionSpec::parse(&expr, &schema).map_err(to_df)?,
            None => PartitionSpec::unpartitioned(),
        };
        let format_version = match properties.remove("format-version").as_deref() {
            None | Some("2") => 2,
            Some("3") => 3,
            Some(other) => {
                return Err(DataFusionError::Plan(format!(
                    "unsupported format-version {other}"
                )));
            }
        };
        let location = cmd.locations.first().filter(|l| !l.is_empty()).cloned();
        let default_namespace = &state.config_options().catalog.default_schema;
        let namespace = cmd.name.schema().unwrap_or(default_namespace);
        let ident = TableIdent::new(namespace, table_name);
        let table = self
            .catalog
            .create_table(&ident, schema, spec, format_version, properties, location)
            .await
            .map_err(to_df)?;
        Ok(Arc::new(IcebergTable::new(table)))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IcebergTable {
    table: Table,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
}

impl IcebergTable {
    pub(crate) fn new(table: Table) -> Self {
        let schema = arrow_schema(&table.schema().fields);
        IcebergTable {
            table,
            snapshot_id: None,
            schema,
        }
    }

    pub(crate) fn at_snapshot(table: Table, id: i64) -> Self {
        IcebergTable {
            snapshot_id: Some(id),
            ..Self::new(table)
        }
    }
}

#[async_trait]
impl TableProvider for IcebergTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // A prepared statement keeps this provider, so the state at planning time can be stale.
        let table = match self.snapshot_id {
            Some(_) => self.table.clone(),
            None => self.table.latest().await.map_err(to_df)?,
        };
        let schema = table.schema();
        let fields: Vec<Field> = match projection {
            Some(p) => p.iter().map(|i| schema.fields[*i].clone()).collect(),
            None => schema.fields.clone(),
        };
        let mut scan = table.scan().filter(to_predicate(filters, schema));
        if let Some(id) = self.snapshot_id {
            scan = scan.snapshot_id(id);
        }
        let plan = scan.plan().await.map_err(to_df)?;
        let target_partitions = state.config().target_partitions();
        Ok(Arc::new(IcebergScanExec::new(
            table.clone(),
            plan,
            fields,
            target_partitions,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.snapshot_id.is_some() || insert_op != InsertOp::Append {
            return Err(DataFusionError::NotImplemented(
                "only appends to the current table are supported".into(),
            ));
        }
        let sink = IcebergSink {
            table: self.table.clone(),
            schema: self.schema.clone(),
        };
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }
}

#[derive(Debug, Clone)]
struct IcebergScanExec {
    table: Arc<Table>,
    groups: Vec<Vec<FileScanTask>>,
    stats: ScanPlan,
    fields: Vec<Field>,
    properties: Arc<PlanProperties>,
}

impl IcebergScanExec {
    fn new(table: Table, plan: ScanPlan, fields: Vec<Field>, target_partitions: usize) -> Self {
        let partitions = plan.tasks.len().clamp(1, target_partitions.max(1));
        let mut groups = vec![Vec::new(); partitions];
        for (i, task) in plan.tasks.iter().enumerate() {
            groups[i % partitions].push(task.clone());
        }
        let properties = PlanProperties::new(
            EquivalenceProperties::new(arrow_schema(&fields)),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        IcebergScanExec {
            table: Arc::new(table),
            groups,
            stats: plan,
            fields,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for IcebergScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = &self.stats;
        write!(
            f,
            "IcebergScanExec: table={}, manifests={}/{}, files={}/{}, \
             pruned_by_partition={}, pruned_by_metrics={}",
            self.table.ident(),
            s.manifests_total - s.manifests_pruned,
            s.manifests_total,
            s.tasks.len(),
            s.files_total,
            s.files_pruned_by_partition,
            s.files_pruned_by_metrics,
        )
    }
}

impl ExecutionPlan for IcebergScanExec {
    fn name(&self) -> &str {
        "IcebergScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let table = self.table.clone();
        let fields: Arc<[Field]> = self.fields.clone().into();
        let tasks = self.groups[partition].clone();
        let stream = futures::stream::iter(tasks)
            .then(move |task| {
                let (table, fields) = (table.clone(), fields.clone());
                async move { read_task(&table, &task, &fields).await }
            })
            .try_flatten()
            .map_err(to_df);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[derive(Debug)]
struct IcebergSink {
    table: Table,
    schema: SchemaRef,
}

impl DisplayAs for IcebergSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IcebergSink: table={}", self.table.ident())
    }
}

#[async_trait]
impl DataSink for IcebergSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let batches: Vec<_> = data.try_collect().await?;
        let rows = batches.iter().map(|b| b.num_rows() as u64).sum();
        self.table.append(&batches).await.map_err(to_df)?;
        Ok(rows)
    }
}
