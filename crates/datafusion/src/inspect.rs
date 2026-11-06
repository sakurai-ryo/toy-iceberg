//! メタデータテーブル（`t$snapshots`、`t$manifests`、`t$files`）。
//!
//! 仕様の一部ではなく、Spark などのエンジンが提供する機能に相当する。
//! metadata と manifest の内容を SQL で確認するためにある。内容は小さいので、メモリ上の表にする。

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{MemTable, Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use toy_iceberg_core::manifest::{DataFile, ManifestFile, STATUS_DELETED};
use toy_iceberg_core::metadata::Snapshot;
use toy_iceberg_core::table::Table;
use toy_iceberg_core::types::Schema;
use toy_iceberg_core::values::Datum;

use crate::to_df;

pub(crate) async fn metadata_table(table: &Table, kind: &str) -> Result<Arc<dyn TableProvider>> {
    let batch = match kind {
        "snapshots" => snapshots(&table.metadata().snapshots),
        "manifests" => manifests(&current_manifests(table).await?),
        "files" => files(table).await,
        other => {
            return Err(DataFusionError::Plan(format!(
                "unknown metadata table {other:?}"
            )));
        }
    }?;
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    Ok(Arc::new(MetadataTable(table)))
}

/// `MemTable` は INSERT を受け付ける。ここでは成功を報告しながら何も変わらないことになるので、
/// `insert_into` を持たないラッパーで包む。
#[derive(Debug)]
struct MetadataTable(MemTable);

#[async_trait]
impl TableProvider for MetadataTable {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        self.0.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.0.scan(state, projection, filters, limit).await
    }
}

async fn current_manifests(table: &Table) -> Result<Vec<ManifestFile>> {
    match table.metadata().current_snapshot() {
        Some(s) => table.manifests(s).await.map_err(to_df),
        None => Ok(vec![]),
    }
}

fn long(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn opt_long(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn int(values: Vec<i32>) -> ArrayRef {
    Arc::new(Int32Array::from(values))
}

fn string(values: Vec<String>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

fn snapshots(snapshots: &[Snapshot]) -> Result<RecordBatch, DataFusionError> {
    let col = |f: fn(&Snapshot) -> i64| long(snapshots.iter().map(f).collect());
    let opt = |f: fn(&Snapshot) -> Option<i64>| opt_long(snapshots.iter().map(f).collect());
    let text = |key: &'static str| {
        let values = snapshots
            .iter()
            .map(|s| s.summary.get(key).cloned().unwrap_or_default());
        string(values.collect())
    };
    let number = |key: &'static str| {
        let values = snapshots.iter().map(|s| s.summary.get(key)?.parse().ok());
        opt_long(values.collect())
    };
    Ok(RecordBatch::try_from_iter([
        ("snapshot_id", col(|s| s.snapshot_id)),
        ("parent_id", opt(|s| s.parent_snapshot_id)),
        ("sequence_number", col(|s| s.sequence_number)),
        ("timestamp_ms", col(|s| s.timestamp_ms)),
        ("operation", text("operation")),
        ("added_records", number("added-records")),
        ("total_records", number("total-records")),
        ("first_row_id", opt(|s| s.first_row_id)),
        ("added_rows", opt(|s| s.added_rows)),
        (
            "manifest_list",
            string(snapshots.iter().map(|s| s.manifest_list.clone()).collect()),
        ),
    ])?)
}

fn manifests(manifests: &[ManifestFile]) -> Result<RecordBatch, DataFusionError> {
    let col = |f: fn(&ManifestFile) -> i64| long(manifests.iter().map(f).collect());
    let count = |f: fn(&ManifestFile) -> i32| int(manifests.iter().map(f).collect());
    Ok(RecordBatch::try_from_iter([
        (
            "path",
            string(manifests.iter().map(|m| m.path.clone()).collect()),
        ),
        ("length", col(|m| m.length)),
        ("partition_spec_id", count(|m| m.partition_spec_id)),
        ("sequence_number", col(|m| m.sequence_number)),
        ("added_snapshot_id", col(|m| m.added_snapshot_id)),
        ("added_files_count", count(|m| m.added_files_count)),
        ("existing_files_count", count(|m| m.existing_files_count)),
        ("added_rows_count", col(|m| m.added_rows_count)),
        (
            "first_row_id",
            opt_long(manifests.iter().map(|m| m.first_row_id).collect()),
        ),
    ])?)
}

async fn files(table: &Table) -> Result<RecordBatch> {
    let mut data_files = Vec::new();
    for manifest in current_manifests(table).await? {
        let entries = table.entries(&manifest).await.map_err(to_df)?;
        data_files.extend(
            entries
                .into_iter()
                .filter(|e| e.status != STATUS_DELETED)
                .map(|e| e.data_file),
        );
    }
    let schema = table.schema();
    let col = |f: fn(&DataFile) -> i64| long(data_files.iter().map(f).collect());
    let text = |f: &dyn Fn(&DataFile) -> String| string(data_files.iter().map(f).collect());
    Ok(RecordBatch::try_from_iter([
        ("file_path", text(&|f| f.file_path.clone())),
        ("file_format", text(&|f| f.file_format.clone())),
        ("partition", text(&|f| format_partition(&f.partition))),
        ("record_count", col(|f| f.record_count)),
        ("file_size_in_bytes", col(|f| f.file_size_in_bytes)),
        (
            "lower_bounds",
            text(&|f| format_bounds(&f.lower_bounds, schema)),
        ),
        (
            "upper_bounds",
            text(&|f| format_bounds(&f.upper_bounds, schema)),
        ),
        (
            "first_row_id",
            opt_long(data_files.iter().map(|f| f.first_row_id).collect()),
        ),
    ])?)
}

fn format_partition(partition: &[Option<Datum>]) -> String {
    let values: Vec<_> = partition
        .iter()
        .map(|v| v.as_ref().map_or("null".to_owned(), Datum::to_string))
        .collect();
    format!("[{}]", values.join(", "))
}

fn format_bounds(bounds: &std::collections::BTreeMap<i32, Vec<u8>>, schema: &Schema) -> String {
    let values: Vec<_> = bounds
        .iter()
        .filter_map(|(id, bytes)| {
            let f = schema.field(*id)?;
            let v = Datum::from_bytes(f.ty, bytes).ok()?;
            Some(format!("{}={v}", f.name))
        })
        .collect();
    values.join(", ")
}
