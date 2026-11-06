//! 読み取り。仕様の「Scan Planning」を実装する。
//!
//! 処理は2段階に分かれる。[`TableScan::plan`] が metadata だけを読んで対象の data file を決め、
//! [`read_task`] が各ファイルの Parquet を読む。

use std::collections::HashMap;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use futures::future::try_join_all;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
#[allow(deprecated)]
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ParquetRecordBatchStreamBuilder, ProjectionMask};

use crate::arrow_schema::{arrow_schema, arrow_type};
use crate::error::{Error, Result, invalid};
use crate::expr::{Predicate, manifest_may_match, metrics_may_match, partition_matches, project};
use crate::manifest::{CONTENT_DATA, DataFile, ManifestFile, STATUS_DELETED};
use crate::partition::PartitionSpec;
use crate::table::Table;
use crate::transform::Transform;
use crate::types::Field;
use crate::values::Datum;

/// 読む data file の1個。パーティション値を解釈するので、書いたときの定義の ID も持つ。
#[derive(Debug, Clone)]
pub struct FileScanTask {
    pub data_file: DataFile,
    pub spec_id: i32,
}

/// 読むファイルの一覧と、pruning の結果の件数。件数は `EXPLAIN` の表示に使う。
#[derive(Debug, Clone, Default)]
pub struct ScanPlan {
    pub tasks: Vec<FileScanTask>,
    pub manifests_total: usize,
    pub manifests_pruned: usize,
    /// pruning を通過した manifest に含まれるファイルの数。テーブル全体のファイル数ではない。
    pub files_total: usize,
    pub files_pruned_by_partition: usize,
    pub files_pruned_by_metrics: usize,
}

pub struct TableScan<'a> {
    table: &'a Table,
    snapshot_id: Option<i64>,
    filter: Predicate,
}

impl<'a> TableScan<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        TableScan {
            table,
            snapshot_id: None,
            filter: Predicate::True,
        }
    }

    pub fn snapshot_id(mut self, id: i64) -> Self {
        self.snapshot_id = Some(id);
        self
    }

    pub fn filter(mut self, filter: Predicate) -> Self {
        self.filter = filter;
        self
    }

    /// 読む data file を決める。除外の判定は [`crate::expr`] の3段階で行う。
    ///
    /// 1. manifest list の要約で manifest を除外する。
    /// 2. manifest を読み、パーティション値でファイルを除外する。
    /// 3. 列統計でファイルを除外する。
    ///
    /// 上の階層の要約で除外できれば、下の階層を読まずに済む。どの段階も
    /// 「一致する行を含みうるファイル」は残す。残りの絞り込みは呼び出し側が行う。
    pub async fn plan(&self) -> Result<ScanPlan> {
        let metadata = self.table.metadata();
        let snapshot = match self.snapshot_id {
            Some(id) => match metadata.snapshot(id) {
                Some(s) => s,
                None => return invalid(format!("snapshot {id} not found")),
            },
            None => match metadata.current_snapshot() {
                Some(s) => s,
                None => return Ok(ScanPlan::default()),
            },
        };
        let schema = self.table.schema();
        let manifests = self.table.manifests(snapshot).await?;
        let mut plan = ScanPlan {
            manifests_total: manifests.len(),
            ..Default::default()
        };

        let mut selected: Vec<(&ManifestFile, Predicate)> = Vec::new();
        for m in &manifests {
            // delete file を読み飛ばすと、削除された行を黙って返してしまう。
            if m.content != CONTENT_DATA {
                return Err(Error::Unsupported("tables with delete files".into()));
            }
            // 仕様の「Scan Planning」は、現在の定義ではなく manifest を書いたときの定義で
            // 述語を変換することを求める。パーティション進化があると manifest ごとに定義が異なるからである。
            let spec = spec_of(self.table, m.partition_spec_id)?;
            let projected = project(&self.filter, spec, schema);
            if manifest_may_match(&projected, &m.partitions, &spec.partition_type(schema)?) {
                selected.push((m, projected));
            } else {
                plan.manifests_pruned += 1;
            }
        }

        let entries = try_join_all(selected.iter().map(|(m, _)| self.table.entries(m))).await?;
        // 仕様の「Scan Planning」のとおり、`DELETED` の entry はスキャンに使わない。
        for ((manifest, projected), entries) in selected.iter().zip(entries) {
            for entry in entries.into_iter().filter(|e| e.status != STATUS_DELETED) {
                plan.files_total += 1;
                if !partition_matches(projected, &entry.data_file.partition) {
                    plan.files_pruned_by_partition += 1;
                } else if !metrics_may_match(&self.filter, &entry.data_file, schema) {
                    plan.files_pruned_by_metrics += 1;
                } else {
                    plan.tasks.push(FileScanTask {
                        data_file: entry.data_file,
                        spec_id: manifest.partition_spec_id,
                    });
                }
            }
        }
        Ok(plan)
    }
}

fn spec_of(table: &Table, id: i32) -> Result<&PartitionSpec> {
    table
        .metadata()
        .spec(id)
        .ok_or_else(|| Error::Invalid(format!("partition spec {id} not found")))
}

/// 1つの data file を読み、`projection` の列のバッチを返す。
///
/// 列は名前ではなく field id で探す。列名の変更があっても、ファイル内の古い名前に影響されず
/// 正しい列を読める。これが Iceberg のスキーマ進化の前提である（仕様の「Schema Evolution」）。
// 代替は `AsyncFileReader` の手書きになる。これを使えば、列の範囲だけを読む処理を書かずに済む。
#[allow(deprecated)]
pub async fn read_task(
    table: &Table,
    task: &FileScanTask,
    projection: &[Field],
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let file = &task.data_file;
    if !file.file_format.eq_ignore_ascii_case("PARQUET") {
        return Err(Error::Unsupported(format!(
            "file format {}",
            file.file_format
        )));
    }
    let catalog = table.catalog();
    let reader = ParquetObjectReader::new(catalog.store().clone(), catalog.path(&file.file_path)?)
        .with_file_size(file.file_size_in_bytes as u64);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

    let file_columns: HashMap<i32, usize> = builder
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            Some((
                f.metadata().get(PARQUET_FIELD_ID_META_KEY)?.parse().ok()?,
                i,
            ))
        })
        .collect();
    // field id のないファイルは仕様の name mapping が必要になる。対応しないのでエラーにする。
    // そのまま進むと、全列が null の行を返してしまう。
    if file_columns.is_empty() && !builder.schema().fields().is_empty() {
        return Err(Error::Unsupported(format!(
            "{} has no field ids",
            file.file_path
        )));
    }

    // Parquet のリーダーは指定の順序によらずファイルの列順で返す。
    // 下の `sources` が結果の位置を探せるよう、ソートしておく。
    let mut roots: Vec<usize> = projection
        .iter()
        .filter_map(|f| file_columns.get(&f.id).copied())
        .collect();
    roots.sort_unstable();
    let mask = ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
    let stream = builder.with_projection(mask).build()?;

    let spec = spec_of(table, task.spec_id)?;
    let sources: Vec<Source> = projection
        .iter()
        .map(|f| match file_columns.get(&f.id) {
            Some(c) => Source::Column(roots.binary_search(c).expect("selected above")),
            None => Source::Constant(identity_value(spec, file, f.id)),
        })
        .collect();
    let fields = projection.to_vec();
    let schema = arrow_schema(projection);
    Ok(stream
        .map_err(Error::from)
        .map(move |batch| shape_batch(&batch?, &sources, &fields, &schema))
        .boxed())
}

/// 出力の1列の取得元。`Column` はファイルから読んだバッチの列の位置を指す。
enum Source {
    Column(usize),
    Constant(Option<Datum>),
}

/// data file にない列の値を求める。仕様の「Schema Evolution」の規則のうち、identity パーティションの
/// 値を使う規則と、それ以外を null にする規則を実装する。name mapping と `initial-default` には対応しない。
fn identity_value(spec: &PartitionSpec, file: &DataFile, field_id: i32) -> Option<Datum> {
    let pos = spec
        .fields
        .iter()
        .position(|p| p.source_id == field_id && p.transform == Transform::Identity)?;
    file.partition.get(pos)?.clone()
}

/// ファイルから読んだバッチを `projection` の形にする。`cast` は、ファイルの型とテーブルの型の
/// 差を埋める。行数を明示するのは、`COUNT(*)` のように列が1つもないバッチでも行数を保つためである。
fn shape_batch(
    batch: &RecordBatch,
    sources: &[Source],
    fields: &[Field],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let columns = sources
        .iter()
        .zip(fields)
        .map(|(source, f)| -> Result<ArrayRef> {
            Ok(match source {
                Source::Column(i) => cast(batch.column(*i), &arrow_type(f.ty))?,
                Source::Constant(v) => Datum::constant_array(v.as_ref(), f.ty, batch.num_rows()),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &options,
    )?)
}
