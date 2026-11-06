//! テーブルの操作。追記のコミット手順を実装する。
//!
//! Iceberg のコミットは楽観的並行性制御である。書き手はロックを取らず、読んだ metadata の上に
//! 新しい metadata を作り、ポインタの差し替えで競合を検出する。追記は他のコミットと論理的に
//! 衝突しない（仕様の「Commit Conflict Resolution and Retry」）ので、競合したら
//! 最新の metadata の上で作り直すだけで適用できる。

use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;

use crate::catalog::{Catalog, TableIdent, now_ms};
use crate::error::{Error, Result};
use crate::manifest::{
    CONTENT_DATA, DataFile, FieldSummary, ManifestEntry, ManifestFile, ManifestListHeader,
    STATUS_ADDED, read_manifest, read_manifest_list, write_manifest, write_manifest_list,
};
use crate::metadata::{Snapshot, TableMetadata};
use crate::scan::TableScan;
use crate::types::Schema;
use crate::values::Datum;
use crate::writer::write_data_files;

// 競合が続いても無限に再試行しないための上限。
const MAX_COMMIT_ATTEMPTS: u32 = 32;

/// ある時点の metadata を持つテーブルのハンドル。
///
/// 不変の metadata を指すだけなので、他の書き手のコミットで内容が古くなることがある。
/// 読み取りはこの時点の snapshot を見る。最新の状態が必要なら [`Table::latest`] で読み直す。
#[derive(Debug, Clone)]
pub struct Table {
    catalog: Arc<Catalog>,
    ident: TableIdent,
    metadata: TableMetadata,
    metadata_location: String,
    /// `metadata_location` を読んだときのポインタの ETag。コミットの check-and-put の条件になる。
    e_tag: Option<String>,
}

impl Table {
    pub(crate) fn new(
        catalog: Arc<Catalog>,
        ident: TableIdent,
        metadata: TableMetadata,
        metadata_location: String,
        e_tag: Option<String>,
    ) -> Self {
        Table {
            catalog,
            ident,
            metadata,
            metadata_location,
            e_tag,
        }
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    pub fn ident(&self) -> &TableIdent {
        &self.ident
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }

    pub(crate) fn e_tag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }

    pub fn schema(&self) -> &Schema {
        self.metadata.current_schema()
    }

    pub fn scan(&self) -> TableScan<'_> {
        TableScan::new(self)
    }

    /// snapshot の manifest list を読む。
    pub async fn manifests(&self, snapshot: &Snapshot) -> Result<Vec<ManifestFile>> {
        read_manifest_list(&self.catalog.read(&snapshot.manifest_list).await?)
    }

    /// manifest の entry を読む。パーティション値の型は、現在の定義ではなく
    /// manifest を書いたときの定義から求める必要がある。
    pub async fn entries(&self, manifest: &ManifestFile) -> Result<Vec<ManifestEntry>> {
        let spec = self
            .metadata
            .spec(manifest.partition_spec_id)
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "partition spec {} not found",
                    manifest.partition_spec_id
                ))
            })?;
        let types = spec.partition_type(self.schema())?;
        read_manifest(&self.catalog.read(&manifest.path).await?, &types, manifest)
    }

    /// `batches` を新しい data file に書き、1つの append snapshot としてコミットする。
    /// 行がなければコミットせず `None` を返す。
    ///
    /// data file と新しい manifest は一度だけ書く。追加する entry の sequence number は `null` にして
    /// manifest list から継承させるので、競合後の再試行で作り直すのは manifest list と metadata ファイル
    /// だけで済む。sequence number はコミットが成功するまで確定しないので、manifest に値を書くと
    /// 再試行のたびに manifest も書き直すことになる（仕様の「Sequence Number Inheritance」）。
    pub async fn append(&self, batches: &[RecordBatch]) -> Result<Option<Snapshot>> {
        let files = write_data_files(self, batches).await?;
        if files.is_empty() {
            return Ok(None);
        }
        // 再試行でも同じ ID を使う。この ID で「自分のコミットが既に成功したか」を判定できる。
        let snapshot_id = new_snapshot_id();
        let added = self.write_added_manifest(&files, snapshot_id).await?;

        for attempt in 0..MAX_COMMIT_ATTEMPTS {
            let base = self.latest().await?;
            // HTTP クライアントが再送したリクエストは、適用されたうえで 412 を返すことがある。
            // 競合しただけでは、このコミットが負けたとは限らない。ここで確認しないと二重にコミットしてしまう。
            if let Some(snapshot) = base.metadata.snapshot(snapshot_id) {
                return Ok(Some(snapshot.clone()));
            }
            let (metadata, snapshot) = base.apply_append(&added, &files, snapshot_id).await?;
            match self.catalog.commit(&base, metadata).await {
                Ok(()) => return Ok(Some(snapshot)),
                Err(Error::CommitConflict(_)) => tokio::time::sleep(backoff(attempt)).await,
                Err(e) => return Err(e),
            }
        }
        Err(Error::CommitConflict(self.ident.to_string()))
    }

    /// テーブルを読み直す。`table-uuid` が異なるなら、その名前は別のテーブルのものになっている。
    /// 仕様の「Table Metadata」は、自分のファイルをそこへコミットせず失敗することを求めている。
    pub async fn latest(&self) -> Result<Table> {
        let base = self.catalog.load_table(&self.ident).await?;
        if base.metadata.table_uuid != self.metadata.table_uuid {
            return Err(Error::Invalid(format!("table {} was replaced", self.ident)));
        }
        Ok(base)
    }

    /// 追加するファイルの manifest を書く。
    ///
    /// entry の `snapshot_id` はここで確定できるので値を書き、sequence number は `null` にする。
    /// 戻り値の `sequence_number` と `first_row_id` は仮の値で、[`Table::apply_append`] が決める。
    async fn write_added_manifest(
        &self,
        files: &[DataFile],
        snapshot_id: i64,
    ) -> Result<ManifestFile> {
        let spec = self.metadata.default_spec();
        let schema = self.schema();
        let entries: Vec<_> = files
            .iter()
            .map(|f| ManifestEntry {
                status: STATUS_ADDED,
                snapshot_id: Some(snapshot_id),
                sequence_number: None,
                file_sequence_number: None,
                data_file: f.clone(),
            })
            .collect();
        let format_version = self.metadata.format_version;
        let bytes = write_manifest(&entries, schema, spec, format_version)?;
        let length = bytes.len() as i64;
        let path = format!(
            "{}/metadata/{}-m0.avro",
            self.metadata.location,
            uuid::Uuid::new_v4()
        );
        self.catalog.write(&path, bytes).await?;
        Ok(ManifestFile {
            path,
            length,
            partition_spec_id: spec.spec_id,
            content: CONTENT_DATA,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: snapshot_id,
            added_files_count: files.len() as i32,
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count: files.iter().map(|f| f.record_count).sum(),
            existing_rows_count: 0,
            deleted_rows_count: 0,
            partitions: partition_summaries(files, spec.fields.len()),
            first_row_id: None,
        })
    }

    /// このテーブルの状態の上に snapshot を組み立てる。`self` は読み直した直後のものでなければならない。
    /// 再試行のたびに呼ばれ、manifest list を書いて新しい metadata を返す。
    async fn apply_append(
        &self,
        added: &ManifestFile,
        files: &[DataFile],
        snapshot_id: i64,
    ) -> Result<(TableMetadata, Snapshot)> {
        let mut metadata = self.metadata.clone();
        let parent = metadata.current_snapshot().cloned();
        // 仕様の「Sequence Numbers」: 親の次の番号を楽観的に割り当て、再試行では割り当て直す。
        let sequence_number = metadata.last_sequence_number + 1;

        let mut new_manifest = added.clone();
        new_manifest.sequence_number = sequence_number;
        new_manifest.min_sequence_number = sequence_number;
        // 新しい manifest を親の manifest の一覧に足す。古い manifest は書き直さない。
        let mut manifests = vec![new_manifest];
        if let Some(parent) = &parent {
            manifests.extend(self.manifests(parent).await?);
        }

        // v3 の row lineage（仕様の「First Row ID Assignment」）。範囲が未割り当ての data manifest に
        // ここで割り当てる。v3 へのアップグレード前から引き継いだ manifest も対象になる。
        let first_row_id = metadata.next_row_id;
        if let Some(start) = first_row_id {
            let mut next = start;
            for m in manifests.iter_mut().filter(|m| m.content == CONTENT_DATA) {
                if m.first_row_id.is_none() {
                    m.first_row_id = Some(next);
                    next += m.added_rows_count + m.existing_rows_count;
                }
            }
            metadata.next_row_id = Some(next);
        }

        // 再試行ごとにファイル名を変える。失敗した試行のファイルを上書きしないためである。
        let list_location = format!(
            "{}/metadata/snap-{snapshot_id}-{sequence_number}-{}.avro",
            metadata.location,
            uuid::Uuid::new_v4()
        );
        let header = ManifestListHeader {
            snapshot_id,
            parent_snapshot_id: parent.as_ref().map(|p| p.snapshot_id),
            sequence_number,
            first_row_id,
        };
        let list = write_manifest_list(&manifests, &header, metadata.format_version)?;
        self.catalog.write(&list_location, list).await?;

        // 合計は親の summary に足して求める。全 manifest を読み直さずに済む。
        let added_records: i64 = files.iter().map(|f| f.record_count).sum();
        let total = |key: &str| -> i64 {
            parent
                .as_ref()
                .and_then(|p| p.summary.get(key))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
        };
        let summary = [
            ("operation", "append".to_owned()),
            ("added-data-files", files.len().to_string()),
            ("added-records", added_records.to_string()),
            (
                "total-data-files",
                (total("total-data-files") + files.len() as i64).to_string(),
            ),
            (
                "total-records",
                (total("total-records") + added_records).to_string(),
            ),
            ("total-delete-files", "0".to_owned()),
            ("total-position-deletes", "0".to_owned()),
            ("total-equality-deletes", "0".to_owned()),
        ];
        let snapshot = Snapshot {
            snapshot_id,
            parent_snapshot_id: header.parent_snapshot_id,
            sequence_number,
            // 書き手の時計がずれていても、履歴の時刻が逆行しないようにする。
            timestamp_ms: now_ms().max(metadata.last_updated_ms),
            manifest_list: list_location,
            summary: summary
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v))
                .collect(),
            schema_id: Some(metadata.current_schema_id),
            first_row_id,
            extra: Default::default(),
            added_rows: metadata
                .next_row_id
                .zip(first_row_id)
                .map(|(next, first)| next - first),
        };
        metadata.add_snapshot(snapshot.clone());
        Ok((metadata, snapshot))
    }

    /// v2 のテーブルを v3 にアップグレードする。既存の snapshot はそのまま残し、
    /// それらが持つ data file には次のコミットで row id を割り当てる。
    pub async fn upgrade_to_v3(&self) -> Result<()> {
        let base = self.latest().await?;
        if base.metadata.format_version >= 3 {
            return Ok(());
        }
        let mut metadata = base.metadata.clone();
        metadata.format_version = 3;
        metadata.next_row_id = Some(0);
        metadata.last_updated_ms = now_ms().max(metadata.last_updated_ms);
        self.catalog.commit(&base, metadata).await
    }
}

/// 仕様の「Appendix F」が求める、衝突しにくい正の値を作る。
///
/// ponytail: 仕様が推奨する既存 ID との衝突の確認は省いている。64 ビットの乱数なので実害は考えにくい。
/// 厳密にするなら、[`Table::append`] のループで ID の重複を検出する。
fn new_snapshot_id() -> i64 {
    (uuid::Uuid::new_v4().as_u128() as i64) & i64::MAX
}

/// ジッタを入れて、競合した書き手が同時に再試行し続けることを避ける。
fn backoff(attempt: u32) -> Duration {
    let jitter = uuid::Uuid::new_v4().as_u128() as u64 % 50;
    Duration::from_millis(20 * attempt.min(10) as u64 + jitter)
}

/// manifest list の `partitions` を作る。上下限には null と NaN を含めない。
/// `contains_nan` は float 型のパーティション値にだけ値を持たせる。他の型では意味を持たない。
fn partition_summaries(files: &[DataFile], fields: usize) -> Vec<FieldSummary> {
    let mut summaries = vec![FieldSummary::default(); fields];
    for (pos, s) in summaries.iter_mut().enumerate() {
        let mut bounds: Option<(&Datum, &Datum)> = None;
        for f in files {
            let Some(v) = &f.partition[pos] else {
                s.contains_null = true;
                continue;
            };
            match v {
                Datum::Float(x) if x.is_nan() => s.contains_nan = Some(true),
                Datum::Double(x) if x.is_nan() => s.contains_nan = Some(true),
                _ => {
                    bounds = Some(match bounds {
                        None => (v, v),
                        Some((lo, hi)) => (
                            if v.compare(lo).is_some_and(|o| o.is_lt()) {
                                v
                            } else {
                                lo
                            },
                            if v.compare(hi).is_some_and(|o| o.is_gt()) {
                                v
                            } else {
                                hi
                            },
                        ),
                    });
                }
            }
        }
        if let Some((lo, hi)) = bounds {
            s.lower_bound = Some(lo.to_bytes());
            s.upper_bound = Some(hi.to_bytes());
        }
        if matches!(
            files[0].partition[pos],
            Some(Datum::Float(_) | Datum::Double(_))
        ) {
            s.contains_nan.get_or_insert(false);
        }
    }
    summaries
}
