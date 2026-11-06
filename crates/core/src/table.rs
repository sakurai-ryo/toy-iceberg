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

const MAX_COMMIT_ATTEMPTS: u32 = 32;

#[derive(Debug, Clone)]
pub struct Table {
    catalog: Arc<Catalog>,
    ident: TableIdent,
    metadata: TableMetadata,
    metadata_location: String,
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

    pub async fn manifests(&self, snapshot: &Snapshot) -> Result<Vec<ManifestFile>> {
        read_manifest_list(&self.catalog.read(&snapshot.manifest_list).await?)
    }

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

    /// Writes `batches` as new data files and commits them as one append snapshot. Returns
    /// `None` without committing when there are no rows.
    ///
    /// Data files and the new manifest are written once. Added entries leave their sequence
    /// numbers null to inherit them from the manifest list, so a retry after a conflict only
    /// rewrites the manifest list and the metadata file.
    pub async fn append(&self, batches: &[RecordBatch]) -> Result<Option<Snapshot>> {
        let files = write_data_files(self, batches).await?;
        if files.is_empty() {
            return Ok(None);
        }
        let snapshot_id = new_snapshot_id();
        let added = self.write_added_manifest(&files, snapshot_id).await?;

        for attempt in 0..MAX_COMMIT_ATTEMPTS {
            let base = self.latest().await?;
            // A request retried by the HTTP client can apply and still report 412, so a
            // conflict alone does not mean this commit lost.
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

    /// Reloads the table. A different `table-uuid` means the name now belongs to another table,
    /// and the spec requires writers to fail rather than commit their files to it.
    pub async fn latest(&self) -> Result<Table> {
        let base = self.catalog.load_table(&self.ident).await?;
        if base.metadata.table_uuid != self.metadata.table_uuid {
            return Err(Error::Invalid(format!("table {} was replaced", self.ident)));
        }
        Ok(base)
    }

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

    /// Builds the snapshot on top of this table's state. `self` must be freshly loaded.
    async fn apply_append(
        &self,
        added: &ManifestFile,
        files: &[DataFile],
        snapshot_id: i64,
    ) -> Result<(TableMetadata, Snapshot)> {
        let mut metadata = self.metadata.clone();
        let parent = metadata.current_snapshot().cloned();
        let sequence_number = metadata.last_sequence_number + 1;

        let mut new_manifest = added.clone();
        new_manifest.sequence_number = sequence_number;
        new_manifest.min_sequence_number = sequence_number;
        let mut manifests = vec![new_manifest];
        if let Some(parent) = &parent {
            manifests.extend(self.manifests(parent).await?);
        }

        // v3 row lineage: every data manifest without an assigned range gets one here,
        // including manifests inherited from before an upgrade to v3.
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

    /// Upgrades a v2 table to v3. Existing snapshots stay as they are; the next commit assigns
    /// row ids to the data files they carry.
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

fn new_snapshot_id() -> i64 {
    (uuid::Uuid::new_v4().as_u128() as i64) & i64::MAX
}

fn backoff(attempt: u32) -> Duration {
    let jitter = uuid::Uuid::new_v4().as_u128() as u64 % 50;
    Duration::from_millis(20 * attempt.min(10) as u64 + jitter)
}

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
