use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::error::{Result, invalid};
use crate::partition::PartitionSpec;
use crate::types::Schema;

pub const MAIN_BRANCH: &str = "main";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    pub snapshot_id: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    /// Snapshots written before an upgrade from v1 have no sequence number; the spec reads 0.
    #[serde(default)]
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    pub summary: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub added_rows: Option<i64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLogEntry {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataLogEntry {
    pub metadata_file: String,
    pub timestamp_ms: i64,
}

/// Table metadata file contents. Sort orders, refs and any field this implementation does not
/// model stay as JSON so that a commit does not drop what another writer stored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    pub format_version: i32,
    pub table_uuid: String,
    pub location: String,
    pub last_sequence_number: i64,
    pub last_updated_ms: i64,
    pub last_column_id: i32,
    pub schemas: Vec<Schema>,
    pub current_schema_id: i32,
    pub partition_specs: Vec<PartitionSpec>,
    pub default_spec_id: i32,
    pub last_partition_id: i32,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    #[serde(default)]
    pub snapshot_log: Vec<SnapshotLogEntry>,
    #[serde(default)]
    pub metadata_log: Vec<MetadataLogEntry>,
    pub sort_orders: Vec<Value>,
    pub default_sort_order_id: i32,
    #[serde(default)]
    pub refs: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_row_id: Option<i64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl TableMetadata {
    pub fn new(
        location: String,
        schema: Schema,
        spec: PartitionSpec,
        format_version: i32,
        properties: BTreeMap<String, String>,
        now_ms: i64,
    ) -> Result<Self> {
        let metadata = TableMetadata {
            format_version,
            table_uuid: uuid::Uuid::new_v4().to_string(),
            location,
            last_sequence_number: 0,
            last_updated_ms: now_ms,
            last_column_id: schema.highest_field_id(),
            current_schema_id: schema.schema_id,
            schemas: vec![schema],
            default_spec_id: spec.spec_id,
            last_partition_id: spec.last_field_id(),
            partition_specs: vec![spec],
            properties,
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![json!({"order-id": 0, "fields": []})],
            default_sort_order_id: 0,
            refs: BTreeMap::new(),
            next_row_id: (format_version >= 3).then_some(0),
            extra: BTreeMap::new(),
        };
        metadata.validate()?;
        Ok(metadata)
    }

    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let mut metadata: TableMetadata = serde_json::from_slice(bytes)?;
        // Older Java writers use -1 for "no current snapshot".
        if metadata.current_snapshot_id == Some(-1) {
            metadata.current_snapshot_id = None;
        }
        metadata.validate()?;
        Ok(metadata)
    }

    fn validate(&self) -> Result<()> {
        if !(2..=3).contains(&self.format_version) {
            return invalid(format!(
                "unsupported format version {}",
                self.format_version
            ));
        }
        if self.format_version >= 3 && self.next_row_id.is_none() {
            return invalid("v3 metadata requires next-row-id");
        }
        if !self
            .schemas
            .iter()
            .any(|s| s.schema_id == self.current_schema_id)
        {
            return invalid(format!(
                "current schema {} not found",
                self.current_schema_id
            ));
        }
        if !self
            .partition_specs
            .iter()
            .any(|s| s.spec_id == self.default_spec_id)
        {
            return invalid(format!("default spec {} not found", self.default_spec_id));
        }
        let order_exists = |o: &Value| {
            o.get("order-id").and_then(Value::as_i64) == Some(self.default_sort_order_id as i64)
        };
        self.default_spec().partition_type(self.current_schema())?;
        if self.default_sort_order_id != 0 && !self.sort_orders.iter().any(order_exists) {
            return invalid(format!(
                "default sort order {} not found",
                self.default_sort_order_id
            ));
        }
        if let Some(id) = self.current_snapshot_id
            && self.snapshot(id).is_none()
        {
            return invalid(format!("current snapshot {id} not found"));
        }
        Ok(())
    }

    pub fn current_schema(&self) -> &Schema {
        self.schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
            .expect("checked by validate")
    }

    pub fn default_spec(&self) -> &PartitionSpec {
        self.spec(self.default_spec_id)
            .expect("checked by validate")
    }

    pub fn spec(&self, id: i32) -> Option<&PartitionSpec> {
        self.partition_specs.iter().find(|s| s.spec_id == id)
    }

    pub fn snapshot(&self, id: i64) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.snapshot_id == id)
    }

    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.current_snapshot_id.and_then(|id| self.snapshot(id))
    }

    pub(crate) fn add_snapshot(&mut self, snapshot: Snapshot) {
        self.last_sequence_number = snapshot.sequence_number;
        self.last_updated_ms = snapshot.timestamp_ms;
        self.current_snapshot_id = Some(snapshot.snapshot_id);
        self.snapshot_log.push(SnapshotLogEntry {
            snapshot_id: snapshot.snapshot_id,
            timestamp_ms: snapshot.timestamp_ms,
        });
        self.refs
            .entry(MAIN_BRANCH.to_owned())
            .and_modify(|r| r["snapshot-id"] = json!(snapshot.snapshot_id))
            .or_insert_with(|| json!({"snapshot-id": snapshot.snapshot_id, "type": "branch"}));
        self.snapshots.push(snapshot);
    }
}
