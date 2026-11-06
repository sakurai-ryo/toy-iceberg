//! Avro encoding of manifests and manifest lists. The writer schemas follow the field ids in the
//! spec; readers look fields up by name, so foreign files with extra fields still parse.

use std::collections::{BTreeMap, HashMap};

use apache_avro::schema::UnionSchema;
use apache_avro::types::Value;
use apache_avro::{Reader, Schema as AvroSchema, Writer};
use serde_json::{Value as Json, json};

use crate::error::{Error, Result, invalid};
use crate::partition::PartitionSpec;
use crate::types::{Schema, Type};
use crate::values::Datum;

pub const STATUS_EXISTING: i32 = 0;
pub const STATUS_ADDED: i32 = 1;
pub const STATUS_DELETED: i32 = 2;
pub const CONTENT_DATA: i32 = 0;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct FieldSummary {
    pub contains_null: bool,
    pub contains_nan: Option<bool>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManifestFile {
    pub path: String,
    pub length: i64,
    pub partition_spec_id: i32,
    pub content: i32,
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    pub added_files_count: i32,
    pub existing_files_count: i32,
    pub deleted_files_count: i32,
    pub added_rows_count: i64,
    pub existing_rows_count: i64,
    pub deleted_rows_count: i64,
    pub partitions: Vec<FieldSummary>,
    pub first_row_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataFile {
    pub content: i32,
    pub file_path: String,
    pub file_format: String,
    pub partition: Vec<Option<Datum>>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub value_counts: BTreeMap<i32, i64>,
    pub null_value_counts: BTreeMap<i32, i64>,
    pub nan_value_counts: BTreeMap<i32, i64>,
    pub lower_bounds: BTreeMap<i32, Vec<u8>>,
    pub upper_bounds: BTreeMap<i32, Vec<u8>>,
    pub first_row_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManifestEntry {
    pub status: i32,
    pub snapshot_id: Option<i64>,
    pub sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub data_file: DataFile,
}

fn opt(ty: Json) -> Json {
    json!(["null", ty])
}

fn field(id: i32, name: &str, ty: Json) -> Json {
    json!({"name": name, "type": ty, "field-id": id})
}

fn opt_field(id: i32, name: &str, ty: Json) -> Json {
    json!({"name": name, "type": opt(ty), "default": null, "field-id": id})
}

/// Iceberg stores maps with non-string keys as an array of key/value records.
fn int_map(key_id: i32, value_id: i32, value: &str) -> Json {
    json!({
        "type": "array",
        "logicalType": "map",
        "items": {
            "type": "record",
            "name": format!("k{key_id}_v{value_id}"),
            "fields": [field(key_id, "key", json!("int")), field(value_id, "value", json!(value))],
        },
    })
}

fn avro_type(ty: Type) -> Json {
    match ty {
        Type::Boolean => json!("boolean"),
        Type::Int => json!("int"),
        Type::Long => json!("long"),
        Type::Float => json!("float"),
        Type::Double => json!("double"),
        Type::Date => json!({"type": "int", "logicalType": "date"}),
        Type::Timestamp => json!({
            "type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": false,
        }),
        Type::Timestamptz => json!({
            "type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": true,
        }),
        Type::String => json!("string"),
        Type::Binary => json!("bytes"),
    }
}

/// `Schema::parse` drops custom attributes on arrays, which readers need: the spec requires
/// `logicalType: map` on integer-keyed maps, and list elements carry their field id.
fn parse_schema(json: &Json) -> Result<AvroSchema> {
    let mut schema = AvroSchema::parse(json)?;
    restore_array_attributes(&mut schema)?;
    Ok(schema)
}

fn restore_array_attributes(schema: &mut AvroSchema) -> Result<()> {
    match schema {
        AvroSchema::Record(r) => {
            for f in &mut r.fields {
                restore_array_attributes(&mut f.schema)?;
            }
        }
        AvroSchema::Union(u) => {
            let mut variants = u.variants().to_vec();
            for v in &mut variants {
                restore_array_attributes(v)?;
            }
            *u = UnionSchema::new(variants)?;
        }
        AvroSchema::Array(a) => {
            restore_array_attributes(&mut a.items)?;
            if let AvroSchema::Record(item) = a.items.as_ref() {
                if item.name.name() == "r508" {
                    a.attributes.insert("element-id".into(), json!(508));
                } else {
                    a.attributes.insert("logicalType".into(), json!("map"));
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Same scheme as Java's `AvroSchemaUtil.makeCompatibleName`.
fn avro_name(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_alphabetic() || c == '_' || (i > 0 && c.is_ascii_digit()) {
            out.push(c);
        } else if c.is_ascii_digit() {
            out.push('_');
            out.push(c);
        } else {
            out.push_str(&format!("_x{:X}", c as u32));
        }
    }
    out
}

fn manifest_schema(
    spec: &PartitionSpec,
    types: &[Type],
    format_version: i32,
) -> Result<AvroSchema> {
    let partition_fields: Vec<Json> = spec
        .fields
        .iter()
        .zip(types)
        .map(|(f, ty)| {
            let mut field = opt_field(f.field_id, &avro_name(&f.name), avro_type(*ty));
            field["iceberg-field-name"] = json!(f.name);
            field
        })
        .collect();
    let mut data_file = vec![
        field(134, "content", json!("int")),
        field(100, "file_path", json!("string")),
        field(101, "file_format", json!("string")),
        field(
            102,
            "partition",
            json!({"type": "record", "name": "r102", "fields": partition_fields}),
        ),
        field(103, "record_count", json!("long")),
        field(104, "file_size_in_bytes", json!("long")),
        opt_field(109, "value_counts", int_map(119, 120, "long")),
        opt_field(110, "null_value_counts", int_map(121, 122, "long")),
        opt_field(137, "nan_value_counts", int_map(138, 139, "long")),
        opt_field(125, "lower_bounds", int_map(126, 127, "bytes")),
        opt_field(128, "upper_bounds", int_map(129, 130, "bytes")),
    ];
    if format_version >= 3 {
        data_file.push(opt_field(142, "first_row_id", json!("long")));
    }
    let schema = json!({
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            field(0, "status", json!("int")),
            opt_field(1, "snapshot_id", json!("long")),
            opt_field(3, "sequence_number", json!("long")),
            opt_field(4, "file_sequence_number", json!("long")),
            field(2, "data_file", json!({"type": "record", "name": "r2", "fields": data_file})),
        ],
    });
    parse_schema(&schema)
}

fn manifest_list_schema(format_version: i32) -> Result<AvroSchema> {
    let summary = json!({
        "type": "record",
        "name": "r508",
        "fields": [
            field(509, "contains_null", json!("boolean")),
            opt_field(518, "contains_nan", json!("boolean")),
            opt_field(510, "lower_bound", json!("bytes")),
            opt_field(511, "upper_bound", json!("bytes")),
        ],
    });
    let mut fields = vec![
        field(500, "manifest_path", json!("string")),
        field(501, "manifest_length", json!("long")),
        field(502, "partition_spec_id", json!("int")),
        field(517, "content", json!("int")),
        field(515, "sequence_number", json!("long")),
        field(516, "min_sequence_number", json!("long")),
        field(503, "added_snapshot_id", json!("long")),
        field(504, "added_files_count", json!("int")),
        field(505, "existing_files_count", json!("int")),
        field(506, "deleted_files_count", json!("int")),
        field(512, "added_rows_count", json!("long")),
        field(513, "existing_rows_count", json!("long")),
        field(514, "deleted_rows_count", json!("long")),
        opt_field(
            507,
            "partitions",
            json!({"type": "array", "items": summary, "element-id": 508}),
        ),
    ];
    if format_version >= 3 {
        fields.push(opt_field(520, "first_row_id", json!("long")));
    }
    parse_schema(&json!({"type": "record", "name": "manifest_file", "fields": fields}))
}

fn union(v: Option<Value>) -> Value {
    match v {
        Some(v) => Value::Union(1, Box::new(v)),
        None => Value::Union(0, Box::new(Value::Null)),
    }
}

fn avro_map<V: Clone>(map: &BTreeMap<i32, V>, to_value: impl Fn(V) -> Value) -> Value {
    union(Some(Value::Array(
        map.iter()
            .map(|(k, v)| {
                Value::Record(vec![
                    ("key".into(), Value::Int(*k)),
                    ("value".into(), to_value(v.clone())),
                ])
            })
            .collect(),
    )))
}

fn record(fields: Vec<(&str, Value)>) -> Value {
    Value::Record(fields.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
}

fn entry_value(e: &ManifestEntry, spec: &PartitionSpec, format_version: i32) -> Value {
    let f = &e.data_file;
    let partition = spec
        .fields
        .iter()
        .zip(&f.partition)
        .map(|(p, v)| (avro_name(&p.name), union(v.as_ref().map(Datum::to_avro))))
        .collect();
    let mut data_file = vec![
        ("content", Value::Int(f.content)),
        ("file_path", Value::String(f.file_path.clone())),
        ("file_format", Value::String(f.file_format.clone())),
        ("partition", Value::Record(partition)),
        ("record_count", Value::Long(f.record_count)),
        ("file_size_in_bytes", Value::Long(f.file_size_in_bytes)),
        ("value_counts", avro_map(&f.value_counts, Value::Long)),
        (
            "null_value_counts",
            avro_map(&f.null_value_counts, Value::Long),
        ),
        (
            "nan_value_counts",
            avro_map(&f.nan_value_counts, Value::Long),
        ),
        ("lower_bounds", avro_map(&f.lower_bounds, Value::Bytes)),
        ("upper_bounds", avro_map(&f.upper_bounds, Value::Bytes)),
    ];
    if format_version >= 3 {
        data_file.push(("first_row_id", union(f.first_row_id.map(Value::Long))));
    }
    record(vec![
        ("status", Value::Int(e.status)),
        ("snapshot_id", union(e.snapshot_id.map(Value::Long))),
        ("sequence_number", union(e.sequence_number.map(Value::Long))),
        (
            "file_sequence_number",
            union(e.file_sequence_number.map(Value::Long)),
        ),
        ("data_file", record(data_file)),
    ])
}

pub fn write_manifest(
    entries: &[ManifestEntry],
    schema: &Schema,
    spec: &PartitionSpec,
    format_version: i32,
) -> Result<Vec<u8>> {
    let types = spec.partition_type(schema)?;
    let avro = manifest_schema(spec, &types, format_version)?;
    let mut w = Writer::new(&avro, Vec::new())?;
    w.add_user_metadata("schema".into(), serde_json::to_string(schema)?)?;
    w.add_user_metadata("schema-id".into(), schema.schema_id.to_string())?;
    w.add_user_metadata(
        "partition-spec".into(),
        serde_json::to_string(&spec.fields)?,
    )?;
    w.add_user_metadata("partition-spec-id".into(), spec.spec_id.to_string())?;
    w.add_user_metadata("format-version".into(), format_version.to_string())?;
    w.add_user_metadata("content".into(), "data")?;
    for e in entries {
        w.append_value(entry_value(e, spec, format_version))?;
    }
    Ok(w.into_inner()?)
}

pub struct ManifestListHeader {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub first_row_id: Option<i64>,
}

pub fn write_manifest_list(
    manifests: &[ManifestFile],
    header: &ManifestListHeader,
    format_version: i32,
) -> Result<Vec<u8>> {
    let avro = manifest_list_schema(format_version)?;
    let mut w = Writer::new(&avro, Vec::new())?;
    w.add_user_metadata("snapshot-id".into(), header.snapshot_id.to_string())?;
    let parent = header
        .parent_snapshot_id
        .map_or("null".to_owned(), |p| p.to_string());
    w.add_user_metadata("parent-snapshot-id".into(), parent)?;
    w.add_user_metadata("sequence-number".into(), header.sequence_number.to_string())?;
    w.add_user_metadata("format-version".into(), format_version.to_string())?;
    if let Some(id) = header.first_row_id {
        w.add_user_metadata("first-row-id".into(), id.to_string())?;
    }
    for m in manifests {
        let summaries = m
            .partitions
            .iter()
            .map(|s| {
                record(vec![
                    ("contains_null", Value::Boolean(s.contains_null)),
                    ("contains_nan", union(s.contains_nan.map(Value::Boolean))),
                    (
                        "lower_bound",
                        union(s.lower_bound.clone().map(Value::Bytes)),
                    ),
                    (
                        "upper_bound",
                        union(s.upper_bound.clone().map(Value::Bytes)),
                    ),
                ])
            })
            .collect();
        let mut fields = vec![
            ("manifest_path", Value::String(m.path.clone())),
            ("manifest_length", Value::Long(m.length)),
            ("partition_spec_id", Value::Int(m.partition_spec_id)),
            ("content", Value::Int(m.content)),
            ("sequence_number", Value::Long(m.sequence_number)),
            ("min_sequence_number", Value::Long(m.min_sequence_number)),
            ("added_snapshot_id", Value::Long(m.added_snapshot_id)),
            ("added_files_count", Value::Int(m.added_files_count)),
            ("existing_files_count", Value::Int(m.existing_files_count)),
            ("deleted_files_count", Value::Int(m.deleted_files_count)),
            ("added_rows_count", Value::Long(m.added_rows_count)),
            ("existing_rows_count", Value::Long(m.existing_rows_count)),
            ("deleted_rows_count", Value::Long(m.deleted_rows_count)),
            ("partitions", union(Some(Value::Array(summaries)))),
        ];
        if format_version >= 3 {
            fields.push(("first_row_id", union(m.first_row_id.map(Value::Long))));
        }
        w.append_value(record(fields))?;
    }
    Ok(w.into_inner()?)
}

type Fields<'a> = HashMap<&'a str, &'a Value>;

fn strip_union(v: &Value) -> &Value {
    match v {
        Value::Union(_, inner) => inner,
        other => other,
    }
}

fn fields(v: &Value) -> Result<Fields<'_>> {
    match strip_union(v) {
        Value::Record(fs) => Ok(fs.iter().map(|(k, v)| (k.as_str(), v)).collect()),
        other => invalid(format!("expected avro record, got {other:?}")),
    }
}

fn optional<'a>(r: &Fields<'a>, name: &str) -> Option<&'a Value> {
    r.get(name)
        .map(|v| strip_union(v))
        .filter(|v| !matches!(v, Value::Null))
}

fn required<'a>(r: &Fields<'a>, name: &str) -> Result<&'a Value> {
    optional(r, name).ok_or_else(|| Error::Invalid(format!("missing avro field {name}")))
}

fn as_i32(v: &Value) -> Result<i32> {
    match v {
        Value::Int(x) => Ok(*x),
        other => invalid(format!("expected int, got {other:?}")),
    }
}

fn as_i64(v: &Value) -> Result<i64> {
    match v {
        Value::Long(x) => Ok(*x),
        Value::Int(x) => Ok(*x as i64),
        other => invalid(format!("expected long, got {other:?}")),
    }
}

fn as_str(v: &Value) -> Result<String> {
    match v {
        Value::String(x) => Ok(x.clone()),
        other => invalid(format!("expected string, got {other:?}")),
    }
}

fn as_bytes(v: &Value) -> Result<Vec<u8>> {
    match v {
        Value::Bytes(x) => Ok(x.clone()),
        other => invalid(format!("expected bytes, got {other:?}")),
    }
}

fn read_map<V>(
    r: &Fields<'_>,
    name: &str,
    read: impl Fn(&Value) -> Result<V>,
) -> Result<BTreeMap<i32, V>> {
    let Some(v) = optional(r, name) else {
        return Ok(BTreeMap::new());
    };
    let Value::Array(items) = v else {
        return invalid(format!("expected array for {name}"));
    };
    items
        .iter()
        .map(|item| {
            let kv = fields(item)?;
            Ok((
                as_i32(required(&kv, "key")?)?,
                read(required(&kv, "value")?)?,
            ))
        })
        .collect()
}

/// Reads a manifest and resolves the values that added entries inherit from `manifest`:
/// snapshot id, sequence numbers and, in v3, `first_row_id`.
pub fn read_manifest(
    bytes: &[u8],
    partition_types: &[Type],
    manifest: &ManifestFile,
) -> Result<Vec<ManifestEntry>> {
    let mut next_row_id = manifest.first_row_id;
    let mut entries = Vec::new();
    for value in Reader::new(bytes)? {
        let value = value?;
        let e = fields(&value)?;
        let status = as_i32(required(&e, "status")?)?;
        let added = status == STATUS_ADDED;
        let inherit = |name: &str, inherited: i64| -> Result<Option<i64>> {
            match optional(&e, name) {
                Some(v) => as_i64(v).map(Some),
                None => Ok(added.then_some(inherited)),
            }
        };
        let snapshot_id = inherit("snapshot_id", manifest.added_snapshot_id)?;
        let sequence_number = inherit("sequence_number", manifest.sequence_number)?;
        let file_sequence_number = inherit("file_sequence_number", manifest.sequence_number)?;

        let f = fields(required(&e, "data_file")?)?;
        let partition = read_partition(required(&f, "partition")?, partition_types)?;
        let record_count = as_i64(required(&f, "record_count")?)?;
        let mut first_row_id = optional(&f, "first_row_id").map(as_i64).transpose()?;
        // Only a file that inherits consumes row ids; one with a value keeps its earlier range.
        if status != STATUS_DELETED && first_row_id.is_none() {
            first_row_id = next_row_id;
            next_row_id = next_row_id.map(|id| id + record_count);
        }
        entries.push(ManifestEntry {
            status,
            snapshot_id,
            sequence_number,
            file_sequence_number,
            data_file: DataFile {
                content: optional(&f, "content").map_or(Ok(CONTENT_DATA), as_i32)?,
                file_path: as_str(required(&f, "file_path")?)?,
                file_format: as_str(required(&f, "file_format")?)?,
                partition,
                record_count,
                file_size_in_bytes: as_i64(required(&f, "file_size_in_bytes")?)?,
                value_counts: read_map(&f, "value_counts", as_i64)?,
                null_value_counts: read_map(&f, "null_value_counts", as_i64)?,
                nan_value_counts: read_map(&f, "nan_value_counts", as_i64)?,
                lower_bounds: read_map(&f, "lower_bounds", as_bytes)?,
                upper_bounds: read_map(&f, "upper_bounds", as_bytes)?,
                first_row_id,
            },
        });
    }
    Ok(entries)
}

fn read_partition(v: &Value, types: &[Type]) -> Result<Vec<Option<Datum>>> {
    let Value::Record(fs) = strip_union(v) else {
        return invalid("expected partition record");
    };
    if fs.len() != types.len() {
        return invalid(format!(
            "partition has {} fields, want {}",
            fs.len(),
            types.len()
        ));
    }
    fs.iter()
        .zip(types)
        .map(|((_, v), ty)| match strip_union(v) {
            Value::Null => Ok(None),
            v => Datum::from_avro(*ty, v).map(Some),
        })
        .collect()
}

pub fn read_manifest_list(bytes: &[u8]) -> Result<Vec<ManifestFile>> {
    let mut manifests = Vec::new();
    for value in Reader::new(bytes)? {
        let value = value?;
        let m = fields(&value)?;
        let int = |n: &str| as_i32(required(&m, n)?);
        let long = |n: &str| as_i64(required(&m, n)?);
        // Some writers use the older `*_data_files_count` names for field ids 504 to 506.
        let count = |kind: &str| {
            let legacy = format!("{kind}_data_files_count");
            match optional(&m, &legacy) {
                Some(v) => as_i32(v),
                None => int(&format!("{kind}_files_count")),
            }
        };
        let partitions = match optional(&m, "partitions") {
            Some(Value::Array(items)) => items
                .iter()
                .map(|item| {
                    let s = fields(item)?;
                    let flag = |n: &str| match optional(&s, n) {
                        Some(Value::Boolean(b)) => Some(*b),
                        _ => None,
                    };
                    Ok(FieldSummary {
                        contains_null: flag("contains_null").unwrap_or(true),
                        contains_nan: flag("contains_nan"),
                        lower_bound: optional(&s, "lower_bound").map(as_bytes).transpose()?,
                        upper_bound: optional(&s, "upper_bound").map(as_bytes).transpose()?,
                    })
                })
                .collect::<Result<_>>()?,
            _ => vec![],
        };
        manifests.push(ManifestFile {
            path: as_str(required(&m, "manifest_path")?)?,
            length: long("manifest_length")?,
            partition_spec_id: int("partition_spec_id")?,
            content: int("content")?,
            sequence_number: long("sequence_number")?,
            min_sequence_number: long("min_sequence_number")?,
            added_snapshot_id: long("added_snapshot_id")?,
            added_files_count: count("added")?,
            existing_files_count: count("existing")?,
            deleted_files_count: count("deleted")?,
            added_rows_count: long("added_rows_count")?,
            existing_rows_count: long("existing_rows_count")?,
            deleted_rows_count: long("deleted_rows_count")?,
            partitions,
            first_row_id: optional(&m, "first_row_id").map(as_i64).transpose()?,
        });
    }
    Ok(manifests)
}
