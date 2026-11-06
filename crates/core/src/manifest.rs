//! manifest と manifest list の Avro 表現。仕様の「Manifests」「Manifest Lists」を実装する。
//!
//! manifest は data file の一覧で、ファイルごとにパーティション値と列統計を持つ。
//! manifest list は1つの snapshot に属する manifest の一覧で、manifest ごとにパーティションの
//! 要約を持つ。読み手はこの2段の要約を使い、data file を開かずに読む対象を絞る。
//!
//! 書くときのスキーマは仕様の field id に従う。読むときはフィールド名で探すので、
//! 他の実装が余分なフィールドを足したファイルも解析できる。
use std::collections::{BTreeMap, HashMap};

use apache_avro::schema::UnionSchema;
use apache_avro::types::Value;
use apache_avro::{Reader, Schema as AvroSchema, Writer};
use serde_json::{Value as Json, json};

use crate::error::{Error, Result, invalid};
use crate::partition::PartitionSpec;
use crate::types::{Schema, Type};
use crate::values::Datum;

// manifest entry の状態。仕様の「Manifest Entry Fields」の `status` にあたる。
// `ADDED` はこの manifest を書いた snapshot が追加したファイル、`EXISTING` は以前の snapshot から
// 引き継いだファイル、`DELETED` はこの snapshot で削除したファイルを表す。
pub const STATUS_EXISTING: i32 = 0;
pub const STATUS_ADDED: i32 = 1;
pub const STATUS_DELETED: i32 = 2;
// データファイルの種別。1 と 2 は delete file で、この実装は読み書きしない。
pub const CONTENT_DATA: i32 = 0;

/// manifest 内の全ファイルについて、パーティションタプルの1要素を要約した値。
/// 仕様の「Manifest Lists」の `field_summary` にあたり、manifest ごとの pruning に使う。
/// 上下限には null と NaN を含めず、その有無を別のフラグで持つ。
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FieldSummary {
    pub contains_null: bool,
    pub contains_nan: Option<bool>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
}

/// manifest list の1行。1つの manifest の場所と要約を持つ。
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestFile {
    pub path: String,
    pub length: i64,
    pub partition_spec_id: i32,
    pub content: i32,
    /// manifest 内の `null` の sequence number は、読むときにこの値で補う。
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    pub added_files_count: i32,
    pub existing_files_count: i32,
    pub deleted_files_count: i32,
    pub added_rows_count: i64,
    pub existing_rows_count: i64,
    pub deleted_rows_count: i64,
    /// パーティションタプルと同じ順序で並ぶ。
    pub partitions: Vec<FieldSummary>,
    /// v3 の row lineage で、この manifest の先頭のファイルに割り当てる row id。
    pub first_row_id: Option<i64>,
}

/// 1つの data file の情報。仕様の「Manifest Entry Fields」の `data_file` にあたる。
#[derive(Debug, Clone, PartialEq)]
pub struct DataFile {
    pub content: i32,
    pub file_path: String,
    pub file_format: String,
    /// ファイル内の全行に共通のパーティションタプル。
    pub partition: Vec<Option<Datum>>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    /// 以降の統計のキーは列の field id。`value_counts` は null と NaN も数える。
    pub value_counts: BTreeMap<i32, i64>,
    pub null_value_counts: BTreeMap<i32, i64>,
    pub nan_value_counts: BTreeMap<i32, i64>,
    /// 上下限は [`Datum::to_bytes`] のバイト列で持ち、null と NaN を含めない。
    pub lower_bounds: BTreeMap<i32, Vec<u8>>,
    pub upper_bounds: BTreeMap<i32, Vec<u8>>,
    /// v3 の row lineage で、このファイルの先頭の行の row id。
    pub first_row_id: Option<i64>,
}

/// manifest の1行。
///
/// `snapshot_id` と2つの sequence number が `Option` なのは、仕様が `null` を「manifest list の値を
/// 継承する」という意味に定めているからである（「Sequence Number Inheritance」）。
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestEntry {
    pub status: i32,
    pub snapshot_id: Option<i64>,
    /// データの論理的な新しさを表す。delete file の適用範囲の判定に使う値である。
    pub sequence_number: Option<i64>,
    /// ファイルをテーブルに追加したコミットの番号。
    pub file_sequence_number: Option<i64>,
    pub data_file: DataFile,
}

fn opt(ty: Json) -> Json {
    json!(["null", ty])
}

/// 仕様の「Appendix A」の Avro の節は、各フィールドに `field-id` を付けることを求める。
fn field(id: i32, name: &str, ty: Json) -> Json {
    json!({"name": name, "type": ty, "field-id": id})
}

fn opt_field(id: i32, name: &str, ty: Json) -> Json {
    json!({"name": name, "type": opt(ty), "default": null, "field-id": id})
}

/// Avro の map はキーが文字列に限られる。そのため Iceberg は、キーが int の map を
/// key と value のレコードの配列として保存する。
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

/// 仕様の「Appendix A」の Avro 型の対応。`adjust-to-utc` が timestamp と timestamptz を区別する。
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

/// `apache_avro` の `Schema::parse` は配列のカスタム属性を捨てるので、解析後に復元する。
/// 読み手はこの属性を必要とする。仕様は int キーの map に `logicalType: map` を要求し、
/// リストの要素は `element-id` で field id を持つからである。
fn parse_schema(json: &Json) -> Result<AvroSchema> {
    let mut schema = AvroSchema::parse(json)?;
    restore_array_attributes(&mut schema)?;
    Ok(schema)
}

/// 配列の要素レコードの名前で属性を決める。この実装が書く配列のうち、リストは manifest list の
/// `partitions`（要素 `r508`）だけで、それ以外は [`int_map`] が作った map である。
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

/// パーティション名を Avro のフィールド名に使える形に変える。Avro の名前は使える文字が限られる。
/// Java の `AvroSchemaUtil.makeCompatibleName` と同じ方式で、元の名前は `iceberg-field-name` に残す。
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

/// manifest の Avro スキーマ。`partition` レコードの形はパーティション定義ごとに変わるので、
/// 固定のスキーマではなく定義から組み立てる。この関数の field id はすべて仕様で決まっている。
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

/// manifest list の Avro スキーマ。この関数の field id も仕様で決まっている。
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

/// manifest を Avro のバイト列にする。
///
/// 仕様の「Manifests」は、スキーマとパーティション定義を Avro ファイルのメタデータに書くことを
/// 求める。これにより、manifest は metadata ファイルがなくても単独で解釈できる。
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

/// manifest list の Avro メタデータに書く値。仕様の「Manifest Lists」が定める。
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

/// manifest を読み、`manifest` から継承する値を解決する。
///
/// 継承するのは snapshot id、2つの sequence number、v3 の `first_row_id` である
/// （「Sequence Number Inheritance」「First Row ID Inheritance」）。sequence number の継承は
/// `ADDED` の entry に限る。`EXISTING` と `DELETED` の entry は値を明示的に持つことを仕様が求めている。
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
        // row id を消費するのは継承するファイルだけで、値を持つファイルは以前の範囲を保つ。
        // 仕様どおり、前のファイルの `record_count` を足して次の値を求める。
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

/// パーティションレコードのフィールドを名前ではなく位置で型と対応づける。
/// 名前は Avro 用に変換されている場合があり（[`avro_name`]）、位置は定義の順序と一致するからである。
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
        // 古い書き手は、field id 504〜506 に `*_data_files_count` という名前を使う。
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
                        // 不明な場合は null があるものとして扱う。pruning で除外しない側である。
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
