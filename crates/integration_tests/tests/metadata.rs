// Ported from apache/iceberg-rust, licensed under the Apache License, Version 2.0.
// http://www.apache.org/licenses/LICENSE-2.0

use toy_iceberg_core::metadata::TableMetadata;

fn read(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/testdata/iceberg-rust/table_metadata/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(path).unwrap()
}

#[test]
fn parses_v2_valid() {
    let m = TableMetadata::parse(&read("TableMetadataV2Valid")).unwrap();
    assert_eq!(m.format_version, 2);
    assert_eq!(m.table_uuid, "9c12d441-03fe-4693-9a96-a0705ddf69c1");
    assert_eq!(m.location, "s3://bucket/test/location");
    assert_eq!(m.last_sequence_number, 34);
    assert_eq!(m.last_column_id, 3);
    assert_eq!(m.current_schema().schema_id, 1);
    assert_eq!(m.current_schema().identifier_field_ids, vec![1, 2]);
    assert_eq!(m.default_spec().fields[0].field_id, 1000);
    assert_eq!(
        m.current_snapshot().unwrap().snapshot_id,
        3055729675574597004
    );
    assert_eq!(m.snapshots.len(), 2);
}

#[test]
fn valid_files_round_trip() {
    for name in [
        "TableMetadataV2Valid",
        "TableMetadataV2ValidMinimal",
        "TableMetadataV3ValidMinimal",
        "TableMetadataV3ValidEncryption",
    ] {
        let m = TableMetadata::parse(&read(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
        let again = TableMetadata::parse(&serde_json::to_vec(&m).unwrap()).unwrap();
        assert_eq!(m, again, "{name}");
    }
}

#[test]
fn unknown_fields_survive_a_rewrite() {
    let m = TableMetadata::parse(&read("TableMetadataV3ValidEncryption")).unwrap();
    assert!(m.extra.contains_key("encryption-keys"));
    let json: serde_json::Value = serde_json::to_value(&m).unwrap();
    assert!(json.get("encryption-keys").is_some());
}

#[test]
fn fields_this_implementation_does_not_model_survive_a_rewrite() {
    let bytes = read("TableMetadataV3ValidEncryption");
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let rewritten = serde_json::to_value(TableMetadata::parse(&bytes).unwrap()).unwrap();
    assert_eq!(rewritten["snapshots"], original["snapshots"]);

    let mut with_default = original.clone();
    with_default["schemas"][0]["fields"][0]["write-default"] = 7.into();
    let bytes = serde_json::to_vec(&with_default).unwrap();
    let rewritten = serde_json::to_value(TableMetadata::parse(&bytes).unwrap()).unwrap();
    assert_eq!(rewritten["schemas"][0]["fields"][0]["write-default"], 7);
}

#[test]
fn snapshots_from_before_an_upgrade_from_v1_default_to_sequence_number_zero() {
    let mut json: serde_json::Value =
        serde_json::from_slice(&read("TableMetadataV2Valid")).unwrap();
    json["snapshots"][0]
        .as_object_mut()
        .unwrap()
        .remove("sequence-number");
    let m = TableMetadata::parse(&serde_json::to_vec(&json).unwrap()).unwrap();
    assert_eq!(m.snapshots[0].sequence_number, 0);
}

#[test]
fn rejects_invalid_files() {
    for name in [
        "TableMetadataUnsupportedVersion",
        "TableMetadataV2CurrentSchemaNotFound",
        "TableMetadataV2MissingLastPartitionId",
        "TableMetadataV2MissingPartitionSpecs",
        "TableMetadataV2MissingSchemas",
        "TableMetadataV2MissingSortOrder",
    ] {
        assert!(
            TableMetadata::parse(&read(name)).is_err(),
            "{name} must be rejected"
        );
    }
}

#[test]
fn v1_is_out_of_scope() {
    assert!(TableMetadata::parse(&read("TableMetadataV1Valid")).is_err());
}
