use toy_iceberg_core::manifest::{
    DataFile, ManifestEntry, ManifestFile, ManifestListHeader, STATUS_ADDED, STATUS_EXISTING,
    read_manifest, read_manifest_list, write_manifest, write_manifest_list,
};
use toy_iceberg_core::partition::PartitionSpec;
use toy_iceberg_core::types::{Field, Schema, Type};
use toy_iceberg_core::values::Datum;

fn testdata(name: &str) -> Vec<u8> {
    let dir = env!("CARGO_MANIFEST_DIR");
    std::fs::read(format!("{dir}/testdata/iceberg-rust/manifests/{name}")).unwrap()
}

fn schema() -> Schema {
    let field = |id, name: &str, ty, required| Field {
        id,
        name: name.into(),
        ty,
        required,
        doc: None,
        extra: Default::default(),
    };
    Schema::new(vec![
        field(1, "id", Type::Long, true),
        field(2, "ts", Type::Timestamp, false),
        field(3, "region", Type::String, false),
    ])
    .unwrap()
}

fn data_file(path: &str, rows: i64, partition: Vec<Option<Datum>>) -> DataFile {
    DataFile {
        content: 0,
        file_path: path.into(),
        file_format: "PARQUET".into(),
        partition,
        record_count: rows,
        file_size_in_bytes: 100,
        value_counts: [(1, rows)].into(),
        null_value_counts: [(1, 0)].into(),
        nan_value_counts: Default::default(),
        lower_bounds: [(1, Datum::Long(1).to_bytes())].into(),
        upper_bounds: [(1, Datum::Long(9).to_bytes())].into(),
        first_row_id: None,
    }
}

fn manifest_file(first_row_id: Option<i64>) -> ManifestFile {
    ManifestFile {
        path: "m.avro".into(),
        length: 1,
        partition_spec_id: 0,
        content: 0,
        sequence_number: 7,
        min_sequence_number: 7,
        added_snapshot_id: 42,
        added_files_count: 2,
        existing_files_count: 0,
        deleted_files_count: 0,
        added_rows_count: 30,
        existing_rows_count: 0,
        deleted_rows_count: 0,
        partitions: vec![],
        first_row_id,
    }
}

fn added(file: DataFile) -> ManifestEntry {
    ManifestEntry {
        status: STATUS_ADDED,
        snapshot_id: None,
        sequence_number: None,
        file_sequence_number: None,
        data_file: file,
    }
}

#[test]
fn reads_a_manifest_list_with_legacy_count_names() {
    let manifests = read_manifest_list(&testdata("manifest-list-v2-1.avro")).unwrap();
    assert!(!manifests.is_empty());
    for m in &manifests {
        assert!(m.path.ends_with(".avro"), "{}", m.path);
        assert!(m.length > 0);
    }
}

#[test]
fn reads_a_manifest_written_by_pyiceberg() {
    let bytes = testdata("pyiceberg-v2-data.avro");
    let header = apache_avro_user_metadata(&bytes, "partition-spec");
    assert!(header.contains(r#""name":"category""#), "{header}");
    let entries = read_manifest(&bytes, &[Type::String], &manifest_file(None)).unwrap();
    assert!(!entries.is_empty());
    for e in &entries {
        assert!(e.data_file.file_path.ends_with(".parquet"));
        assert!(e.data_file.record_count > 0);
        assert!(!e.data_file.value_counts.is_empty());
        assert!(matches!(
            e.data_file.partition[..],
            [Some(Datum::String(_))]
        ));
    }
}

/// Finds a key in the Avro file header without a dependency on the Avro crate.
fn apache_avro_user_metadata(bytes: &[u8], key: &str) -> String {
    let start = bytes
        .windows(key.len())
        .position(|w| w == key.as_bytes())
        .unwrap()
        + key.len();
    let (mut len, mut shift, mut i) = (0u64, 0, start);
    loop {
        len |= ((bytes[i] & 0x7f) as u64) << shift;
        shift += 7;
        i += 1;
        if bytes[i - 1] & 0x80 == 0 {
            break;
        }
    }
    let len = (len >> 1) as usize;
    String::from_utf8(bytes[i..i + len].to_vec()).unwrap()
}

#[test]
fn added_entries_inherit_from_the_manifest_list() {
    let spec = PartitionSpec::parse("region", &schema()).unwrap();
    let region = |s: &str| vec![Some(Datum::String(s.into()))];
    let entries = [
        added(data_file("s3://b/1.parquet", 10, region("jp"))),
        added(data_file("s3://b/2.parquet", 20, vec![None])),
    ];
    let bytes = write_manifest(&entries, &schema(), &spec, 2).unwrap();
    let read = read_manifest(&bytes, &[Type::String], &manifest_file(None)).unwrap();

    assert_eq!(read.len(), 2);
    assert_eq!(read[0].snapshot_id, Some(42));
    assert_eq!(read[0].sequence_number, Some(7));
    assert_eq!(read[0].file_sequence_number, Some(7));
    assert_eq!(read[0].data_file.partition, region("jp"));
    assert_eq!(read[1].data_file.partition, vec![None]);
    assert_eq!(
        read[0].data_file.lower_bounds,
        entries[0].data_file.lower_bounds
    );
}

#[test]
fn existing_entries_do_not_inherit() {
    let spec = PartitionSpec::unpartitioned();
    let entry = ManifestEntry {
        status: STATUS_EXISTING,
        snapshot_id: Some(1),
        sequence_number: Some(2),
        file_sequence_number: Some(3),
        data_file: data_file("s3://b/1.parquet", 10, vec![]),
    };
    let bytes = write_manifest(&[entry], &schema(), &spec, 2).unwrap();
    let read = read_manifest(&bytes, &[], &manifest_file(None)).unwrap();
    assert_eq!(read[0].snapshot_id, Some(1));
    assert_eq!(read[0].sequence_number, Some(2));
    assert_eq!(read[0].file_sequence_number, Some(3));
}

#[test]
fn v3_first_row_id_is_inherited_in_file_order() {
    let spec = PartitionSpec::unpartitioned();
    let entries = [
        added(data_file("s3://b/1.parquet", 10, vec![])),
        added(data_file("s3://b/2.parquet", 20, vec![])),
    ];
    let bytes = write_manifest(&entries, &schema(), &spec, 3).unwrap();
    let read = read_manifest(&bytes, &[], &manifest_file(Some(1000))).unwrap();
    assert_eq!(read[0].data_file.first_row_id, Some(1000));
    assert_eq!(read[1].data_file.first_row_id, Some(1010));
}

#[test]
fn all_partition_types_round_trip() {
    let schema = Schema::new(vec![
        Field {
            id: 1,
            name: "b".into(),
            ty: Type::Boolean,
            required: false,
            doc: None,
            extra: Default::default(),
        },
        Field {
            id: 2,
            name: "d".into(),
            ty: Type::Date,
            required: false,
            doc: None,
            extra: Default::default(),
        },
        Field {
            id: 3,
            name: "t".into(),
            ty: Type::Timestamptz,
            required: false,
            doc: None,
            extra: Default::default(),
        },
        Field {
            id: 4,
            name: "x".into(),
            ty: Type::Binary,
            required: false,
            doc: None,
            extra: Default::default(),
        },
        Field {
            id: 5,
            name: "f".into(),
            ty: Type::Double,
            required: false,
            doc: None,
            extra: Default::default(),
        },
    ])
    .unwrap();
    let spec = PartitionSpec::parse("b, d, t, x, f", &schema).unwrap();
    let partition = vec![
        Some(Datum::Boolean(true)),
        Some(Datum::Date(-1)),
        Some(Datum::Timestamptz(123)),
        Some(Datum::Binary(vec![1, 2])),
        Some(Datum::Double(1.5)),
    ];
    let entry = added(data_file("s3://b/1.parquet", 10, partition.clone()));
    let bytes = write_manifest(&[entry], &schema, &spec, 2).unwrap();
    let types = spec.partition_type(&schema).unwrap();
    let read = read_manifest(&bytes, &types, &manifest_file(None)).unwrap();
    assert_eq!(read[0].data_file.partition, partition);
}

#[test]
fn manifest_list_round_trip() {
    let header = ManifestListHeader {
        snapshot_id: 42,
        parent_snapshot_id: None,
        sequence_number: 7,
        first_row_id: Some(5),
    };
    let manifest = manifest_file(Some(5));
    let bytes = write_manifest_list(std::slice::from_ref(&manifest), &header, 3).unwrap();
    assert_eq!(read_manifest_list(&bytes).unwrap(), vec![manifest]);
}
