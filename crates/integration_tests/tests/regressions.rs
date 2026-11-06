//! Regressions for failures that `/code-review` found. These run on an in-memory store, which
//! implements the same conditional puts, so they need no server.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{AsArray, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field as ArrowField, Int64Type, Schema as ArrowSchema};
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use toy_iceberg_core::Error;
use toy_iceberg_core::catalog::{Catalog, TableIdent};
use toy_iceberg_core::manifest::{
    DataFile, ManifestEntry, ManifestListHeader, STATUS_ADDED, STATUS_DELETED, STATUS_EXISTING,
    read_manifest, read_manifest_list, write_manifest, write_manifest_list,
};
use toy_iceberg_core::partition::{PartitionField, PartitionSpec};
use toy_iceberg_core::table::Table;
use toy_iceberg_core::transform::Transform;
use toy_iceberg_core::types::{Field, Schema, Type};
use toy_iceberg_core::values::Datum;
use toy_iceberg_datafusion::{IcebergSession, session};

fn catalog() -> Arc<Catalog> {
    Catalog::new(Arc::new(InMemory::new()), "warehouse")
}

async fn run(ctx: &IcebergSession, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

async fn count(ctx: &IcebergSession, sql: &str) -> i64 {
    let batches = run(ctx, sql).await.unwrap();
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

async fn table_with_row(ctx: &IcebergSession) {
    run(
        ctx,
        "CREATE EXTERNAL TABLE ns.t (a INT) STORED AS ICEBERG LOCATION ''",
    )
    .await
    .unwrap();
    run(ctx, "INSERT INTO ns.t VALUES (1)").await.unwrap();
}

#[tokio::test]
async fn or_replace_fails_without_touching_the_existing_table() {
    let ctx = session(catalog()).await.unwrap();
    table_with_row(&ctx).await;
    for sql in [
        "CREATE OR REPLACE TABLE ns.t AS SELECT 2 AS a",
        "CREATE OR REPLACE VIEW ns.t AS SELECT 2 AS a",
        "CREATE OR REPLACE EXTERNAL TABLE ns.t (a INT) STORED AS ICEBERG LOCATION '' \
         OPTIONS ('partitioning' 'nope')",
    ] {
        assert!(run(&ctx, sql).await.is_err(), "{sql}");
        assert_eq!(count(&ctx, "SELECT count(*) FROM ns.t").await, 1, "{sql}");
    }
}

#[tokio::test]
async fn statements_this_catalog_cannot_store_fail_instead_of_vanishing() {
    let ctx = session(catalog()).await.unwrap();
    for sql in [
        "CREATE TABLE ns.m (a INT)",
        "CREATE TABLE ns.m AS SELECT 1 AS a",
        "CREATE VIEW ns.m AS SELECT 1 AS a",
    ] {
        assert!(run(&ctx, sql).await.is_err(), "{sql}");
    }
    run(
        &ctx,
        "CREATE EXTERNAL TABLE ns.m (a INT) STORED AS ICEBERG LOCATION ''",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn drop_table_runs_through_the_session_and_respects_if_exists() {
    let ctx = session(catalog()).await.unwrap();
    table_with_row(&ctx).await;
    run(&ctx, "DROP TABLE ns.t").await.unwrap();
    assert!(run(&ctx, "SELECT * FROM ns.t").await.is_err());
    assert!(run(&ctx, "DROP TABLE ns.t").await.is_err());
    run(&ctx, "DROP TABLE IF EXISTS ns.t").await.unwrap();
    run(
        &ctx,
        "CREATE EXTERNAL TABLE ns.t (a INT) STORED AS ICEBERG LOCATION ''",
    )
    .await
    .unwrap();
}

fn int_schema(name: &str) -> Schema {
    let field = Field {
        id: 1,
        name: name.into(),
        ty: Type::Int,
        required: true,
        doc: None,
        extra: Default::default(),
    };
    Schema::new(vec![field]).unwrap()
}

fn int_batch(name: &str, values: Vec<i32>) -> RecordBatch {
    let schema = ArrowSchema::new(vec![ArrowField::new(name, DataType::Int32, false)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int32Array::from(values))]).unwrap()
}

async fn create(catalog: &Arc<Catalog>, ident: &TableIdent, schema: Schema) -> Table {
    let spec = PartitionSpec::unpartitioned();
    catalog
        .create_table(ident, schema, spec, 2, BTreeMap::new(), None)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_stale_handle_cannot_commit_to_a_recreated_table() {
    let catalog = catalog();
    let ident = TableIdent::new("ns", "t");
    let stale = create(&catalog, &ident, int_schema("a")).await;
    catalog.drop_table(&ident).await.unwrap();
    create(&catalog, &ident, int_schema("a")).await;

    let result = stale.append(&[int_batch("a", vec![1])]).await;
    assert!(matches!(result, Err(Error::Invalid(_))), "{result:?}");
    assert!(
        catalog
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .snapshots
            .is_empty()
    );
}

/// Applies the next conditional update, then reports a precondition failure, as a store does
/// when the HTTP client retries a request whose first attempt succeeded.
#[derive(Debug)]
struct LostResponse {
    inner: InMemory,
    armed: AtomicBool,
}

impl fmt::Display for LostResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LostResponse")
    }
}

#[async_trait::async_trait]
impl ObjectStore for LostResponse {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let update = matches!(opts.mode, PutMode::Update(_));
        let result = self.inner.put_opts(location, payload, opts).await?;
        if update && self.armed.swap(false, Ordering::SeqCst) {
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: "retried request".into(),
            });
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn a_commit_that_applied_despite_a_conflict_is_not_applied_twice() {
    let store = Arc::new(LostResponse {
        inner: InMemory::new(),
        armed: AtomicBool::new(false),
    });
    let catalog = Catalog::new(store.clone(), "warehouse");
    let ident = TableIdent::new("ns", "t");
    let table = create(&catalog, &ident, int_schema("a")).await;

    store.armed.store(true, Ordering::SeqCst);
    let snapshot = table
        .append(&[int_batch("a", vec![1])])
        .await
        .unwrap()
        .unwrap();

    let latest = catalog.load_table(&ident).await.unwrap();
    assert_eq!(latest.metadata().snapshots, vec![snapshot]);
    assert_eq!(latest.scan().plan().await.unwrap().tasks.len(), 1);
}

fn occurrences(haystack: &[u8], needle: &str) -> usize {
    haystack
        .windows(needle.len())
        .filter(|w| *w == needle.as_bytes())
        .count()
}

fn empty_file(partition: Vec<Option<Datum>>) -> ManifestEntry {
    ManifestEntry {
        status: STATUS_ADDED,
        snapshot_id: None,
        sequence_number: None,
        file_sequence_number: None,
        data_file: DataFile {
            content: 0,
            file_path: "s3://b/f.parquet".into(),
            file_format: "PARQUET".into(),
            partition,
            record_count: 1,
            file_size_in_bytes: 1,
            value_counts: BTreeMap::new(),
            null_value_counts: BTreeMap::new(),
            nan_value_counts: BTreeMap::new(),
            lower_bounds: BTreeMap::new(),
            upper_bounds: BTreeMap::new(),
            first_row_id: None,
        },
    }
}

#[test]
fn manifest_headers_keep_the_map_logical_type() {
    let schema = int_schema("a");
    let spec = PartitionSpec::unpartitioned();
    let bytes = write_manifest(&[empty_file(vec![])], &schema, &spec, 2).unwrap();
    assert_eq!(occurrences(&bytes, r#""logicalType":"map""#), 5);
}

#[test]
fn manifest_lists_keep_the_element_id() {
    let header = ManifestListHeader {
        snapshot_id: 1,
        parent_snapshot_id: None,
        sequence_number: 1,
        first_row_id: None,
    };
    let bytes = write_manifest_list(&[], &header, 2).unwrap();
    assert_eq!(occurrences(&bytes, r#""element-id":508"#), 1);
}

#[tokio::test]
async fn append_rejects_batches_that_do_not_fit_instead_of_writing_nulls() {
    let catalog = catalog();
    let table = create(&catalog, &TableIdent::new("ns", "t"), int_schema("a")).await;

    let wide = ArrowSchema::new(vec![ArrowField::new("a", DataType::Int64, false)]);
    let too_big = RecordBatch::try_new(
        Arc::new(wide),
        vec![Arc::new(Int64Array::from(vec![5_000_000_000]))],
    )
    .unwrap();
    assert!(table.append(&[too_big]).await.is_err());
    assert!(table.append(&[int_batch("other", vec![1])]).await.is_err());
}

#[tokio::test]
async fn truncate_near_the_type_minimum_stays_correct() {
    assert_eq!(
        Transform::Truncate(10)
            .apply(&Datum::Int(i32::MIN))
            .unwrap(),
        Some(Datum::Int(i32::MIN))
    );
    assert_eq!(
        Transform::Truncate(10)
            .apply(&Datum::Long(i64::MIN))
            .unwrap(),
        Some(Datum::Long(i64::MIN))
    );

    let ctx = session(catalog()).await.unwrap();
    run(
        &ctx,
        "CREATE EXTERNAL TABLE ns.t (i INT) STORED AS ICEBERG LOCATION '' \
         OPTIONS ('partitioning' 'truncate(10, i)')",
    )
    .await
    .unwrap();
    run(&ctx, "INSERT INTO ns.t VALUES (-2147483648), (0), (7)")
        .await
        .unwrap();
    let q = "SELECT count(*) FROM ns.t WHERE i = -2147483648";
    assert_eq!(count(&ctx, q).await, 1);
    let q = "SELECT count(*) FROM ns.t WHERE i >= -2147483648";
    assert_eq!(count(&ctx, q).await, 3);
}

#[test]
fn hour_outside_the_int_range_is_an_error() {
    assert!(Transform::Hour.apply(&Datum::Timestamp(i64::MAX)).is_err());
}

#[tokio::test]
async fn sql_timestamptz_columns_become_iceberg_timestamptz() {
    let catalog = catalog();
    let ctx = session(catalog.clone()).await.unwrap();
    run(
        &ctx,
        "CREATE EXTERNAL TABLE ns.t (a TIMESTAMP WITH TIME ZONE, b TIMESTAMPTZ, c TIMESTAMP) \
         STORED AS ICEBERG LOCATION ''",
    )
    .await
    .unwrap();
    run(
        &ctx,
        "INSERT INTO ns.t VALUES \
         (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00', NULL)",
    )
    .await
    .unwrap();
    let table = catalog
        .load_table(&TableIdent::new("ns", "t"))
        .await
        .unwrap();
    let types: Vec<_> = table.schema().fields.iter().map(|f| f.ty).collect();
    assert_eq!(
        types,
        vec![Type::Timestamptz, Type::Timestamptz, Type::Timestamp]
    );
    assert_eq!(count(&ctx, "SELECT count(*) FROM ns.t").await, 1);
}

#[tokio::test]
async fn partition_columns_with_names_avro_rejects_can_be_written() {
    let ctx = session(catalog()).await.unwrap();
    run(
        &ctx,
        r#"CREATE EXTERNAL TABLE ns.t ("user-id" BIGINT, "地域" VARCHAR) STORED AS ICEBERG
           LOCATION '' OPTIONS ('partitioning' 'bucket(4, "user-id"), "地域"')"#,
    )
    .await
    .expect_err("quoted names are not part of the partition expression syntax");
    run(
        &ctx,
        r#"CREATE EXTERNAL TABLE ns.t ("user-id" BIGINT, "地域" VARCHAR) STORED AS ICEBERG
           LOCATION '' OPTIONS ('partitioning' 'bucket(4, user-id), 地域')"#,
    )
    .await
    .unwrap();
    run(&ctx, "INSERT INTO ns.t VALUES (1, '東京')")
        .await
        .unwrap();
    assert_eq!(
        count(&ctx, r#"SELECT count(*) FROM ns.t WHERE "地域" = '東京'"#).await,
        1
    );
}

#[tokio::test]
async fn long_partition_values_stay_within_key_segment_limits() {
    let catalog = catalog();
    let ctx = session(catalog.clone()).await.unwrap();
    run(
        &ctx,
        "CREATE EXTERNAL TABLE ns.t (s VARCHAR) STORED AS ICEBERG LOCATION '' \
         OPTIONS ('partitioning' 's')",
    )
    .await
    .unwrap();
    let long = "x".repeat(1000);
    run(&ctx, &format!("INSERT INTO ns.t VALUES ('{long}')"))
        .await
        .unwrap();
    assert_eq!(
        count(
            &ctx,
            &format!("SELECT count(*) FROM ns.t WHERE s = '{long}'")
        )
        .await,
        1
    );

    let table = catalog
        .load_table(&TableIdent::new("ns", "t"))
        .await
        .unwrap();
    let path = table
        .scan()
        .plan()
        .await
        .unwrap()
        .tasks
        .remove(0)
        .data_file
        .file_path;
    assert!(
        path.split('/').all(|segment| segment.len() <= 128),
        "{path}"
    );
}

#[tokio::test]
async fn reserved_characters_are_rejected_at_creation() {
    let ctx = session(catalog()).await.unwrap();
    for name in ["a$b", "a@1", "base$snapshots"] {
        let sql =
            format!(r#"CREATE EXTERNAL TABLE ns."{name}" (a INT) STORED AS ICEBERG LOCATION ''"#);
        assert!(run(&ctx, &sql).await.is_err(), "{name}");
    }
}

#[tokio::test]
async fn unqualified_tables_land_in_the_session_default_namespace() {
    let ctx = session(catalog()).await.unwrap();
    run(&ctx, "SET datafusion.catalog.default_schema = 'foo'")
        .await
        .unwrap();
    run(
        &ctx,
        "CREATE EXTERNAL TABLE t (a INT) STORED AS ICEBERG LOCATION ''",
    )
    .await
    .unwrap();
    run(&ctx, "INSERT INTO t VALUES (1)").await.unwrap();
    assert_eq!(count(&ctx, "SELECT count(*) FROM foo.t").await, 1);
}

#[tokio::test]
async fn column_defaults_are_refused_rather_than_ignored() {
    let ctx = session(catalog()).await.unwrap();
    let sql = "CREATE EXTERNAL TABLE ns.t (a INT, b INT DEFAULT 7) STORED AS ICEBERG LOCATION ''";
    assert!(run(&ctx, sql).await.is_err());
}

#[test]
fn partition_names_cannot_shadow_another_column() {
    let field = |id, name: &str, ty| Field {
        id,
        name: name.into(),
        ty,
        required: false,
        doc: None,
        extra: Default::default(),
    };
    let schema = Schema::new(vec![
        field(1, "ts", Type::Timestamp),
        field(2, "ts_day", Type::Int),
    ])
    .unwrap();
    assert!(PartitionSpec::parse("day(ts)", &schema).is_err());
    assert!(PartitionSpec::parse("ts_day", &schema).is_ok());
}

#[tokio::test]
async fn a_commit_keeps_what_other_writers_stored_on_the_main_ref() {
    let catalog = catalog();
    let ident = TableIdent::new("ns", "t");
    let table = create(&catalog, &ident, int_schema("a")).await;
    table.append(&[int_batch("a", vec![1])]).await.unwrap();

    let table = catalog.load_table(&ident).await.unwrap();
    let mut metadata = table.metadata().clone();
    metadata.refs.get_mut("main").unwrap()["min-snapshots-to-keep"] = 3.into();
    let location = format!("{}/metadata/foreign.metadata.json", metadata.location);
    catalog
        .write(&location, serde_json::to_vec(&metadata).unwrap())
        .await
        .unwrap();
    let pointer = serde_json::json!({"metadata-location": location}).to_string();
    let path = Path::from("_catalog/ns/t.json");
    catalog.store().put(&path, pointer.into()).await.unwrap();

    let snapshot = table
        .append(&[int_batch("a", vec![2])])
        .await
        .unwrap()
        .unwrap();
    let latest = catalog.load_table(&ident).await.unwrap();
    let main = &latest.metadata().refs["main"];
    assert_eq!(main["min-snapshots-to-keep"], 3);
    assert_eq!(main["snapshot-id"], snapshot.snapshot_id);
}

#[test]
fn only_files_that_inherit_consume_row_ids() {
    let schema = int_schema("a");
    let spec = PartitionSpec::unpartitioned();
    let file = |rows, first_row_id| {
        let mut entry = empty_file(vec![]);
        entry.data_file.record_count = rows;
        entry.data_file.first_row_id = first_row_id;
        entry
    };
    let mut existing = file(25, Some(800));
    existing.status = STATUS_EXISTING;
    existing.snapshot_id = Some(1);
    existing.sequence_number = Some(1);
    existing.file_sequence_number = Some(1);
    let entries = [existing, file(50, None), file(50, None)];
    let bytes = write_manifest(&entries, &schema, &spec, 3).unwrap();

    let header = ManifestListHeader {
        snapshot_id: 2,
        parent_snapshot_id: None,
        sequence_number: 2,
        first_row_id: Some(1000),
    };
    let mut manifest = toy_iceberg_core::manifest::ManifestFile {
        path: "m.avro".into(),
        length: bytes.len() as i64,
        partition_spec_id: 0,
        content: 0,
        sequence_number: 2,
        min_sequence_number: 1,
        added_snapshot_id: 2,
        added_files_count: 2,
        existing_files_count: 1,
        deleted_files_count: 0,
        added_rows_count: 100,
        existing_rows_count: 25,
        deleted_rows_count: 0,
        partitions: vec![],
        first_row_id: None,
    };
    manifest.first_row_id = Some(1000);
    let list = write_manifest_list(std::slice::from_ref(&manifest), &header, 3).unwrap();
    let manifest = read_manifest_list(&list).unwrap().remove(0);

    let ids: Vec<_> = read_manifest(&bytes, &[], &manifest)
        .unwrap()
        .iter()
        .map(|e| e.data_file.first_row_id.unwrap())
        .collect();
    assert_eq!(
        ids,
        vec![800, 1000, 1050],
        "the example in the spec's Row Lineage section"
    );
}

#[tokio::test]
async fn namespaces_with_non_ascii_names_list_decoded() {
    let catalog = catalog();
    let ctx = session(catalog.clone()).await.unwrap();
    let ddl = r#"CREATE EXTERNAL TABLE IF NOT EXISTS "売上"."注文" (a INT)
                 STORED AS ICEBERG LOCATION ''"#;
    run(&ctx, ddl).await.unwrap();
    assert_eq!(catalog.list_namespaces().await.unwrap(), vec!["売上"]);
    assert_eq!(catalog.list_tables("売上").await.unwrap(), vec!["注文"]);

    let restarted = session(catalog).await.unwrap();
    run(&restarted, ddl).await.unwrap();
}

#[tokio::test]
async fn metadata_tables_are_read_only() {
    let ctx = session(catalog()).await.unwrap();
    table_with_row(&ctx).await;
    let sql = r#"INSERT INTO ns."t$files" SELECT * FROM ns."t$files""#;
    assert!(run(&ctx, sql).await.is_err());
}

#[tokio::test]
async fn snapshot_counts_are_numbers() {
    let ctx = session(catalog()).await.unwrap();
    table_with_row(&ctx).await;
    for _ in 0..14 {
        run(&ctx, "INSERT INTO ns.t VALUES (1)").await.unwrap();
    }
    assert_eq!(
        count(&ctx, r#"SELECT max(total_records) FROM ns."t$snapshots""#).await,
        15
    );
}

#[tokio::test]
async fn the_files_table_skips_deleted_entries() {
    let catalog = catalog();
    let ctx = session(catalog.clone()).await.unwrap();
    table_with_row(&ctx).await;
    let table = catalog
        .load_table(&TableIdent::new("ns", "t"))
        .await
        .unwrap();
    let snapshot = table.metadata().current_snapshot().unwrap().clone();
    let mut manifest = table.manifests(&snapshot).await.unwrap().remove(0);
    let mut entry = table.entries(&manifest).await.unwrap().remove(0);
    entry.status = STATUS_DELETED;

    let bytes =
        write_manifest(&[entry], table.schema(), &PartitionSpec::unpartitioned(), 2).unwrap();
    manifest.path = format!("{}/metadata/deleted-m0.avro", table.metadata().location);
    manifest.length = bytes.len() as i64;
    catalog.write(&manifest.path, bytes).await.unwrap();
    let header = ManifestListHeader {
        snapshot_id: snapshot.snapshot_id,
        parent_snapshot_id: None,
        sequence_number: snapshot.sequence_number,
        first_row_id: None,
    };
    let list = write_manifest_list(&[manifest], &header, 2).unwrap();
    catalog.write(&snapshot.manifest_list, list).await.unwrap();

    assert_eq!(count(&ctx, r#"SELECT count(*) FROM ns."t$files""#).await, 0);
}

#[tokio::test]
async fn create_schema_succeeds_because_namespaces_are_implicit() {
    let ctx = session(catalog()).await.unwrap();
    run(&ctx, "CREATE SCHEMA sales").await.unwrap();
    run(
        &ctx,
        "CREATE EXTERNAL TABLE sales.t (a INT) STORED AS ICEBERG LOCATION ''",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_spec_that_does_not_fit_the_schema_is_rejected_at_creation() {
    let spec = PartitionSpec {
        spec_id: 0,
        fields: vec![PartitionField {
            source_id: 99,
            field_id: 1000,
            name: "x".into(),
            transform: Transform::Identity,
        }],
    };
    let ident = TableIdent::new("ns", "t");
    let created = catalog()
        .create_table(&ident, int_schema("a"), spec, 2, BTreeMap::new(), None)
        .await;
    assert!(matches!(created, Err(Error::Invalid(_))));
}

#[tokio::test]
async fn prepared_statements_see_later_commits() {
    let ctx = session(catalog()).await.unwrap();
    table_with_row(&ctx).await;
    run(&ctx, "PREPARE q AS SELECT count(*) FROM ns.t")
        .await
        .unwrap();
    run(&ctx, "INSERT INTO ns.t VALUES (2)").await.unwrap();
    assert_eq!(count(&ctx, "EXECUTE q").await, 2);
}
