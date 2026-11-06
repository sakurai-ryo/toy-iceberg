//! End-to-end tests against RustFS. Requires `mise run up`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field as ArrowField, Int64Type, Schema as ArrowSchema};
use arrow::util::pretty::pretty_format_batches;
use futures::TryStreamExt;
use iceberg::io::{
    FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
    S3_SECRET_ACCESS_KEY,
};
use iceberg::table::StaticTable;
use iceberg_arrow_array::{Array as _, Int64Array as IcebergInt64Array};
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::arrow::ArrowWriter;
use toy_iceberg_core::arrow_schema::arrow_schema;
use toy_iceberg_core::catalog::{Catalog, S3Config, TableIdent};
use toy_iceberg_core::partition::PartitionSpec;
use toy_iceberg_core::scan::read_task;
use toy_iceberg_core::table::Table;
use toy_iceberg_core::types::{Field, Schema, Type};
use toy_iceberg_datafusion::{IcebergSession, session};

fn catalog() -> Arc<Catalog> {
    Catalog::s3(&S3Config::from_env()).unwrap()
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

async fn sql(ctx: &IcebergSession, query: &str) -> Vec<RecordBatch> {
    let df = ctx
        .sql(query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    df.collect()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
}

async fn show(ctx: &IcebergSession, query: &str) -> String {
    pretty_format_batches(&sql(ctx, query).await)
        .unwrap()
        .to_string()
}

async fn long(ctx: &IcebergSession, query: &str) -> i64 {
    let batches = sql(ctx, query).await;
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

fn lines(text: &str) -> Vec<&str> {
    text.lines().map(str::trim).collect()
}

async fn plan(ctx: &IcebergSession, query: &str) -> String {
    let explain = show(ctx, &format!("EXPLAIN {query}")).await;
    explain
        .lines()
        .find(|l| l.contains("IcebergScanExec"))
        .unwrap()
        .to_owned()
}

async fn create_events(ctx: &IcebergSession, ns: &str, options: &str) {
    let ddl = format!(
        "CREATE EXTERNAL TABLE {ns}.events (
            id BIGINT NOT NULL, ts TIMESTAMP, name VARCHAR, score DOUBLE
         ) STORED AS ICEBERG LOCATION '' OPTIONS ({options})"
    );
    sql(ctx, &ddl).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_round_trip_with_every_supported_type_and_transform() {
    let ns = unique("types");
    let ctx = session(catalog()).await.unwrap();
    sql(
        &ctx,
        &format!(
            "CREATE EXTERNAL TABLE {ns}.t (
                id BIGINT NOT NULL, i INT, b BOOLEAN, f FLOAT, d DOUBLE, dt DATE,
                ts TIMESTAMP, s VARCHAR, x BYTEA
             ) STORED AS ICEBERG LOCATION '' OPTIONS (
                'partitioning' 'bucket(4, id), truncate(10, i), truncate(2, s), month(dt), hour(ts), b, year(ts), day(dt)'
             )"
        ),
    )
    .await;
    sql(
        &ctx,
        &format!(
            "INSERT INTO {ns}.t VALUES
              (1, 15, true, CAST(1.5 AS FLOAT), 2.5, DATE '2024-02-29', TIMESTAMP '2024-02-29 23:59:59.123456', 'イルカ', X'00ff'),
              (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
              (3, -5, false, CAST('NaN' AS FLOAT), -0.0, DATE '1969-12-31', TIMESTAMP '1969-12-31 23:00:00', '', X'')"
        ),
    )
    .await;
    let got = show(
        &ctx,
        &format!("SELECT id, i, b, f, dt, ts, s, x FROM {ns}.t ORDER BY id"),
    )
    .await;
    assert_eq!(
        lines(&got),
        vec![
            "+----+----+-------+-----+------------+----------------------------+--------+------+",
            "| id | i  | b     | f   | dt         | ts                         | s      | x    |",
            "+----+----+-------+-----+------------+----------------------------+--------+------+",
            "| 1  | 15 | true  | 1.5 | 2024-02-29 | 2024-02-29T23:59:59.123456 | イルカ | 00ff |",
            "| 2  |    |       |     |            |                            |        |      |",
            "| 3  | -5 | false | NaN | 1969-12-31 | 1969-12-31T23:00:00        |        |      |",
            "+----+----+-------+-----+------------+----------------------------+--------+------+",
        ]
    );
    assert_eq!(
        long(&ctx, &format!("SELECT count(*) FROM {ns}.t WHERE f > 100")).await,
        1,
        "NaN"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_pruning_stage_removes_work_without_losing_rows() {
    let ns = unique("prune");
    let ctx = session(catalog()).await.unwrap();
    create_events(&ctx, &ns, "'partitioning' 'day(ts), bucket(4, id)'").await;
    sql(
        &ctx,
        &format!(
            "INSERT INTO {ns}.events VALUES
              (1, TIMESTAMP '2024-01-01 10:00:00', 'a', 1.0),
              (2, TIMESTAMP '2024-01-02 11:00:00', 'b', NULL),
              (3, NULL, NULL, 3.0)"
        ),
    )
    .await;
    sql(
        &ctx,
        &format!("INSERT INTO {ns}.events VALUES (4, TIMESTAMP '2024-03-05 10:00:00', 'c', 9.0)"),
    )
    .await;

    let q = format!("SELECT * FROM {ns}.events WHERE ts >= TIMESTAMP '2024-03-01 00:00:00'");
    assert!(
        plan(&ctx, &q).await.contains("manifests=1/2, files=1/1"),
        "manifest list"
    );
    assert_eq!(
        sql(&ctx, &q)
            .await
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        1
    );

    let q = format!("SELECT * FROM {ns}.events WHERE ts = TIMESTAMP '2024-01-02 11:00:00'");
    assert!(
        plan(&ctx, &q).await.contains("pruned_by_partition=2"),
        "manifest entries"
    );
    assert_eq!(
        sql(&ctx, &q)
            .await
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        1
    );

    let q = format!("SELECT * FROM {ns}.events WHERE name = 'zzz'");
    assert!(
        plan(&ctx, &q)
            .await
            .contains("files=0/4, pruned_by_partition=0, pruned_by_metrics=4")
    );
    assert_eq!(
        sql(&ctx, &q)
            .await
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        0
    );

    let q = format!("SELECT id FROM {ns}.events WHERE ts IS NULL");
    assert_eq!(long(&ctx, &q).await, 3);

    let q =
        format!("SELECT id FROM {ns}.events WHERE NOT (name = 'a' OR length(name) > 5) AND id = 2");
    assert_eq!(long(&ctx, &q).await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn time_travel_and_metadata_tables() {
    let ns = unique("history");
    let ctx = session(catalog()).await.unwrap();
    create_events(&ctx, &ns, "'partitioning' 'day(ts)'").await;
    let insert = |id: i64| {
        format!(
            "INSERT INTO {ns}.events VALUES ({id}, TIMESTAMP '2024-01-0{id} 00:00:00', 'x', 1.0)"
        )
    };
    sql(&ctx, &insert(1)).await;
    sql(&ctx, &insert(2)).await;

    let first = long(
        &ctx,
        &format!(
            r#"SELECT snapshot_id FROM {ns}."events$snapshots" ORDER BY sequence_number LIMIT 1"#
        ),
    )
    .await;
    assert_eq!(
        long(&ctx, &format!("SELECT count(*) FROM {ns}.events")).await,
        2
    );
    assert_eq!(
        long(
            &ctx,
            &format!(r#"SELECT count(*) FROM {ns}."events@{first}""#)
        )
        .await,
        1
    );

    let snapshots = sql(
        &ctx,
        &format!(r#"SELECT count(*) FROM {ns}."events$snapshots""#),
    )
    .await;
    assert_eq!(
        snapshots[0].column(0).as_primitive::<Int64Type>().value(0),
        2
    );
    assert_eq!(
        long(
            &ctx,
            &format!(r#"SELECT count(*) FROM {ns}."events$manifests""#)
        )
        .await,
        2
    );
    assert_eq!(
        long(
            &ctx,
            &format!(r#"SELECT count(*) FROM {ns}."events$files""#)
        )
        .await,
        2
    );
    let files = show(
        &ctx,
        &format!(r#"SELECT partition FROM {ns}."events$files" ORDER BY 1"#),
    )
    .await;
    assert!(
        files.contains("[2024-01-01]") && files.contains("[2024-01-02]"),
        "{files}"
    );

    let insert_old = format!(r#"INSERT INTO {ns}."events@{first}" VALUES (9, NULL, NULL, NULL)"#);
    let refused = match ctx.sql(&insert_old).await {
        Ok(df) => df.collect().await.is_err(),
        Err(_) => true,
    };
    assert!(refused, "a historical snapshot must not accept writes");
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_removes_it_from_the_catalog_only() {
    let ns = unique("drop");
    let catalog = catalog();
    let ctx = session(catalog.clone()).await.unwrap();
    create_events(&ctx, &ns, "'format-version' '2'").await;
    let ident = TableIdent::new(&ns, "events");
    let metadata_location = catalog
        .load_table(&ident)
        .await
        .unwrap()
        .metadata_location()
        .to_owned();

    sql(&ctx, &format!("DROP TABLE {ns}.events")).await;
    assert!(matches!(
        catalog.load_table(&ident).await,
        Err(toy_iceberg_core::Error::TableNotFound(_))
    ));
    assert!(catalog.read(&metadata_location).await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn creating_a_table_twice_fails() {
    let ns = unique("twice");
    let catalog = catalog();
    let schema = long_schema();
    let ident = TableIdent::new(&ns, "t");
    let create = || {
        catalog.create_table(
            &ident,
            schema.clone(),
            PartitionSpec::unpartitioned(),
            2,
            BTreeMap::new(),
            None,
        )
    };
    create().await.unwrap();
    assert!(matches!(
        create().await,
        Err(toy_iceberg_core::Error::TableExists(_))
    ));
}

fn long_schema() -> Schema {
    let field = Field {
        id: 1,
        name: "v".into(),
        ty: Type::Long,
        required: true,
        doc: None,
        extra: Default::default(),
    };
    Schema::new(vec![field]).unwrap()
}

fn long_batch(values: Vec<i64>) -> RecordBatch {
    let schema = ArrowSchema::new(vec![ArrowField::new("v", DataType::Int64, false)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(values))]).unwrap()
}

async fn long_table(catalog: &Arc<Catalog>, version: i32) -> Table {
    let ident = TableIdent::new(unique("ns"), "t");
    catalog
        .create_table(
            &ident,
            long_schema(),
            PartitionSpec::unpartitioned(),
            version,
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_appends_all_land_in_one_linear_history() {
    const WRITERS: i64 = 8;
    let catalog = catalog();
    let base = long_table(&catalog, 3).await;
    let writers: Vec<_> = (0..WRITERS)
        .map(|i| {
            let table = base.clone();
            tokio::spawn(async move { table.append(&[long_batch(vec![i, i + 100])]).await })
        })
        .collect();
    for w in writers {
        w.await.unwrap().unwrap().unwrap();
    }

    let table = catalog.load_table(base.ident()).await.unwrap();
    let metadata = table.metadata();
    assert_eq!(metadata.snapshots.len(), WRITERS as usize);
    assert_eq!(metadata.last_sequence_number, WRITERS);
    assert_eq!(metadata.next_row_id, Some(WRITERS * 2));

    let mut snapshots = metadata.snapshots.clone();
    snapshots.sort_by_key(|s| s.sequence_number);
    for (i, s) in snapshots.iter().enumerate() {
        assert_eq!(s.sequence_number, i as i64 + 1);
        assert_eq!(s.first_row_id, Some(i as i64 * 2));
        let parent = (i > 0).then(|| snapshots[i - 1].snapshot_id);
        assert_eq!(s.parent_snapshot_id, parent);
    }

    let plan = table.scan().plan().await.unwrap();
    assert_eq!(plan.tasks.len(), WRITERS as usize);
    let mut ids: Vec<_> = plan
        .tasks
        .iter()
        .map(|t| t.data_file.first_row_id.unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        (0..WRITERS).map(|i| i * 2).collect::<Vec<_>>(),
        "disjoint row id ranges"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_table_handle_rebases_on_the_latest_commit() {
    let catalog = catalog();
    let stale = long_table(&catalog, 2).await;
    let fresh = catalog.load_table(stale.ident()).await.unwrap();
    fresh.append(&[long_batch(vec![1])]).await.unwrap();
    stale.append(&[long_batch(vec![2])]).await.unwrap();

    let table = catalog.load_table(stale.ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots.len(), 2);
    assert_eq!(table.metadata().last_sequence_number, 2);
    assert_eq!(table.scan().plan().await.unwrap().tasks.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn upgrading_to_v3_assigns_row_ids_to_existing_files_on_the_next_commit() {
    let catalog = catalog();
    let table = long_table(&catalog, 2).await;
    table.append(&[long_batch(vec![1, 2, 3])]).await.unwrap();
    table.upgrade_to_v3().await.unwrap();

    let upgraded = catalog.load_table(table.ident()).await.unwrap();
    assert_eq!(upgraded.metadata().format_version, 3);
    assert_eq!(upgraded.metadata().next_row_id, Some(0));
    assert_eq!(
        upgraded.metadata().current_snapshot().unwrap().first_row_id,
        None
    );

    let snapshot = upgraded
        .append(&[long_batch(vec![4])])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.first_row_id, Some(0));
    assert_eq!(snapshot.added_rows, Some(4));

    let latest = catalog.load_table(table.ident()).await.unwrap();
    assert_eq!(latest.metadata().next_row_id, Some(4));
    let plan = latest.scan().plan().await.unwrap();
    let mut assigned: Vec<_> = plan
        .tasks
        .iter()
        .map(|t| (t.data_file.first_row_id.unwrap(), t.data_file.record_count))
        .collect();
    assigned.sort_unstable();
    assert_eq!(
        assigned,
        vec![(0, 1), (1, 3)],
        "new manifest comes first in the list"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn columns_resolve_by_field_id_not_by_name() {
    let catalog = catalog();
    let ident = TableIdent::new(unique("ids"), "t");
    let field = |id, name: &str, ty, required| Field {
        id,
        name: name.into(),
        ty,
        required,
        doc: None,
        extra: Default::default(),
    };
    let schema = Schema::new(vec![
        field(1, "a", Type::Long, true),
        field(2, "region", Type::String, true),
    ])
    .unwrap();
    let spec = PartitionSpec::parse("region", &schema).unwrap();
    let table = catalog
        .create_table(&ident, schema, spec, 2, BTreeMap::new(), None)
        .await
        .unwrap();
    let arrow = ArrowSchema::new(vec![
        ArrowField::new("a", DataType::Int64, false),
        ArrowField::new("region", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(StringArray::from(vec!["jp"])),
        ],
    )
    .unwrap();
    table.append(&[batch]).await.unwrap();
    let table = catalog.load_table(&ident).await.unwrap();
    let task = table.scan().plan().await.unwrap().tasks.remove(0);

    // The spec's example: rename a column, reorder, and project a column the file lacks.
    let projection = [
        field(9, "added_later", Type::Int, false),
        field(1, "renamed", Type::Long, true),
    ];
    let batches: Vec<_> = read_task(&table, &task, &projection)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches[0].schema().field(1).name(), "renamed");
    assert_eq!(batches[0].column(0).null_count(), 1);
    assert_eq!(batches[0].column(1).as_primitive::<Int64Type>().value(0), 7);

    // Spec: a field the file lacks takes its identity partition value. Such files come from
    // metadata-only Hive migrations, which keep partition columns out of the data files.
    let hive_schema = arrow_schema(&[field(1, "a", Type::Long, true)]);
    let hive_batch = RecordBatch::try_new(
        hive_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![8]))],
    )
    .unwrap();
    let mut parquet = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut parquet, hive_schema, None).unwrap();
    writer.write(&hive_batch).unwrap();
    writer.close().unwrap();
    let mut hive_task = task.clone();
    hive_task.data_file.file_path = format!("{}/data/hive.parquet", table.metadata().location);
    hive_task.data_file.file_size_in_bytes = parquet.len() as i64;
    catalog
        .write(&hive_task.data_file.file_path, parquet)
        .await
        .unwrap();

    let region = [field(2, "region", Type::String, true)];
    let batches: Vec<_> = read_task(&table, &hive_task, &region)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "jp");
}

fn file_io() -> iceberg::io::FileIO {
    let config = S3Config::from_env();
    FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    }))
    .with_props(HashMap::from([
        (S3_ENDPOINT.to_owned(), config.endpoint),
        (S3_PATH_STYLE_ACCESS.to_owned(), "true".to_owned()),
        (S3_REGION.to_owned(), config.region),
        (S3_ACCESS_KEY_ID.to_owned(), config.access_key),
        (S3_SECRET_ACCESS_KEY.to_owned(), config.secret_key),
    ]))
    .build()
}

/// Reads with the independent iceberg-rust implementation and returns sorted `id` values.
async fn read_ids_with_iceberg_rust(
    metadata_location: &str,
    filter: Option<iceberg::expr::Predicate>,
) -> Vec<i64> {
    let ident = iceberg::TableIdent::from_strs(["ns", "t"]).unwrap();
    let table = StaticTable::from_metadata_file(metadata_location, ident, file_io())
        .await
        .unwrap();
    let mut scan = table.scan().select(["id"]);
    if let Some(filter) = filter {
        scan = scan.with_filter(filter);
    }
    let stream = scan.build().unwrap().to_arrow().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let column = b
                .column(0)
                .as_any()
                .downcast_ref::<IcebergInt64Array>()
                .unwrap();
            column.values().to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

async fn cross_read(format_version: u8) {
    let ns = unique("cross");
    let catalog = catalog();
    let ctx = session(catalog.clone()).await.unwrap();
    let options = format!(
        "'format-version' '{format_version}', \
         'partitioning' 'day(ts), bucket(4, id), truncate(1, name)'"
    );
    create_events(&ctx, &ns, &options).await;
    sql(
        &ctx,
        &format!(
            "INSERT INTO {ns}.events VALUES
              (1, TIMESTAMP '2024-01-01 10:00:00', 'alice', 1.5),
              (2, TIMESTAMP '2024-01-02 11:00:00', 'bob', NULL),
              (3, NULL, NULL, 3.5)"
        ),
    )
    .await;
    sql(
        &ctx,
        &format!(
            "INSERT INTO {ns}.events VALUES (4, TIMESTAMP '2024-03-05 10:00:00', 'carol', 9.0)"
        ),
    )
    .await;

    let table = catalog
        .load_table(&TableIdent::new(&ns, "events"))
        .await
        .unwrap();
    let location = table.metadata_location();
    assert_eq!(
        read_ids_with_iceberg_rust(location, None).await,
        vec![1, 2, 3, 4]
    );

    let filter =
        iceberg::expr::Reference::new("name").equal_to(iceberg::spec::Datum::string("bob"));
    assert_eq!(
        read_ids_with_iceberg_rust(location, Some(filter)).await,
        vec![2]
    );
    let filter = iceberg::expr::Reference::new("id").greater_than(iceberg::spec::Datum::long(2));
    assert_eq!(
        read_ids_with_iceberg_rust(location, Some(filter)).await,
        vec![3, 4]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_rust_reads_a_v2_table_written_here() {
    cross_read(2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_rust_reads_a_v3_table_written_here() {
    cross_read(3).await;
}
