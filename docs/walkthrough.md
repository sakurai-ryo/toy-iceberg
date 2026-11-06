# コードで追う toy-iceberg

この文書は、1つのテーブルに SQL を実行したときの処理を、CLI の `main` からオブジェクトストレージへの PUT まで、呼び出しの順に追います。Iceberg の知識は前提にしません。用語は処理の中で初めて現れたところで説明します。

コードの引用は、コードブロックの直前に示したファイルからの抜粋です。`// ...` は省略を表します。場所は行番号ではなくシンボル名（`Table::append` など）で示します。「仕様の『〜』」は [Iceberg Table Spec](https://iceberg.apache.org/spec/) の節名です。

## 目次

1. [題材の SQL とファイルの全体像](#1-題材の-sql-とファイルの全体像)
2. [起動と SQL の振り分け](#2-起動と-sql-の振り分け)
3. [CREATE TABLE](#3-create-table)
4. [INSERT: data file を書く](#4-insert-data-file-を書く)
5. [INSERT: manifest を書く](#5-insert-manifest-を書く)
6. [INSERT: コミットする](#6-insert-コミットする)
7. [コミットの競合と再試行](#7-コミットの競合と再試行)
8. [SELECT: フィルタを述語に変換する](#8-select-フィルタを述語に変換する)
9. [SELECT: 読むファイルを3段階で絞る](#9-select-読むファイルを3段階で絞る)
10. [SELECT: Parquet を読む](#10-select-parquet-を読む)
11. [タイムトラベルとメタデータテーブル](#11-タイムトラベルとメタデータテーブル)
12. [v3 の row lineage](#12-v3-の-row-lineage)
13. [DROP TABLE](#13-drop-table)
14. [実装していないものとテスト](#14-実装していないものとテスト)

## 1. 題材の SQL とファイルの全体像

この文書では、次の SQL を順に実行したときの処理を追います。`mise run up` で RustFS を起動し、`mise run sql "<SQL>"` で1文ずつ実行すれば同じ状態を再現できます。出力例はこの手順で得た実際の値です。UUID、snapshot id、時刻は実行ごとに変わります。

```sql
CREATE EXTERNAL TABLE tour.events (id BIGINT NOT NULL, ts TIMESTAMP, name VARCHAR)
  STORED AS ICEBERG LOCATION '' OPTIONS ('partitioning' 'day(ts)');

INSERT INTO tour.events VALUES
  (1, TIMESTAMP '2024-01-01 10:00:00', 'a'), (2, TIMESTAMP '2024-01-01 11:00:00', 'b');
INSERT INTO tour.events VALUES
  (3, TIMESTAMP '2024-01-02 10:00:00', 'c'), (4, TIMESTAMP '2024-01-03 10:00:00', 'd');
INSERT INTO tour.events VALUES (5, TIMESTAMP '2024-01-03 12:00:00', 'e');

SELECT * FROM tour.events WHERE ts >= TIMESTAMP '2024-01-03 11:00:00';
```

すべてを実行すると、バケットには次のオブジェクトができます。

```text
_catalog/tour/events.json
tour/events/data/ts_day=2024-01-01/<uuid>.parquet
tour/events/data/ts_day=2024-01-02/<uuid>.parquet
tour/events/data/ts_day=2024-01-03/<uuid>.parquet
tour/events/data/ts_day=2024-01-03/<uuid>.parquet
tour/events/metadata/00000-<uuid>.metadata.json
tour/events/metadata/00001-<uuid>.metadata.json
tour/events/metadata/00002-<uuid>.metadata.json
tour/events/metadata/00003-<uuid>.metadata.json
tour/events/metadata/<uuid>-m0.avro
tour/events/metadata/<uuid>-m0.avro
tour/events/metadata/<uuid>-m0.avro
tour/events/metadata/snap-<snapshot-id>-1-<uuid>.avro
tour/events/metadata/snap-<snapshot-id>-2-<uuid>.avro
tour/events/metadata/snap-<snapshot-id>-3-<uuid>.avro
```

これらのファイルでは、上の階層が下の階層のパスを持ちます。次の図は、現在の snapshot（ある時点のテーブルの内容）から1本の経路をたどったものです。実際には metadata ファイルがすべての snapshot の manifest list を指し、1つの manifest を複数の manifest list が参照するので、全体は厳密な木ではありません。

```text
ポインタ                  _catalog/tour/events.json
  └ metadata ファイル     00003-<uuid>.metadata.json   スキーマ、パーティション定義、全 snapshot
      └ manifest list     snap-<id>-3-<uuid>.avro      1 snapshot に属する manifest の一覧
          └ manifest      <uuid>-m0.avro               data file の一覧と列統計
              └ data file <uuid>.parquet               行データ
```

| 階層 | 形式 | Rust の型 | 読むコード | 書くコード |
|---|---|---|---|---|
| ポインタ | JSON | `Pointer` | `Catalog::load_table` | `Catalog::create_table`、`Catalog::commit` |
| metadata ファイル | JSON | `TableMetadata`、`Snapshot` | `TableMetadata::parse` | `Catalog::write_metadata` |
| manifest list | Avro | `ManifestFile`、`FieldSummary` | `read_manifest_list` | `write_manifest_list` |
| manifest | Avro | `ManifestEntry`、`DataFile` | `read_manifest` | `write_manifest` |
| data file | Parquet | `RecordBatch` | `read_task` | `write_file` |

ポインタ以外のファイルは、一度書いたら変更しません。変更するのはポインタだけなので、ポインタの差し替えがテーブル全体の原子的な更新になります。この文書で追う処理の大半は、不変のファイルを下の階層から順に書き、最後にポインタを差し替えるという手順の実装です。

実装は3つのクレートに分かれています。ほかに、テスト専用の `crates/integration_tests` があります。

| クレート | 役割 |
|---|---|
| `crates/cli` | SQL を1文実行するコマンド |
| `crates/datafusion` | core の API を DataFusion の trait に対応づける層 |
| `crates/core` | テーブルフォーマットの実装。Iceberg の仕様に関わる処理はここにある |

## 2. 起動と SQL の振り分け

どの SQL も、同じ経路で core に届きます。

```text
main                                 crates/cli/src/main.rs
└ run
  ├ Catalog::s3                      crates/core/src/catalog.rs
  ├ session                          crates/datafusion/src/lib.rs
  │ ├ IcebergTableFactory::new       crates/datafusion/src/table.rs
  │ └ IcebergCatalogProvider::load   crates/datafusion/src/catalog.rs
  └ IcebergSession::sql              crates/datafusion/src/lib.rs
    └ DataFrame::show
```

`main` は引数を連結して SQL にし、`run` を呼びます。

`crates/cli/src/main.rs`

```rust
async fn run(sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Catalog::s3(&S3Config::from_env())?;
    let ctx = session(catalog).await?;
    ctx.sql(sql).await?.show().await?;
    Ok(())
}
```

このプロセスはテーブルの状態を持ちません。実行のたびにオブジェクトストレージのポインタから読み直すので、別のプロセスのコミットも次の実行で見えます。

`Catalog` は `ObjectStore` とバケット名を持つだけの構造体です。`Catalog::s3` が `object_store` の S3 クライアントを作ります。core の入出力は `ObjectStore` trait を通るので、テストでは `Catalog::new` にメモリ上の実装を渡して、S3 なしで実行できます。

`session` は DataFusion のセッションを組み立てます。

`crates/datafusion/src/lib.rs`

```rust
pub async fn session(catalog: Arc<Catalog>) -> Result<IcebergSession> {
    let mut config = SessionConfig::new()
        .with_default_catalog_and_schema(CATALOG_NAME, DEFAULT_NAMESPACE)
        .with_create_default_catalog_and_schema(false);
    // セッションにタイムゾーンがないと、SQL の `TIMESTAMPTZ` はタイムゾーンなしの timestamp になる。
    // Iceberg の `timestamptz` は UTC で保存するので、セッションも UTC にする。
    config.options_mut().execution.time_zone = Some("UTC".to_owned());
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_table_factory(
            "ICEBERG".into(),
            Arc::new(IcebergTableFactory::new(catalog.clone())),
        )
        .build();
    let ctx = SessionContext::new_with_state(state);
    let provider = Arc::new(IcebergCatalogProvider::load(catalog.clone()).await?);
    ctx.register_catalog(CATALOG_NAME, provider.clone());
    Ok(IcebergSession {
        ctx,
        catalog,
        provider,
    })
}
```

ここで DataFusion に2つのものを登録しています。

- `with_table_factory` の `"ICEBERG"` により、`STORED AS ICEBERG` の `CREATE EXTERNAL TABLE` が `IcebergTableFactory` に渡ります。
- `register_catalog` により、`tour.events` のような名前の解決が `IcebergCatalogProvider` に渡ります。Iceberg の namespace が DataFusion のスキーマに対応します。

`IcebergCatalogProvider::load` は `_catalog/` の下を一覧して、namespace とテーブル名を読みます。

`crates/datafusion/src/catalog.rs`

```rust
    pub async fn load(catalog: Arc<Catalog>) -> Result<Self> {
        let mut schemas = BTreeMap::new();
        for namespace in catalog.list_namespaces().await.map_err(to_df)? {
            let tables = catalog.list_tables(&namespace).await.map_err(to_df)?;
            let provider = IcebergSchemaProvider::new(catalog.clone(), &namespace, tables);
            schemas.insert(namespace, Arc::new(provider));
        }
        Ok(IcebergCatalogProvider {
            catalog,
            schemas: Mutex::new(schemas),
        })
    }
```

先に読んでおくのは、DataFusion の `schema_names` と `table_exist` が同期関数で、その中でストレージを読めないからです。一方、テーブルの実体を返す `SchemaProvider::table` は非同期関数なので、呼ばれるたびに最新の metadata を読みます（[4章](#4-insert-data-file-を書く)）。

最後に `IcebergSession::sql` が SQL を論理プランにして振り分けます。

`crates/datafusion/src/lib.rs`

```rust
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let plan = self.ctx.state().create_logical_plan(sql).await?;
        match &plan {
            LogicalPlan::Ddl(DdlStatement::DropTable(cmd)) => self.drop_table(cmd).await,
            // namespace は暗黙に存在する。そのまま渡すと DataFusion が重複として報告する。
            LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(_)) => self.ctx.read_empty(),
            _ => self.ctx.execute_logical_plan(plan).await,
        }
    }
```

`DROP TABLE` を自前で処理する理由は[13章](#13-drop-table)で説明します。`CREATE SCHEMA` は、namespace が暗黙に存在するので何もしません。残りは `execute_logical_plan` に渡りますが、実行のタイミングは文の種類で異なります。

- `CREATE EXTERNAL TABLE` は DDL なので、`execute_logical_plan` の中で実行されます。
- `INSERT` と `SELECT` は `DataFrame` を返すだけで、`run` の `.show()` で物理プランが作られて実行されます。

## 3. CREATE TABLE

`CREATE EXTERNAL TABLE` の処理は、metadata ファイルを1つ書き、それを指すポインタを作るだけです。data file も manifest もまだありません。

```text
SessionContext::create_external_table     DataFusion
├ IcebergTableFactory::create             crates/datafusion/src/table.rs
│ ├ schema_from_arrow                     crates/core/src/arrow_schema.rs
│ ├ PartitionSpec::parse                  crates/core/src/partition.rs
│ └ Catalog::create_table                 crates/core/src/catalog.rs
│   ├ TableMetadata::new                  crates/core/src/metadata.rs
│   ├ Catalog::write_metadata             → 00000-<uuid>.metadata.json
│   ├ put_opts(PutMode::Create)           → _catalog/tour/events.json
│   └ Catalog::load_table
└ SchemaProvider::register_table          crates/datafusion/src/catalog.rs
```

### SQL の定義を Iceberg の定義に変換する

`IcebergTableFactory::create` は、DataFusion の `CreateExternalTable` から core の引数を組み立てます。

`crates/datafusion/src/table.rs`

```rust
        let schema = schema_from_arrow(cmd.schema.as_arrow()).map_err(to_df)?;
        // DataFusion は、ドットを含まないオプションのキーに `format.` を付ける。
        let mut properties: BTreeMap<_, _> = cmd
            .options
            .iter()
            .map(|(k, v)| (k.strip_prefix("format.").unwrap_or(k).to_owned(), v.clone()))
            .collect();
        let spec = match properties.remove("partitioning") {
            Some(expr) => PartitionSpec::parse(&expr, &schema).map_err(to_df)?,
            None => PartitionSpec::unpartitioned(),
        };
```

`schema_from_arrow` は Arrow の型を Iceberg の型に変換し、列の順に field id を割り当てます。

`crates/core/src/arrow_schema.rs`

```rust
            Ok(Field {
                id: i as i32 + 1,
                name: f.name().clone(),
                required: !f.is_nullable(),
                ty,
                doc: None,
                extra: Default::default(),
            })
```

field id は列の同一性を表す番号です。Iceberg は列を名前ではなく field id で識別します。統計もパーティション定義もこの番号で列を参照するので、列名を変えても既存のファイルを書き換えずに済みます。`tour.events` の列は次のようになります。

| field id | 名前 | 型 | required |
|---|---|---|---|
| 1 | `id` | `long` | true |
| 2 | `ts` | `timestamp` | false |
| 3 | `name` | `string` | false |

`PartitionSpec::parse` は `day(ts)` のような文字列を解析します。この構文はこの実装独自のものです。パーティション定義は「どの列にどの transform を適用するか」のリストで、transform は列の値からパーティション値を計算する関数です。

`crates/core/src/partition.rs`

```rust
        for term in split_top_level(expr) {
            let (transform, column) = parse_term(term)?;
            let Some(source) = schema.field_by_name(column) else {
                return invalid(format!("partition column {column:?} does not exist"));
            };
            transform.result_type(source.ty)?;
            let name = match transform {
                Transform::Identity => source.name.clone(),
                Transform::Bucket(_) => format!("{}_bucket", source.name),
                Transform::Truncate(_) => format!("{}_trunc", source.name),
                Transform::Year => format!("{}_year", source.name),
                Transform::Month => format!("{}_month", source.name),
                Transform::Day => format!("{}_day", source.name),
                Transform::Hour => format!("{}_hour", source.name),
                Transform::Void => format!("{}_null", source.name),
            };
            // ...
            fields.push(PartitionField {
                source_id: source.id,
                field_id: FIRST_PARTITION_FIELD_ID + fields.len() as i32,
                name,
                transform,
            });
        }
```

`day(ts)` からは、次の `PartitionField` が1つできます。`source_id` は列を field id で参照するので、列名を変えてもパーティション定義は影響を受けません。

```json
{ "source-id": 2, "field-id": 1000, "name": "ts_day", "transform": "day" }
```

`Transform::result_type` は、transform をその型に適用できるかの検査と、結果の型の計算を兼ねています。`day` の結果は `date` 型です。この型はのちに manifest の読み書きで使います。

### metadata ファイルとポインタを書く

`Catalog::create_table` が書き込みの本体です。

`crates/core/src/catalog.rs`

```rust
        let location = location
            .unwrap_or_else(|| format!("s3://{}/{}/{}", self.bucket, ident.namespace, ident.name));
        self.path(&location)?;
        let metadata =
            TableMetadata::new(location, schema, spec, format_version, properties, now_ms())?;
        let metadata_location = self.write_metadata(&metadata).await?;
        let pointer = serde_json::to_vec(&Pointer { metadata_location })?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&ident.pointer_path(), pointer.into(), options)
            .await
        {
            Ok(_) => self.load_table(ident).await,
            Err(object_store::Error::AlreadyExists { .. }) => {
                Err(Error::TableExists(ident.to_string()))
            }
            Err(e) => Err(e.into()),
        }
```

SQL の `LOCATION ''` は `None` になるので、location は `s3://warehouse/tour/events` になります。`self.path` は `s3://...` をバケット内のキーに変換する関数で、ここではバケットの外を指す location を拒否するために呼んでいます。

順序は「metadata ファイルを書く、ポインタを作る」です。ポインタが見えた時点で指す先が必ず存在するように、この順に書きます。

`PutMode::Create` は `If-None-Match: *` 付きの PUT で、キーが既に存在すれば失敗します。同名のテーブルを同時に作ると、ポインタを作れるのは1つの書き手だけです。負けた書き手の metadata ファイルは、どこからも参照されないまま残ります。

`write_metadata` のファイル名は仕様の命名規則に従います。

`crates/core/src/catalog.rs`

```rust
    async fn write_metadata(&self, metadata: &TableMetadata) -> Result<String> {
        let location = format!(
            "{}/metadata/{:05}-{}.metadata.json",
            metadata.location,
            metadata.metadata_log.len(),
            uuid::Uuid::new_v4()
        );
        self.write(&location, serde_json::to_vec_pretty(metadata)?)
            .await?;
        Ok(location)
    }
```

バージョン番号には `metadata_log` の長さを使います。この時点では履歴が空なので `00000` です。

書かれた `00000-<uuid>.metadata.json` の内容は次のとおりです（`schemas` と `partition-specs` は上で示した内容なので省略します）。

```json
{
  "format-version": 2,
  "table-uuid": "e24e8bf3-a3b4-47fb-88a5-c34483ccc1c2",
  "location": "s3://warehouse/tour/events",
  "last-sequence-number": 0,
  "last-updated-ms": 1793940755347,
  "last-column-id": 3,
  "current-schema-id": 0,
  "default-spec-id": 0,
  "last-partition-id": 1000,
  "properties": {},
  "snapshots": [],
  "snapshot-log": [],
  "metadata-log": [],
  "sort-orders": [{ "fields": [], "order-id": 0 }],
  "default-sort-order-id": 0,
  "refs": {}
}
```

`current-snapshot-id` がないのは、snapshot がまだ1つもないからです。`table-uuid` は作成時に割り当てられ、以後変わりません。[7章](#7-コミットの競合と再試行)で、同名の別テーブルとの区別に使います。

`TableMetadata` には `#[serde(flatten)] pub extra` というフィールドがあります。この実装が知らないフィールドを JSON のまま保持して、次のコミットでも書き戻すためです。捨てると、他の実装が保存した情報がコミットのたびに消えてしまいます。`Field` と `Snapshot` の `extra` も同じ理由で持っています。他の型には `extra` がないので、未知のキーは保持しません。

ポインタの内容は1行です。この形式はこの実装の独自のもので、仕様にはありません。

```json
{"metadata-location":"s3://warehouse/tour/events/metadata/00000-3cd04b2d-a204-4192-af96-6cc6cae1571b.metadata.json"}
```

仕様の「Metastore Tables」は、コミットに必要な原子的な差し替えを、check-and-put で更新するポインタで実現できると述べています。check-and-put は「値が読んだときのままなら書き換える」操作です。通常は Hive Metastore などが受け持つこの処理を、ここでは S3 の条件付き PUT で代用しています。

### テーブルを読み直す

作成の最後に呼ぶ `load_table` は、ポインタから metadata までをたどる処理で、テーブルを読み書きする操作の出発点です。

`crates/core/src/catalog.rs`

```rust
    pub async fn load_table(self: &Arc<Self>, ident: &TableIdent) -> Result<Table> {
        let result = match self.store.get(&ident.pointer_path()).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(Error::TableNotFound(ident.to_string()));
            }
            Err(e) => return Err(e.into()),
        };
        let e_tag = result.meta.e_tag.clone();
        let pointer: Pointer = serde_json::from_slice(&result.bytes().await?)?;
        let metadata = TableMetadata::parse(&self.read(&pointer.metadata_location).await?)?;
        Ok(Table::new(
            self.clone(),
            ident.clone(),
            metadata,
            pointer.metadata_location,
            e_tag,
        ))
    }
```

`Table` に保存した ETag は、コミットの条件になります（[6章](#6-insert-コミットする)）。`Table` は「ある時点の metadata と、それを読んだときのポインタの ETag」を持つハンドルです。指している metadata は不変ですが、他の書き手のコミットで最新ではなくなることがあります。

`TableMetadata::parse` は JSON を解析して `validate` を呼びます。`validate` が参照先の存在を確認するので、`current_schema` などのアクセサは `expect` で値を取り出せます。

テーブルを作ったあと、DataFusion が `SchemaProvider::register_table` を呼びます。テーブルは既に Catalog にあるので、ここではローカルのテーブル名の一覧に名前を足すだけです。

## 4. INSERT: data file を書く

ここから6章までで、2回目の `INSERT` を追います。1回の INSERT から2個の data file ができるので、パーティション分けの処理を確認できるからです。

```sql
INSERT INTO tour.events VALUES
  (3, TIMESTAMP '2024-01-02 10:00:00', 'c'), (4, TIMESTAMP '2024-01-03 10:00:00', 'd');
```

全体の呼び出しは次のとおりです。

```text
IcebergSchemaProvider::table              crates/datafusion/src/catalog.rs
IcebergTable::insert_into                 crates/datafusion/src/table.rs
IcebergSink::write_all
└ Table::append                           crates/core/src/table.rs
  ├ write_data_files                      crates/core/src/writer.rs    （4章）
  │ ├ conform
  │ ├ group_by_partition
  │ └ write_file                          → data/ts_day=.../<uuid>.parquet
  │   └ collect_metrics
  ├ Table::write_added_manifest           crates/core/src/table.rs     （5章）
  │ ├ write_manifest                      → metadata/<uuid>-m0.avro
  │ └ partition_summaries
  └ for attempt in 0..MAX_COMMIT_ATTEMPTS                              （6章、7章）
    ├ Table::latest
    ├ Table::apply_append
    │ └ write_manifest_list               → metadata/snap-<id>-<seq>-<uuid>.avro
    └ Catalog::commit
      ├ Catalog::write_metadata           → metadata/<NNNNN>-<uuid>.metadata.json
      └ put_opts(PutMode::Update)         → _catalog/tour/events.json
```

### DataFusion から Table::append まで

DataFusion はプランの作成中に、テーブル名を `SchemaProvider::table` で解決します。

`crates/datafusion/src/catalog.rs`

```rust
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some((base, kind)) = name.split_once('$') {
            let Some(table) = self.load(base).await? else {
                return Ok(None);
            };
            return metadata_table(&table, kind).await.map(Some);
        }
        if let Some((base, id)) = name.rsplit_once('@')
            && let Ok(id) = id.parse::<i64>()
        {
            let Some(table) = self.load(base).await? else {
                return Ok(None);
            };
            return Ok(Some(Arc::new(IcebergTable::at_snapshot(table, id))));
        }
        let table = self.load(name).await?;
        Ok(table.map(|t| Arc::new(IcebergTable::new(t)) as Arc<dyn TableProvider>))
    }
```

`events` には `$` も `@` も含まれないので、最後の2行に進みます。`self.load` は `Catalog::load_table` を呼ぶので、ポインタと metadata はここで読まれます。`$` と `@` の分岐は[11章](#11-タイムトラベルとメタデータテーブル)で扱います。

次に物理プランの作成で `IcebergTable::insert_into` が呼ばれます。

`crates/datafusion/src/table.rs`

```rust
        if self.snapshot_id.is_some() || insert_op != InsertOp::Append {
            return Err(DataFusionError::NotImplemented(
                "only appends to the current table are supported".into(),
            ));
        }
        let sink = IcebergSink {
            table: self.table.clone(),
            schema: self.schema.clone(),
        };
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
```

`EXPLAIN INSERT` で確認すると、`VALUES` の出力を `IcebergSink` が受け取るプランになっています。

```text
physical_plan | DataSinkExec: sink=IcebergSink: table=tour.events
              |   ProjectionExec: expr=[column1@0 as id, column2@1 as ts, column3@2 as name]
              |     DataSourceExec: partitions=1, partition_sizes=[1]
```

プランの実行で `IcebergSink::write_all` が呼ばれ、ストリームの全バッチを集めてから `Table::append` に渡します。

`crates/datafusion/src/table.rs`

```rust
        // 全バッチを集めて1回でコミットする。Iceberg の snapshot がコミットの単位なので、
        // 1つの INSERT の行が途中まで見える状態を作らない。
        let batches: Vec<_> = data.try_collect().await?;
        let rows = batches.iter().map(|b| b.num_rows() as u64).sum();
        self.table.append(&batches).await.map_err(to_df)?;
        Ok(rows)
```

`Table::append` が書き込みの全体を制御します。

`crates/core/src/table.rs`

```rust
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
```

処理は `for` の前後で性質が分かれます。`for` の前で書く data file と manifest は一度だけ書き、競合しても再利用します。`for` の中の処理は、競合のたびに最新の状態の上でやり直します。なぜこの分割が成り立つのかは[7章](#7-コミットの競合と再試行)で説明します。

### 列をテーブルのスキーマに合わせる

`write_data_files` は、受け取ったバッチをパーティションごとの Parquet に変えます。

`crates/core/src/writer.rs`

```rust
pub(crate) async fn write_data_files(
    table: &Table,
    batches: &[RecordBatch],
) -> Result<Vec<DataFile>> {
    let schema = table.schema();
    let spec = table.metadata().default_spec();
    let arrow = arrow_schema(&schema.fields);
    let conformed = batches
        .iter()
        .map(|b| conform(b, &arrow))
        .collect::<Result<Vec<_>>>()?;
    let all = concat_batches(&arrow, &conformed)?;
    if all.num_rows() == 0 {
        return Ok(vec![]);
    }

    let groups = group_by_partition(&all, schema, spec)?;
    let writes = groups.into_values().map(|(partition, rows)| {
        let batch = take_record_batch(&all, &UInt32Array::from(rows));
        async move { write_file(table, schema, spec, batch?, partition).await }
    });
    try_join_all(writes).await
}
```

最初の `arrow_schema` は、Iceberg のスキーマから Arrow のスキーマを作ります。ここで作る Arrow のフィールドには、field id がメタデータとして付いています。

`crates/core/src/arrow_schema.rs`

```rust
pub fn arrow_field(f: &Field) -> ArrowField {
    ArrowField::new(&f.name, arrow_type(f.ty), !f.required).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_owned(),
        f.id.to_string(),
    )]))
}
```

`PARQUET_FIELD_ID_META_KEY` を付けたスキーマで `ArrowWriter` を使うと、Parquet の列に field id が記録されます。読み手はこの番号で列を探します（[10章](#10-select-parquet-を読む)）。

`conform` は、入力のバッチをこのスキーマに合わせます。

`crates/core/src/writer.rs`

```rust
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let columns = schema
        .fields()
        .iter()
        .map(|f| match batch.column_by_name(f.name()) {
            Some(c) => Ok(cast_with_options(c, f.data_type(), &options)?),
            None => invalid(format!("missing column {:?}", f.name())),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
```

列は名前で対応づけます。入力のバッチの field id は使えないからです。`VALUES` の出力には field id がなく、`INSERT ... SELECT` の出力には元のテーブルの field id が付いています。`conform` はテーブルのスキーマでバッチを作り直すので、ファイルにはこのテーブルの field id が記録されます。`safe: false` は、変換できない値をエラーにする設定です。既定の `safe: true` は null に置き換えるので、値が気付かれないまま失われます。

### 行をパーティションに分ける

Iceberg では、1つの data file の行はすべて同じパーティション値を持ちます（仕様の「Partitioning」）。読み手がファイル単位でパーティションを判定できるのはこの制約のおかげです。`group_by_partition` は、この制約を満たすように行を分けます。

`crates/core/src/writer.rs`

```rust
    let mut groups = PartitionGroups::new();
    for row in 0..batch.num_rows() {
        let mut partition = Vec::with_capacity(spec.fields.len());
        let mut key = Vec::new();
        for (p, (column, ty)) in spec.fields.iter().zip(&sources) {
            let source = Datum::from_array(batch.column(*column), row, *ty);
            let value = match source {
                Some(v) => p.transform.apply(&v)?,
                None => None,
            };
            match &value {
                Some(v) => {
                    let bytes = v.to_bytes();
                    key.push(1);
                    key.extend((bytes.len() as u64).to_be_bytes());
                    key.extend(bytes);
                }
                None => key.push(0),
            }
            partition.push(value);
        }
        groups
            .entry(key)
            .or_insert_with(|| (partition, vec![]))
            .1
            .push(row as u32);
    }
    Ok(groups)
```

`Datum` は単一の値を表す enum です。同じ値が Arrow の配列、manifest の Avro、統計のバイト列という3つの形で現れるので、`Datum` がそれらの間の共通の表現になります。timestamp はマイクロ秒、date は 1970-01-01 からの日数で持ちます。

行 3 の `ts` は `Datum::Timestamp(1704189600000000)` です。`p.transform.apply` は `Transform::Day` の分岐に進みます。

`crates/core/src/transform.rs`

```rust
fn days_of(v: &Datum) -> i32 {
    match v {
        Datum::Date(d) => *d,
        Datum::Timestamp(t) | Datum::Timestamptz(t) => t.div_euclid(MICROS_PER_DAY) as i32,
        _ => unreachable!("rejected by result_type"),
    }
}
```

`1704189600000000 / 86400000000` を切り捨てると `19724` で、これが 2024-01-02 を表す `Datum::Date(19724)` です。`/` ではなく `div_euclid` を使うのは、1970 年より前の負の値でも小さい側へ切り捨てるためです。

グループのキーは、値の有無の目印、長さ、値のバイト列をつなげたものです。2つの行は次のキーになります。

| 行 | パーティション値 | キー（16進） |
|---|---|---|
| 3 | `Date(19724)` | `01 0000000000000004 0c4d0000` |
| 4 | `Date(19725)` | `01 0000000000000004 0d4d0000` |

目印と長さを付けないと、異なるタプルが同じキーになることがあります。たとえば文字列の2要素のタプルでは、`("ab", "c")` と `("a", "bc")` が同じバイト列になります。

`groups` は2要素になり、`write_data_files` は `take_record_batch` で行を取り出して、グループごとに `write_file` を並行に実行します。

#### day 以外の transform

`Transform::apply` の他の分岐も、値から値への純粋な関数です。

`crates/core/src/transform.rs`

```rust
        Ok(match self {
            Transform::Identity => Some(v.clone()),
            Transform::Void => None,
            Transform::Bucket(n) => Some(Datum::Int(((hash(v) & i32::MAX) as u32 % n) as i32)),
            Transform::Truncate(w) => Some(truncate(v, *w)),
            Transform::Year => Some(Datum::Int(date_of(v)?.year() - 1970)),
            Transform::Month => {
                let d = date_of(v)?;
                Some(Datum::Int((d.year() - 1970) * 12 + d.month0() as i32))
            }
            Transform::Day => Some(Datum::Date(days_of(v))),
            // ...
        })
```

transform の結果はすべての実装で一致する必要があります。`bucket` の値が他の実装とずれると、同じ値の行が別の bucket にあるものとして扱われ、pruning が正しいファイルを除外してしまいます。そのため `hash` は、仕様の「Appendix B」が指定する Murmur3 の x86 32 ビット版を手で実装しています。`crates/integration_tests/tests/transform.rs` の `appendix_b_hashes` が、仕様に載っている値との一致を確認します。

### Parquet を書く

`write_file` は1つのパーティションの行を Parquet にして PUT します。

`crates/core/src/writer.rs`

```rust
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;

    let path = format!(
        "{}/data/{}{}.parquet",
        table.metadata().location,
        partition_dir(spec, &partition),
        uuid::Uuid::new_v4()
    );
    let file_size_in_bytes = buffer.len() as i64;
    table.catalog().write(&path, buffer).await?;
```

`partition_dir` は `ts_day=2024-01-02/` のようなディレクトリ名を作ります。Hive のパーティションと見た目は同じですが、役割が異なります。Hive はパスを解析して値を得るのに対し、Iceberg の読み手は manifest の値を使い、パスを解析しません。ディレクトリ名は人が読むためだけにあるので、`partition_dir` は安全でない文字をエスケープせず `_` に置き換えます。

この時点の data file はどこからも参照されていません。読み手はポインタから順にたどってファイルを見つけるので、コミットするまで読み手に見えません。

`write_file` の後半は、Parquet の情報を `DataFile` に記録します。

`crates/core/src/writer.rs`

```rust
    let mut file = DataFile {
        content: CONTENT_DATA,
        file_path: path,
        file_format: "PARQUET".to_owned(),
        partition,
        record_count: batch.num_rows() as i64,
        file_size_in_bytes,
        value_counts: BTreeMap::new(),
        null_value_counts: BTreeMap::new(),
        nan_value_counts: BTreeMap::new(),
        lower_bounds: BTreeMap::new(),
        upper_bounds: BTreeMap::new(),
        first_row_id: None,
    };
    for (i, f) in schema.fields.iter().enumerate() {
        collect_metrics(&mut file, f, &batch, i);
    }
    Ok(file)
```

`collect_metrics` は列ごとの件数と上下限を計算します。読み手はこの統計を使い、data file を開かずに読む対象を絞ります（[9章](#9-select-読むファイルを3段階で絞る)）。

`crates/core/src/writer.rs`

```rust
    let (mut nulls, mut nans) = (0, 0);
    let mut bounds: Option<(Datum, Datum)> = None;
    for row in 0..batch.num_rows() {
        let Some(v) = Datum::from_array(batch.column(column), row, field.ty) else {
            nulls += 1;
            continue;
        };
        if matches!(v, Datum::Float(x) if x.is_nan()) || matches!(v, Datum::Double(x) if x.is_nan())
        {
            nans += 1;
            continue;
        }
        // ...
    }
    file.value_counts.insert(field.id, batch.num_rows() as i64);
    file.null_value_counts.insert(field.id, nulls);
    if matches!(field.ty, Type::Float | Type::Double) {
        file.nan_value_counts.insert(field.id, nans);
    }
    if let Some((lo, hi)) = bounds {
        file.lower_bounds.insert(field.id, lo.to_bytes());
        file.upper_bounds.insert(field.id, hi.to_bytes());
    }
```

統計のキーは field id です。上下限は `Datum::to_bytes` で仕様の「Appendix D」のバイト列にします。int と long はリトルエンディアン、文字列は UTF-8 のバイト列です。上下限に null と NaN を含めないので、その有無は別の件数で持ちます。

行 3 の data file の `DataFile` は次の値になります。1行だけのファイルなので下限と上限は同じです。

| field id | 列 | `lower_bounds` / `upper_bounds` | 値 |
|---|---|---|---|
| 1 | `id` | `0300000000000000` | 3 |
| 2 | `ts` | `00e8bc8ff30d0600` | 1704189600000000（2024-01-02 10:00:00） |
| 3 | `name` | `63` | `c` |

## 5. INSERT: manifest を書く

manifest は data file の一覧を持つ Avro ファイルです。data file を書き終えたので、`Table::append` は `write_added_manifest` で manifest を書きます。

`crates/core/src/table.rs`

```rust
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
```

1つの data file が manifest の1行（entry）になります。`status` はこの entry の状態で、`STATUS_ADDED` は「この manifest を書いた snapshot が追加したファイル」を表します。`snapshot_id` は `append` の冒頭で決めた値を書きますが、`sequence_number` は `None` のままにします。sequence number はコミットの順序を表す番号で、コミットが成功するまで確定しないからです。この `None` の扱いが[7章](#7-コミットの競合と再試行)の再試行の前提になります。

### Avro への変換

Avro はスキーマ付きのバイナリ形式です。Iceberg の manifest のスキーマは、フィールド名と field id が仕様で決まっています。`manifest_schema` は、この実装で書くスキーマを JSON で組み立てます。

`crates/core/src/manifest.rs`

```rust
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
```

`partition` レコードの形だけはパーティション定義で変わるので、manifest のスキーマは固定にできません。`tour.events` では、`ts_day` という `date` 型のフィールドが1つあるレコードになります。ここで使う型が `Transform::result_type` の結果です。

`value_counts` などの統計は、キーが field id の map です。Avro の map はキーが文字列に限られるので、`int_map` は key と value のレコードの配列として表現します。

`write_manifest` はスキーマを作り、entry を書きます。

`crates/core/src/manifest.rs`

```rust
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
```

スキーマとパーティション定義を Avro のファイルメタデータにも書くのは、仕様の「Manifests」の要求です。この実装の読み手はこれらを使わず、metadata ファイルの定義を使います。

`entry_value` は `ManifestEntry` を Avro の `Value` に変換するだけの関数です。実際に書かれた manifest の1行目を、統計を省略して JSON で示すと次のようになります。

```json
{
  "status": 1,
  "snapshot_id": 3859074123582057973,
  "sequence_number": null,
  "file_sequence_number": null,
  "data_file": {
    "content": 0,
    "file_path": "s3://warehouse/tour/events/data/ts_day=2024-01-02/7f001570-6215-4e4b-a6c8-ef3652863259.parquet",
    "file_format": "PARQUET",
    "partition": { "ts_day": "2024-01-02" },
    "record_count": 1,
    "file_size_in_bytes": 1332
  }
}
```

ファイル上の `sequence_number` は `null` です。

`parse_schema` には1つ回避策があります。`apache_avro` の `Schema::parse` が配列の `logicalType` を捨てるので、`restore_array_attributes` が解析後に `logicalType: map` を復元しています。

### manifest の要約を作る

`write_added_manifest` の戻り値は、manifest list に載せる1行である `ManifestFile` です。

`crates/core/src/table.rs`

```rust
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
```

`sequence_number: 0` は仮の値で、`apply_append` が上書きします。

`partitions` は、読み手が manifest を開かずに読む対象を絞るときに使います。`partition_summaries` は、manifest 内の全ファイルについて、パーティションタプルの要素ごとに上下限と null の有無をまとめます。

`crates/core/src/table.rs`

```rust
    let mut summaries = vec![FieldSummary::default(); fields];
    for (pos, s) in summaries.iter_mut().enumerate() {
        let mut bounds: Option<(&Datum, &Datum)> = None;
        for f in files {
            let Some(v) = &f.partition[pos] else {
                s.contains_null = true;
                continue;
            };
            // ...
        }
        if let Some((lo, hi)) = bounds {
            s.lower_bound = Some(lo.to_bytes());
            s.upper_bound = Some(hi.to_bytes());
        }
        // ...
    }
    summaries
```

2回目の INSERT の manifest は 01-02 と 01-03 のファイルを持つので、要約は `lower_bound = 0c4d0000`（19724）、`upper_bound = 0d4d0000`（19725）になります。

## 6. INSERT: コミットする

`for` の中に入ります。この章は競合がない場合の流れを追い、競合した場合は[7章](#7-コミットの競合と再試行)で扱います。

最初の `Table::latest` は、テーブルを読み直して最新の状態を得ます。

`crates/core/src/table.rs`

```rust
    pub async fn latest(&self) -> Result<Table> {
        let base = self.catalog.load_table(&self.ident).await?;
        if base.metadata.table_uuid != self.metadata.table_uuid {
            return Err(Error::Invalid(format!("table {} was replaced", self.ident)));
        }
        Ok(base)
    }
```

### 新しい snapshot を組み立てる

`apply_append` は、読み直した `base` の上で呼ばれます。前半が manifest の一覧を作る処理です。

`crates/core/src/table.rs`

```rust
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
```

2回目の INSERT では、`last_sequence_number` が 1 なので sequence number は 2 です。`self.manifests(parent)` は親の snapshot の manifest list を読むので、`manifests` は次の2行になります。

| 順 | 由来 | sequence number | `partitions` の範囲 |
|---|---|---|---|
| 1 | この INSERT の manifest | 2 | 01-02〜01-03 |
| 2 | 親から引き継いだ manifest | 1 | 01-01 |

親の manifest は manifest list の行を引き継ぐだけで、manifest のファイルは読みも書き直しもしません。manifest list の大きさは、テーブルの行数ではなく manifest の個数で決まります。

次の v3 の処理は、`next_row_id` が `None` の v2 テーブルでは何もしません（[12章](#12-v3-の-row-lineage)）。その次に manifest list を書きます。

`crates/core/src/table.rs`

```rust
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
```

`write_manifest_list` の構造は `write_manifest` と同じで、Avro のファイルメタデータに snapshot の情報を書き、`ManifestFile` を1行ずつ `Value` にします。

後半は `Snapshot` の組み立てです。

`crates/core/src/table.rs`

```rust
        // 合計は親の summary に足して求める。全 manifest を読み直さずに済む。
        let added_records: i64 = files.iter().map(|f| f.record_count).sum();
        let total = |key: &str| -> i64 {
            parent
                .as_ref()
                .and_then(|p| p.summary.get(key))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
        };
```

`Snapshot` の `timestamp_ms` は `now_ms().max(metadata.last_updated_ms)` です。最後に `add_snapshot` が metadata を更新します。

`crates/core/src/metadata.rs`

```rust
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
```

`snapshots` に過去の snapshot を残すので、過去の状態へのタイムトラベルが可能になります。`refs` の `main` は、仕様の「Table Metadata」が `current-snapshot-id` と同じ snapshot を指すことを求めているフィールドです。

### ポインタを差し替える

ここまでに書いたファイルには、ポインタからまだ到達できません。`Catalog::commit` が metadata ファイルを書き、ポインタを差し替えます。

`crates/core/src/catalog.rs`

```rust
    pub(crate) async fn commit(&self, base: &Table, mut metadata: TableMetadata) -> Result<()> {
        metadata.metadata_log.push(MetadataLogEntry {
            metadata_file: base.metadata_location().to_owned(),
            timestamp_ms: base.metadata().last_updated_ms,
        });
        let metadata_location = self.write_metadata(&metadata).await?;
        let pointer = serde_json::to_vec(&Pointer { metadata_location })?;
        let version = UpdateVersion {
            e_tag: base.e_tag().map(str::to_owned),
            version: None,
        };
        let options = PutOptions {
            mode: PutMode::Update(version),
            ..Default::default()
        };
        match self
            .store
            .put_opts(&base.ident().pointer_path(), pointer.into(), options)
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::Precondition { .. }) => {
                Err(Error::CommitConflict(base.ident().to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }
```

`metadata_log` に親の metadata ファイルを足してから `write_metadata` を呼ぶので、ファイルのバージョン番号は親より1つ進みます。2回目の INSERT では `00002-<uuid>.metadata.json` です。

`PutMode::Update` は `If-Match: <ETag>` 付きの PUT になります。`base` の ETag は `latest` がポインタを読んだときの値なので、その後にポインタが変わっていれば PUT は 412 で失敗します。成功した PUT がコミットの瞬間です。この PUT の前にプロセスが落ちても、読み手が見るテーブルの状態は変わりません。

コミット後の状態を、metadata から順にたどると次のようになります。最後の INSERT まで実行したあとの metadata ファイルから、snapshot の部分を抜粋します。

```json
{
  "last-sequence-number": 3,
  "current-snapshot-id": 3494277665899704234,
  "snapshots": [
    {
      "snapshot-id": 2633461923208599094,
      "sequence-number": 1,
      "manifest-list": "s3://warehouse/tour/events/metadata/snap-2633461923208599094-1-6e5cc967-6008-4cc8-8d8e-b9a31c79d6ce.avro",
      "summary": { "operation": "append", "added-records": "2", "total-records": "2" }
    },
    {
      "snapshot-id": 3859074123582057973,
      "parent-snapshot-id": 2633461923208599094,
      "sequence-number": 2,
      "manifest-list": "s3://warehouse/tour/events/metadata/snap-3859074123582057973-2-0a50f7c7-e48e-40dc-a929-5e72951929ba.avro",
      "summary": { "operation": "append", "added-records": "2", "total-records": "4" }
    },
    {
      "snapshot-id": 3494277665899704234,
      "parent-snapshot-id": 3859074123582057973,
      "sequence-number": 3,
      "manifest-list": "s3://warehouse/tour/events/metadata/snap-3494277665899704234-3-2a7aff55-61a1-404a-870f-76257837d8dc.avro",
      "summary": { "operation": "append", "added-records": "1", "total-records": "5" }
    }
  ],
  "refs": { "main": { "snapshot-id": 3494277665899704234, "type": "branch" } }
}
```

最新の manifest list は3行を持ちます。古い manifest を書き直していないので、1回目と2回目の manifest のパスは、それぞれの INSERT で書いたときのままです。

| `sequence_number` | manifest のパス | `added_files_count` | `lower_bound` | `upper_bound` |
|---|---|---|---|---|
| 3 | `1845ddb2-...-m0.avro` | 1 | `0d4d0000`（01-03） | `0d4d0000`（01-03） |
| 2 | `51a9e4c1-...-m0.avro` | 2 | `0c4d0000`（01-02） | `0d4d0000`（01-03） |
| 1 | `acf73087-...-m0.avro` | 1 | `0b4d0000`（01-01） | `0b4d0000`（01-01） |

## 7. コミットの競合と再試行

Iceberg のコミットは楽観的並行性制御です。書き手はロックを取らず、読んだ状態の上に新しい metadata を作り、ポインタの差し替えで競合を検出します。2つの書き手が同じ状態から同時にコミットすると、次の順に進みます。

```text
書き手 A                         ポインタ      書き手 B
latest（ETag e1）                  e1          latest（ETag e1）
apply_append（seq 6）                          apply_append（seq 6）
write_metadata 00006-aaa                       write_metadata 00006-bbb
PUT If-Match: e1 → 200             e2
                                               PUT If-Match: e1 → 412
                                               sleep(backoff)
                                               latest（ETag e2）
                                               apply_append（seq 7）
                                               write_metadata 00007-ccc
                                   e3          PUT If-Match: e2 → 200
```

`If-Match` により、同じ親から枝分かれする2つのコミットは成立しません。両方の書き手のファイル名に同じバージョン番号が付きますが、UUID があるので上書きし合うことはありません。B の `00006-bbb` は参照されないまま残ります。

追記は他の追記と論理的に衝突しないので（仕様の「Commit Conflict Resolution and Retry」）、B は最新の状態の上で snapshot を作り直すだけでコミットできます。作り直すのは manifest list と metadata ファイルだけです。

| ファイル | 再試行で | 理由 |
|---|---|---|
| data file | 再利用する | 内容が他のコミットに依存しない |
| manifest | 再利用する | sequence number を `null` にしたので、内容がコミットの順序に依存しない。`snapshot_id` は全試行で同じ値を使う |
| manifest list | 作り直す | 親の manifest の一覧と `sequence_number` が変わる |
| metadata ファイル | 作り直す | snapshot の一覧が変わる |

もし manifest に番号を書くなら、競合のたびに番号が変わるので manifest も書き直すことになります。`null` にしておけば番号の実体は manifest list の1か所だけになり、読み手が補います（仕様の「Sequence Number Inheritance」）。補う処理は[9章](#manifest-を読む)にあります。

### 再試行の細部

`append` のループには、単純な再試行に加えて2つの確認があります。

1つ目は `base.metadata.snapshot(snapshot_id)` の確認です。`object_store` の HTTP クライアントは、失敗したように見える PUT を内部で再送することがあります。1回目の PUT が実は成功していると、再送は `If-Match` の不一致で 412 を返します。呼び出し側には競合に見えますが、コミットは適用済みです。ここで再試行すると同じデータを二重にコミットするので、最新の metadata に自分の snapshot があれば成功として返します。`snapshot_id` を `for` の外で決めるのは、この判定のためであり、再利用する manifest に値を書いているためでもあります。

2つ目は `Table::latest` の `table_uuid` の確認です。読み直す間にテーブルが DROP され、同じ名前で別のテーブルが作られていることがあります。そのテーブルにコミットすると、別のテーブルのデータが混ざります。

待ち時間はジッタ付きで増やします。

`crates/core/src/table.rs`

```rust
fn backoff(attempt: u32) -> Duration {
    let jitter = uuid::Uuid::new_v4().as_u128() as u64 % 50;
    Duration::from_millis(20 * attempt.min(10) as u64 + jitter)
}
```

`MAX_COMMIT_ATTEMPTS`（32回）を超えると `Error::CommitConflict` を返します。

失敗した試行の manifest list と metadata ファイル、コミットに至らなかった data file と manifest は、ストレージに残ります。ポインタから到達できないので読み手には見えません。この実装はこれらを片付けません。

## 8. SELECT: フィルタを述語に変換する

ここからは読み取りの処理を、次の `SELECT` で追います。4個の data file のうち、1個だけを読みます。

```sql
SELECT * FROM tour.events WHERE ts >= TIMESTAMP '2024-01-03 11:00:00';
```

`EXPLAIN` の結果は次のとおりです。`IcebergScanExec` の行の数字が、これから追う処理の結果です。`RoundRobinBatch` の数は実行環境の CPU 数で変わります。

```text
logical_plan  | Filter: tour.events.ts >= TimestampMicrosecond(1704279600000000, None)
              |   TableScan: tour.events projection=[id, ts, name], partial_filters=[tour.events.ts >= TimestampMicrosecond(1704279600000000, None)]
physical_plan | FilterExec: ts@1 >= 1704279600000000
              |   RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1
              |     IcebergScanExec: table=tour.events, manifests=2/3, files=1/3, pruned_by_partition=1, pruned_by_metrics=1
```

全体の呼び出しは次のとおりです。

```text
IcebergSchemaProvider::table              crates/datafusion/src/catalog.rs
IcebergTable::supports_filters_pushdown   crates/datafusion/src/table.rs
IcebergTable::scan
├ Table::latest
├ to_predicate                            crates/datafusion/src/filter.rs   （8章）
│ └ convert
├ TableScan::plan                         crates/core/src/scan.rs           （9章）
│ ├ Table::manifests                      ← manifest list を読む
│ ├ project                               crates/core/src/expr.rs
│ ├ manifest_may_match                    段階1
│ ├ Table::entries                        ← manifest を読む
│ ├ partition_matches                     段階2
│ └ metrics_may_match                     段階3
└ IcebergScanExec::new
IcebergScanExec::execute                                                    （10章）
└ read_task                               crates/core/src/scan.rs           ← Parquet を読む
  └ shape_batch
```

### pruning が守る条件

pruning は、行の内容を見ずに読むファイルを減らす処理です。以降のすべての判定は、次の条件だけを守ります。

**条件を満たす行を含みうるファイルを、除外しない。**

逆に、関係のないファイルを残すことは許容します。残ったファイルの行は DataFusion が絞り込むからです。`supports_filters_pushdown` はこのことを DataFusion に伝えています。

`crates/datafusion/src/table.rs`

```rust
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
```

`Inexact` は「押し下げたフィルタで絞るが、残った行の再評価は必要」という意味です。`EXPLAIN` の `partial_filters` と、上に残る `FilterExec` がその結果です。

判定できないときは「含みうる」側に倒す、という方針がこの条件の実装です。`convert` は項を捨て、`project` は `Predicate::True` を返し、`range_may_match` は `true` を返します。

### IcebergTable::scan

DataFusion は物理プランの作成で `IcebergTable::scan` を呼びます。

`crates/datafusion/src/table.rs`

```rust
        // プリペアドステートメントはこのプロバイダを保持し続けるので、プラン作成時の状態は古い場合がある。
        let table = match self.snapshot_id {
            Some(_) => self.table.clone(),
            None => self.table.latest().await.map_err(to_df)?,
        };
        let schema = table.schema();
        let fields: Vec<Field> = match projection {
            Some(p) => p.iter().map(|i| schema.fields[*i].clone()).collect(),
            None => schema.fields.clone(),
        };
        let mut scan = table.scan().filter(to_predicate(filters, schema));
        if let Some(id) = self.snapshot_id {
            scan = scan.snapshot_id(id);
        }
        let plan = scan.plan().await.map_err(to_df)?;
        let target_partitions = state.config().target_partitions();
        Ok(Arc::new(IcebergScanExec::new(
            table.clone(),
            plan,
            fields,
            target_partitions,
        )))
```

`TableScan::plan` はプランの作成中に実行されるので、`EXPLAIN` でも manifest を読み、pruning の件数を表示できます。

### DataFusion の式を Predicate に変換する

core は DataFusion に依存しないので、`Expr` を core の `Predicate` に変換します。

`crates/core/src/expr.rs`

```rust
pub enum Predicate {
    True,
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Cmp(i32, Op, Datum),
    In(i32, Vec<Datum>),
    IsNull(i32),
    NotNull(i32),
}
```

`i32` は列の field id です。`Not` がないのは、pruning の判定が否定できないからです。「`p` を満たす行を含みうる」と判定できても、「`NOT p` を満たす行を含まない」とは言えません。ファイルには `p` を満たす行と満たさない行の両方がありうるからです。そこで否定は変換の時点で内側へ押し込み、`NOT (x = 1)` を `x != 1` にしておきます。

変換の入口が `to_predicate` です。

`crates/datafusion/src/filter.rs`

```rust
pub(crate) fn to_predicate(filters: &[Expr], schema: &Schema) -> Predicate {
    let terms: Vec<_> = filters
        .iter()
        .filter_map(|e| convert(e, schema, false))
        .collect();
    match terms.len() {
        0 => Predicate::True,
        _ => Predicate::And(terms),
    }
}
```

`filters` の要素は暗黙に AND で結ばれているので、変換できない要素は `filter_map` で捨てて構いません。AND の項を捨てると条件が緩むだけで、含みうるファイルを除外しないからです。

`ts >= ...` は `convert` の比較の分岐に進みます。

`crates/datafusion/src/filter.rs`

```rust
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (field, op, value) = match (column(left, schema), column(right, schema)) {
                (Some(f), None) => (f, to_op(*op)?, right),
                (None, Some(f)) => (f, flip(to_op(*op)?), left),
                _ => return None,
            };
            let op = if negated { negate(op) } else { op };
            Some(Predicate::Cmp(field.id, op, literal(value, field.ty)?))
        }
```

左が列、右がリテラルなので1つ目の腕に進みます。`5 < x` のように逆向きなら `flip` で `x > 5` に直します。`literal` は `ScalarValue::TimestampMicrosecond` を `Datum::Timestamp` にします。結果の述語は次のとおりです（Rust の値を簡略化して書きます。以下同様）。

```text
And([Cmp(2, GtEq, Timestamp(1704279600000000))])
```

AND と OR の分岐では、捨ててよい項かどうかを判断します。

`crates/datafusion/src/filter.rs`

```rust
fn connective(
    left: &Expr,
    right: &Expr,
    schema: &Schema,
    negated: bool,
    conjunction: bool,
) -> Option<Predicate> {
    let l = convert(left, schema, negated);
    let r = convert(right, schema, negated);
    if conjunction {
        match (l, r) {
            (Some(l), Some(r)) => Some(Predicate::And(vec![l, r])),
            (l, r) => l.or(r),
        }
    } else {
        Some(Predicate::Or(vec![l?, r?]))
    }
}
```

AND は変換できた側だけを残します。OR は片方でも変換できなければ全体を諦めます。OR の項を捨てると条件が厳しくなり、捨てた項だけを満たす行のファイルを除外してしまうからです。

呼び出し側は、否定の状態に応じて AND と OR を入れ替えます（ド・モルガンの法則）。`lower(name) = 'a'` のような関数呼び出しは変換できないので、次のようになります。

| フィルタ | 変換後の接続 | 結果 |
|---|---|---|
| `id = 1 AND lower(name) = 'a'` | AND | `id = 1` だけを残す |
| `id = 1 OR lower(name) = 'a'` | OR | 全体を捨てる |
| `NOT (id = 1 OR lower(name) = 'a')` | AND | `id != 1` だけを残す |

3行目が、否定を先に押し込む理由です。元の式の見た目は OR ですが、否定を押し込むと AND になるので項を捨てられます。

この表は、`convert` が受け取った式をどう扱うかの説明です。SQL の経路では、`scan` に届く前に DataFusion の簡約が否定の押し込みと `5 < x` の正規化を済ませ、AND の項を `filters` の別々の要素に分けます。そのため、SQL の AND の項を捨てるのは多くの場合 `to_predicate` の `filter_map` です。

## 9. SELECT: 読むファイルを3段階で絞る

`TableScan::plan` は、metadata だけを読んで対象の data file を決めます。各段階では上の階層の要約を使うので、除外できれば下の階層を読まずに済みます。

| 段階 | 関数 | 単位 | 判定に使う情報 | 使う述語 |
|---|---|---|---|---|
| 1 | `manifest_may_match` | manifest | manifest list の `partitions` | 変換後 |
| 2 | `partition_matches` | data file | manifest entry の `partition` | 変換後 |
| 3 | `metrics_may_match` | data file | manifest entry の列統計 | 変換前 |

`plan` の前半で、読む snapshot を決めて manifest list を読みます。

`crates/core/src/scan.rs`

```rust
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
```

`Table::manifests` は `read_manifest_list` を呼びます。読むときはフィールドを名前で探します。そのため、他の実装が余分なフィールドを足したファイルも解析できます。

### 段階1: manifest を除外する

`crates/core/src/scan.rs`

```rust
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
```

manifest list の `partitions` はパーティション値の要約なので、`ts` の条件のままでは比較できません。`project` は列の述語をパーティションの述語に変換します。仕様の「Scan Planning」の inclusive projection です。

inclusive は、「ある行が元の述語を満たすなら、その行のパーティションタプルは変換後の述語を満たす」という性質を指します。逆は成り立たなくて構いません。これは pruning の条件をパーティションの述語に言い換えたものです。

`crates/core/src/expr.rs`

```rust
    let per_field = |id: i32, f: &dyn Fn(usize, Transform) -> Predicate| -> Predicate {
        let terms: Vec<_> = spec
            .fields
            .iter()
            .enumerate()
            .filter(|(_, p)| p.source_id == id)
            .map(|(pos, p)| f(pos, p.transform))
            .filter(|p| *p != Predicate::True)
            .collect();
        and(terms)
    };
```

`per_field` は、列 `id` を元にするパーティションフィールドを探し、それぞれを変換して AND で結びます。列からパーティションフィールドへの対応がなければ `and` は空のリストを受け取って `True` を返します。`WHERE name = 'e'` のようにパーティションと無関係な条件は、段階1と2では何も除外しません。

`Cmp` の変換の本体が `project_cmp` です。

`crates/core/src/expr.rs`

```rust
fn project_cmp(pos: usize, t: Transform, op: Op, v: &Datum) -> Predicate {
    // 離散的な型には隣の値があるので、`x < v` は `x <= v - 1` と書ける。隣の値を変換すると除外が増える。
    // v が 0 時ちょうどのとき、`day(x) <= day(v)` では丸1日分余計に残る。
    let tightened = match (t, op) {
        (Transform::Identity, _) => None,
        (_, Op::Lt) => successor(v, -1).map(|v| (Op::LtEq, v)),
        (_, Op::Gt) => successor(v, 1).map(|v| (Op::GtEq, v)),
        _ => None,
    };
    let (op, v) = tightened.unwrap_or((op, v.clone()));
    let Ok(Some(tv)) = t.apply(&v) else {
        return Predicate::True;
    };
    let pos = pos as i32;
    match (t, op) {
        (Transform::Identity, _) => Predicate::Cmp(pos, op, tv),
        (_, Op::Eq) => Predicate::Cmp(pos, Op::Eq, tv),
        // `x != v` でも、同じパーティションに別の値の行がありうるので除外できない。
        // `bucket` はハッシュなので大小関係を保たず、範囲比較に使えない。
        (_, Op::NotEq) | (Transform::Bucket(_) | Transform::Void, _) => Predicate::True,
        // 残りの transform は単調だが情報を落とす。別の値が同じ結果になるので、厳密な不等号は緩める。
        (_, Op::Lt | Op::LtEq) => Predicate::Cmp(pos, Op::LtEq, tv),
        (_, Op::Gt | Op::GtEq) => Predicate::Cmp(pos, Op::GtEq, tv),
    }
}
```

題材の `ts >= 2024-01-03 11:00:00` は `GtEq` なので `tightened` は `None` です。`Day` を適用すると `Date(19725)` になり、最後の腕で次の述語になります。`i32` の意味が、field id からパーティションタプル内の位置に変わる点に注意してください。

```text
Cmp(0, GtEq, Date(19725))    -- ts_day >= 2024-01-03
```

行の `ts` が 11:00 以降なら、その日付は必ず 01-03 以降です。逆に `ts_day = 01-03` のパーティションには 10:00 の行もありえますが、残す分には問題ありません。

`tightened` は、条件を守ったまま除外を増やす最適化です。`ts < 2024-01-02 00:00:00` を例にすると、次のようになります。

| 手順 | 述語 |
|---|---|
| そのまま変換した場合 | `ts_day <= 2024-01-02` |
| `x <= v - 1` に直す | `ts <= 2024-01-01 23:59:59.999999` |
| 直してから変換した場合 | `ts_day <= 2024-01-01` |

`project` は、`float` と `double` の列の比較と `IN` を `True` にします。

`crates/core/src/expr.rs`

```rust
        Predicate::Cmp(id, _, _) | Predicate::In(id, _) if is_float(*id) => Predicate::True,
```

DataFusion の比較では、正の NaN はどの数より大きく、負の NaN はどの数より小さくなります。一方、上下限は NaN を含みません。`x > 100.0` を上限で判定すると、NaN の行を持つファイルを除外するおそれがあるので、float の比較には上下限を使いません。`IS NULL`、`IS NOT NULL` と、全行が null かどうかの判定は NaN の影響を受けないので、pruning に使います。

変換した述語を、manifest ごとに `manifest_may_match` で判定します。

`crates/core/src/expr.rs`

```rust
pub fn manifest_may_match(pred: &Predicate, summaries: &[FieldSummary], types: &[Type]) -> bool {
    let bounds = |pos: i32| {
        let s = summaries.get(pos as usize)?;
        let ty = *types.get(pos as usize)?;
        let decode = |b: &Option<Vec<u8>>| b.as_ref().and_then(|b| Datum::from_bytes(ty, b).ok());
        Some((s, decode(&s.lower_bound), decode(&s.upper_bound)))
    };
    match pred {
        Predicate::True => true,
        Predicate::And(ps) => ps.iter().all(|p| manifest_may_match(p, summaries, types)),
        Predicate::Or(ps) => ps.iter().any(|p| manifest_may_match(p, summaries, types)),
        Predicate::Cmp(pos, op, v) => bounds(*pos).is_none_or(|(s, lo, hi)| {
            !only_nulls(s) && range_may_match(lo.as_ref(), hi.as_ref(), *op, v)
        }),
        // ...
    }
}
```

要約の上下限はバイト列なので、`partition_type` で求めた型を使って `Datum::from_bytes` で値に戻します。統計を型に依存しない `bytes` で持つので、読み手が型を与えて解釈します。

範囲の判定は `range_may_match` が受け持ちます。

`crates/core/src/expr.rs`

```rust
fn range_may_match(lower: Option<&Datum>, upper: Option<&Datum>, op: Op, v: &Datum) -> bool {
    let lo = lower.and_then(|b| b.compare(v));
    let hi = upper.and_then(|b| b.compare(v));
    match op {
        Op::NotEq => true,
        Op::Lt => lo != Some(Ordering::Greater) && lo != Some(Ordering::Equal),
        Op::LtEq => lo != Some(Ordering::Greater),
        Op::Gt => hi != Some(Ordering::Less) && hi != Some(Ordering::Equal),
        Op::GtEq => hi != Some(Ordering::Less),
        Op::Eq => lo != Some(Ordering::Greater) && hi != Some(Ordering::Less),
    }
}
```

各腕が「除外できる場合」を否定の形で書いているので、上下限が不明で `lo` や `hi` が `None` のときは `true` になります。

`GtEq` は上限だけを見ます。3つの manifest の判定は次のとおりです。

| `sequence_number` | 範囲 | `hi.compare(19725)` | 結果 |
|---|---|---|---|
| 3 | 19725〜19725 | `Equal` | 残す |
| 2 | 19724〜19725 | `Equal` | 残す |
| 1 | 19723〜19723 | `Less` | 除外 |

1回目の INSERT の manifest はファイルを開かずに除外できました。`manifests=2/3` はこの結果です。

### manifest を読む

残った manifest を並行に読みます。

`crates/core/src/scan.rs`

```rust
        let entries = try_join_all(selected.iter().map(|(m, _)| self.table.entries(m))).await?;
```

`Table::entries` は、manifest を書いたときのパーティション定義から型を求めて `read_manifest` を呼びます。[5章](#5-insert-manifest-を書く)で `null` にした sequence number は、ここで補います。

`crates/core/src/manifest.rs`

```rust
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
```

`manifest` 引数は manifest list の1行です。entry の値が `null` で状態が `ADDED` なら、manifest list の値を使います。2回目の INSERT の entry の `sequence_number` は、ファイル上は `null` ですが、ここで `Some(2)` になります。

`read_partition` は、パーティションレコードのフィールドを名前ではなく位置で型と対応づけます。名前は `avro_name` で変換されている場合があるからです。`ts_day` の値は Avro の `date` として読まれ、`Datum::Date(19724)` になります。

### 段階2と段階3: data file を除外する

`crates/core/src/scan.rs`

```rust
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
```

段階2の `partition_matches` は、変換後の述語をファイルのパーティションタプルで評価します。1つのファイルの行はすべて同じタプルを持つので、範囲ではなく単一の値で判定できます。

`crates/core/src/expr.rs`

```rust
pub fn partition_matches(pred: &Predicate, partition: &[Option<Datum>]) -> bool {
    let value = |pos: i32| partition.get(pos as usize).and_then(Option::as_ref);
    match pred {
        Predicate::True => true,
        Predicate::And(ps) => ps.iter().all(|p| partition_matches(p, partition)),
        Predicate::Or(ps) => ps.iter().any(|p| partition_matches(p, partition)),
        Predicate::Cmp(pos, op, v) => value(*pos).is_some_and(|x| {
            let Some(ord) = x.compare(v) else {
                return true;
            };
            match op {
                Op::Eq => ord.is_eq(),
                Op::NotEq => ord.is_ne(),
                Op::Lt => ord.is_lt(),
                Op::LtEq => ord.is_le(),
                Op::Gt => ord.is_gt(),
                Op::GtEq => ord.is_ge(),
            }
        }),
        // ...
    }
}
```

段階3の `metrics_may_match` は、列の統計で判定します。ここで使う述語は変換前の `self.filter` です。統計はパーティション値ではなく列の値についての情報だからです。パーティションにしていない列の条件も、この段階なら除外に使えます。

`crates/core/src/expr.rs`

```rust
pub fn metrics_may_match(pred: &Predicate, file: &DataFile, schema: &Schema) -> bool {
    let all_null = |id: i32| match (file.null_value_counts.get(&id), file.value_counts.get(&id)) {
        (Some(nulls), Some(values)) => nulls == values,
        _ => false,
    };
    let bounds = |id: i32| {
        let ty = schema.field(id)?.ty;
        // [`project`] と同じ理由で、float の上下限は使わない。
        if matches!(ty, Type::Float | Type::Double) {
            return None;
        }
        let decode = |b: Option<&Vec<u8>>| b.and_then(|b| Datum::from_bytes(ty, b).ok());
        Some((
            decode(file.lower_bounds.get(&id)),
            decode(file.upper_bounds.get(&id)),
        ))
    };
    match pred {
        Predicate::True => true,
        Predicate::And(ps) => ps.iter().all(|p| metrics_may_match(p, file, schema)),
        Predicate::Or(ps) => ps.iter().any(|p| metrics_may_match(p, file, schema)),
        Predicate::Cmp(id, op, v) => {
            !all_null(*id)
                && bounds(*id)
                    .is_none_or(|(lo, hi)| range_may_match(lo.as_ref(), hi.as_ref(), *op, v))
        }
        // ...
    }
}
```

`all_null` の判定は、null は比較の述語を満たさないという SQL の規則に基づきます。全行が null の列に比較の条件があれば、そのファイルは除外できます。

残った2つの manifest には3個のファイルがあります。

| manifest | ファイルの行 | 段階2 `ts_day >= 01-03` | 段階3 `ts >= 11:00` | 結果 |
|---|---|---|---|---|
| sequence 3 | id=5（01-03 12:00） | 01-03 で通過 | 上限 12:00 で通過 | 読む |
| sequence 2 | id=3（01-02 10:00） | 01-02 で除外 | 判定しない | `pruned_by_partition` |
| sequence 2 | id=4（01-03 10:00） | 01-03 で通過 | 上限 10:00 で除外 | `pruned_by_metrics` |

`EXPLAIN` の `manifests=2/3, files=1/3, pruned_by_partition=1, pruned_by_metrics=1` が、この結果です。`files=1/3` の分母は段階1を通過した manifest に含まれるファイルの数で、テーブル全体の4個ではありません。

## 10. SELECT: Parquet を読む

`plan` の結果を受けて、`IcebergScanExec::new` が `FileScanTask` を DataFusion のパーティションに分配します。

`crates/datafusion/src/table.rs`

```rust
        let partitions = plan.tasks.len().clamp(1, target_partitions.max(1));
        let mut groups = vec![Vec::new(); partitions];
        for (i, task) in plan.tasks.iter().enumerate() {
            groups[i % partitions].push(task.clone());
        }
```

`clamp(1, ...)` の下限は、タスクが0個でも1つの空のパーティションを作るためです。DataFusion はパーティションごとに `execute` を呼びます。

`crates/datafusion/src/table.rs`

```rust
        let table = self.table.clone();
        let fields: Arc<[Field]> = self.fields.clone().into();
        let tasks = self.groups[partition].clone();
        let stream = futures::stream::iter(tasks)
            .then(move |task| {
                let (table, fields) = (table.clone(), fields.clone());
                async move { read_task(&table, &task, &fields).await }
            })
            .try_flatten()
            .map_err(to_df);
```

パーティション内のタスクを順に `read_task` に渡し、各ファイルのストリームを1本につなげます。

### 列を field id で探す

`read_task` は1つの data file を読みます。

`crates/core/src/scan.rs`

```rust
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
```

ファイルサイズは manifest に記録されているので、それを `with_file_size` で渡します。`ParquetRecordBatchStreamBuilder::new` が Parquet のフッターを読むと、`builder.schema()` からファイルの列がわかります。

`file_columns` は field id からファイル内の列位置への対応表です。[4章](#列をテーブルのスキーマに合わせる)の `arrow_field` が書いた field id を、ここで読み戻します。`tour.events` のファイルでは `{1: 0, 2: 1, 3: 2}` になります。列名ではなく field id で探すので、テーブルの列名が変わっていても、ファイル内の古い名前に影響されません。

field id を1つも持たないファイルは拒否します。一部の列にだけ field id があるファイルでは、field id のない列をファイルにない列として扱います。

`crates/core/src/scan.rs`

```rust
    // field id のないファイルは仕様の name mapping が必要になる。対応しないのでエラーにする。
    // そのまま進むと、全列が null の行を返してしまう。
    if file_columns.is_empty() && !builder.schema().fields().is_empty() {
```

次に、読む列を Parquet のリーダーに指定します。

`crates/core/src/scan.rs`

```rust
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
```

`sources` は出力の各列の取得元です。仮に `projection` が `[name, id]` の順なら、次のようになります。

| 変数 | 値 | 意味 |
|---|---|---|
| `roots` | `[0, 2]` | ソート済みの、読むファイルの列位置 |
| `sources[0]`（`name`） | `Column(1)` | 読んだバッチの2列目 |
| `sources[1]`（`id`） | `Column(0)` | 読んだバッチの1列目 |

ファイルにない列は `Source::Constant` になります。`identity_value` は、その列の値がパーティションタプルにあればそれを使い、なければ null にします。identity パーティションの列はタプルに値そのものがあるので、ファイルに列がなくても復元できます。

### バッチの形を整える

読んだバッチを `projection` の形に直すのが `shape_batch` です。

`crates/core/src/scan.rs`

```rust
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
```

`with_row_count` を指定するのは、`COUNT(*)` のように読む列が1つもない場合があるからです。列のないバッチは列から行数を求められないので、明示します。

バッチは `IcebergScanExec` から `RepartitionExec` を経て `FilterExec` に届きます。`FilterExec` は `ts >= 11:00` を行ごとに再評価します。題材のファイルは id=5 の1行だけで、この行は条件を満たすので、結果は次のようになります。

```text
+----+---------------------+------+
| id | ts                  | name |
+----+---------------------+------+
| 5  | 2024-01-03T12:00:00 | e    |
+----+---------------------+------+
```

ファイルの中に条件を満たさない行が混ざっていれば、ここで落ちます。pruning がファイルを残しすぎても結果が正しいのは、この再評価があるからです。

## 11. タイムトラベルとメタデータテーブル

タイムトラベルは、ここまでの読み取りの入口を変えるだけです。メタデータテーブルは pruning と Parquet の読み出しを通らず、metadata と manifest の内容をメモリ上の表で返します。

### タイムトラベル

`SELECT * FROM tour."events@3859074123582057973"` は、`IcebergSchemaProvider::table` の `@` の分岐に進み、`IcebergTable::at_snapshot` を返します。`snapshot_id` が `Some` なので、`IcebergTable::scan` は `TableScan::snapshot_id` を呼び、`plan` の冒頭で `metadata.snapshot(id)` がその snapshot を選びます。

snapshot は自分の manifest list を持ち、過去の manifest list も manifest も不変なので、処理の起点を変えるだけで過去のテーブルの内容を読めます。スキーマは現在のものを使いますが、この実装にはスキーマを変える操作がないので、この実装で作ったテーブルでは結果に影響しません。過去の snapshot への `INSERT` は `insert_into` が拒否します。

### メタデータテーブル

`"events$snapshots"`、`"events$manifests"`、`"events$files"` は、`$` の分岐から `metadata_table` に進みます。これらは仕様の一部ではなく、Spark などのエンジンが提供する機能に相当します。

`crates/datafusion/src/inspect.rs`

```rust
pub(crate) async fn metadata_table(table: &Table, kind: &str) -> Result<Arc<dyn TableProvider>> {
    let batch = match kind {
        "snapshots" => snapshots(&table.metadata().snapshots),
        "manifests" => manifests(&current_manifests(table).await?),
        "files" => files(table).await,
        other => {
            return Err(DataFusionError::Plan(format!(
                "unknown metadata table {other:?}"
            )));
        }
    }?;
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    Ok(Arc::new(MetadataTable(table)))
}
```

`$snapshots` は metadata の snapshot の一覧を、`$manifests` は `Table::manifests` の結果を、`$files` はさらに `Table::entries` の結果を、メモリ上の表にします。`MetadataTable` で `MemTable` を包むのは、`MemTable` の `insert_into` を隠すためです。そのまま公開すると、INSERT が成功を報告しながら何も保存しません。

`$files` を使うと、[9章](#9-select-読むファイルを3段階で絞る)で判定に使った情報を確認できます。

```sh
mise run sql 'SELECT partition, record_count, lower_bounds, upper_bounds FROM tour."events$files"'
```

```text
+--------------+--------------+---------------------------------------------+---------------------------------------------+
| partition    | record_count | lower_bounds                                | upper_bounds                                |
+--------------+--------------+---------------------------------------------+---------------------------------------------+
| [2024-01-03] | 1            | id=5, ts=2024-01-03T12:00:00.000000, name=e | id=5, ts=2024-01-03T12:00:00.000000, name=e |
| [2024-01-02] | 1            | id=3, ts=2024-01-02T10:00:00.000000, name=c | id=3, ts=2024-01-02T10:00:00.000000, name=c |
| [2024-01-03] | 1            | id=4, ts=2024-01-03T10:00:00.000000, name=d | id=4, ts=2024-01-03T10:00:00.000000, name=d |
| [2024-01-01] | 2            | id=1, ts=2024-01-01T10:00:00.000000, name=a | id=2, ts=2024-01-01T11:00:00.000000, name=b |
+--------------+--------------+---------------------------------------------+---------------------------------------------+
```

`IcebergTableFactory::create` は、名前解決と衝突しないように、`$` と `@` を含むテーブル名を拒否します。

## 12. v3 の row lineage

row lineage は v3 の機能で、テーブルの各行に ID を割り当てます。行の ID は、data file の `first_row_id` にファイル内の位置を足した値です。`first_row_id` は sequence number と同じ理由で、コミット時に確定して読み手が補います。`tour.events` は v2 なので、この章の説明では `OPTIONS ('format-version' '3')` を付けたテーブルを想定します。

`TableMetadata` の `next_row_id` が次に使える ID を持ちます。`apply_append` は、manifest list に載せる manifest のうち、範囲が未割り当てのものに ID を割り当てます。

`crates/core/src/table.rs`

```rust
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
```

割り当ては manifest 単位です。ファイル単位の値は manifest に書かず、`read_manifest` が求めます。

`crates/core/src/manifest.rs`

```rust
        let mut first_row_id = optional(&f, "first_row_id").map(as_i64).transpose()?;
        // row id を消費するのは継承するファイルだけで、値を持つファイルは以前の範囲を保つ。
        // 仕様どおり、前のファイルの `record_count` を足して次の値を求める。
        if status != STATUS_DELETED && first_row_id.is_none() {
            first_row_id = next_row_id;
            next_row_id = next_row_id.map(|id| id + record_count);
        }
```

`next_row_id` の初期値は manifest の `first_row_id` です。manifest 内の entry を順に読みながら `record_count` を足します。この値はファイルの順序に依存しますが、manifest は不変なので、読むたびに同じ結果になります。

`Table::upgrade_to_v3` は `format_version` を 3 に、`next_row_id` を 0 にしてコミットするだけです。既存の manifest は `first_row_id` を持たないので、次のコミットの `apply_append` が他の manifest と同様に範囲を割り当てます。テスト `upgrading_to_v3_assigns_row_ids_to_existing_files_on_the_next_commit` の流れは次のとおりです。

| 操作 | manifest list の順 | 結果 |
|---|---|---|
| v2 で3行を追記 | m1 | `first_row_id` なし |
| `upgrade_to_v3` | m1 | `next_row_id = 0` |
| 1行を追記 | m2、m1 | m2 に 0、m1 に 1 を割り当てる。`next_row_id = 4`、snapshot の `added_rows = 4` |

アップグレード後の最初のコミットでは、`manifests` の先頭が新しい manifest なので、新しいファイルの ID のほうが小さくなります。以降のコミットでは古い manifest が割り当て済みなので、新しいファイルの ID のほうが大きくなります。

この実装が扱うのは ID の割り当てまでで、`_row_id` 列の読み出しは実装していません。割り当てた値はメタデータテーブルの `first_row_id` 列で確認できます。

## 13. DROP TABLE

`DROP TABLE` は `IcebergSession::sql` で振り分けられ、`IcebergSession::drop_table` から `Catalog::drop_table` を呼びます。

`crates/core/src/catalog.rs`

```rust
    pub async fn drop_table(&self, ident: &TableIdent) -> Result<()> {
        // S3 の DELETE は存在しないキーでも成功するので、存在の確認を別に行う。
        let path = ident.pointer_path();
        match self.store.head(&path).await {
            Err(object_store::Error::NotFound { .. }) => {
                return Err(Error::TableNotFound(ident.to_string()));
            }
            other => other?,
        };
        Ok(self.store.delete(&path).await?)
    }
```

消すのはポインタだけです。data file と metadata ファイルは残ります。

DataFusion の通常の経路を使わないのは、`SchemaProvider::deregister_table` が `DROP TABLE` と `CREATE OR REPLACE` の最初の手順の両方から呼ばれるからです。プロバイダからは、確定した削除と、これから失敗するかもしれない置き換えを区別できません。置き換えの途中で失敗すると既存のテーブルが失われるので、プロバイダは登録解除を拒否し、`DROP TABLE` だけを `IcebergSession` で処理します。

## 14. 実装していないものとテスト

この実装は追記だけを扱います。次の機能はありません。

| 機能 | この実装の動作 |
|---|---|
| delete file（行の削除） | 読む snapshot に delete manifest があれば `TableScan::plan` がエラーにする。メタデータテーブルは検査しない |
| スキーマ進化 | 変更の操作がない。読み取りは field id で解決する |
| パーティション進化 | 変更の操作がない。この実装が作るテーブルの `spec_id` は常に 0。読み取りは manifest の `partition_spec_id` で定義を選ぶ |
| name mapping | field id を1つも持たない Parquet の読み取りはエラーにする |
| `initial-default` | ファイルにない列は、identity パーティションの値がなければ null にする |
| 不要なファイルの削除 | 失敗したコミットのファイルも、DROP したテーブルのファイルも残る |
| `_row_id` の読み出し | ID の割り当てだけを行う |

各章の処理の性質は、テストでも確認できます。`rustfs.rs` と `conditional_put.rs` は RustFS が必要です。`regressions.rs` と `manifest.rs` は RustFS なしで実行できます。

| 確かめたい性質 | テスト | ファイル | 章 |
|---|---|---|---|
| RustFS が条件付き PUT を守る | `rustfs_enforces_if_none_match_and_if_match` | `conditional_put.rs` | 3、6 |
| sequence number の継承 | `added_entries_inherit_from_the_manifest_list` | `manifest.rs` | 5、9 |
| 並行コミットが直線の履歴になる | `concurrent_appends_all_land_in_one_linear_history` | `rustfs.rs` | 7 |
| 再送された PUT で二重コミットしない | `a_commit_that_applied_despite_a_conflict_is_not_applied_twice` | `regressions.rs` | 7 |
| 作り直されたテーブルにコミットしない | `a_stale_handle_cannot_commit_to_a_recreated_table` | `regressions.rs` | 7 |
| 3段階の pruning が行を失わない | `every_pruning_stage_removes_work_without_losing_rows` | `rustfs.rs` | 9 |
| 列を field id で解決する | `columns_resolve_by_field_id_not_by_name` | `rustfs.rs` | 10 |
| v3 へのアップグレード後の row id | `upgrading_to_v3_assigns_row_ids_to_existing_files_on_the_next_commit` | `rustfs.rs` | 12 |
| 他の実装が読めるファイルを書く | `iceberg_rust_reads_a_v2_table_written_here` | `rustfs.rs` | 全体 |
