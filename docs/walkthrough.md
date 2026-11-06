# 実装ウォークスルー

実際に小さなテーブルを作り、オブジェクトストレージ上のファイルを見ながら、書き込みと読み取りの処理をコードで追います。Iceberg の知識は前提にしません。

コードの場所は行番号ではなくシンボル名（`Table::append` など）で示します。仕様の節名は [Iceberg Table Spec](https://iceberg.apache.org/spec/) の見出しです。

## テーブルを作ってファイルを見る

`mise run up` で RustFS を起動してから、テーブルを作り、3回 INSERT します。

```sh
mise run sql "CREATE EXTERNAL TABLE tour.events (id BIGINT NOT NULL, ts TIMESTAMP, name VARCHAR)
  STORED AS ICEBERG LOCATION '' OPTIONS ('partitioning' 'day(ts)')"
mise run sql "INSERT INTO tour.events VALUES
  (1, TIMESTAMP '2024-01-01 10:00:00', 'a'), (2, TIMESTAMP '2024-01-01 11:00:00', 'b')"
mise run sql "INSERT INTO tour.events VALUES
  (3, TIMESTAMP '2024-01-02 10:00:00', 'c'), (4, TIMESTAMP '2024-01-03 10:00:00', 'd')"
mise run sql "INSERT INTO tour.events VALUES (5, TIMESTAMP '2024-01-03 12:00:00', 'e')"
```

RustFS は1オブジェクトを1ディレクトリとして保存し、その中に `xl.meta` を置きます。次のコマンドでオブジェクトの一覧を確認できます。

```sh
docker exec toy-iceberg-rustfs-1 sh -c \
  "cd /data/warehouse && find _catalog/tour tour -name xl.meta | sed 's|/xl.meta||' | sort"
```

結果は次のとおりです。`<uuid>` と snapshot id は実行ごとに変わります。

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

ファイル数は操作の回数と対応しています。metadata ファイルは CREATE で1個、INSERT ごとに1個増えるので4個です。manifest と manifest list は INSERT ごとに1個ずつ増えるので3個ずつです。Parquet はパーティションごとに分かれるので、2回目の INSERT は2個書き、全体で4個になります。

## ファイルの階層

Iceberg のテーブルは、次の参照関係をたどる木です。上の階層が下の階層のパスを持ちます。

```text
ポインタ                  _catalog/tour/events.json
  └ metadata ファイル     00003-<uuid>.metadata.json   スキーマ、パーティション定義、全 snapshot
      └ manifest list     snap-<id>-3-<uuid>.avro      1 snapshot に属する manifest の一覧
          └ manifest      <uuid>-m0.avro               data file の一覧と列統計
              └ data file <uuid>.parquet               行データ
```

| 階層 | 形式 | Rust の型 | 読み書きするコード |
|---|---|---|---|
| ポインタ | JSON | `Pointer` | `Catalog::load_table`、`Catalog::commit` |
| metadata ファイル | JSON | `TableMetadata`、`Snapshot` | `TableMetadata::parse`、`Catalog::write_metadata` |
| manifest list | Avro | `ManifestFile`、`FieldSummary` | `read_manifest_list`、`write_manifest_list` |
| manifest | Avro | `ManifestEntry`、`DataFile` | `read_manifest`、`write_manifest` |
| data file | Parquet | `RecordBatch` | `read_task`、`write_file` |

ポインタ以外のファイルは一度書いたら変更しません。変更するのはポインタだけなので、ポインタの差し替えがテーブル全体の原子的な更新になります。読み手は、どの時点のポインタをたどっても、1つの snapshot の整合した状態を見ます。

ポインタはテーブルフォーマットの一部ではなく、この実装の Catalog が持つオブジェクトです。仕様の Metastore Tables 方式は、現在の metadata ファイルを指すポインタを check-and-put で更新します。通常は Hive Metastore などが受け持つこの処理を、ここでは S3 の条件付き PUT で実現しています。

snapshot は manifest list を1つ持ちます。`Table::apply_append` は新しい manifest を先頭に置き、親 snapshot の manifest を後ろに続けます。古い manifest は書き直しません。追記で新しく書くのは、manifest 1個と、manifest の数だけ行が増える manifest list です。メタデータテーブルで確認できます。

```sh
mise run sql 'SELECT sequence_number, added_files_count FROM tour."events$manifests"'
```

```text
+-----------------+-------------------+
| sequence_number | added_files_count |
+-----------------+-------------------+
| 3               | 1                 |
| 2               | 2                 |
| 1               | 1                 |
+-----------------+-------------------+
```

過去の snapshot の manifest list も残っているので、タイムトラベル（`"events@<snapshot-id>"`）は `TableScan::snapshot_id` で起点の manifest list を変えるだけで実現できます。

## 書き込み: INSERT からコミットまで

入口は `Table::append` です。DataFusion 経由の INSERT では `IcebergSink::write_all` が全バッチを集めてから呼ぶので、1回の INSERT が1つの snapshot になります。処理は、一度だけ行う前半と、競合時に繰り返す後半に分かれます。

前半では、競合しても無駄にならないファイルを書きます。

1. `write_data_files` が列を名前で合わせ（`conform`）、行をパーティションごとに分けます（`group_by_partition`）。
2. `write_file` がパーティションごとに Parquet を1個書き、`collect_metrics` が列ごとの null 数と上下限を `DataFile` に記録します。
3. `Table::write_added_manifest` が manifest を書きます。このとき entry の `sequence_number` は null のままにします。

後半の `for` ループでは、次の処理を最大 `MAX_COMMIT_ATTEMPTS` 回繰り返します。

1. `Table::latest` で最新の metadata を読みます。`table-uuid` が変わっていれば、同名の別テーブルに置き換わったと判断して失敗します。
2. `Table::apply_append` が最新の状態の上に sequence number を採番し、manifest list を書き、新しい `TableMetadata` を組み立てます。
3. `Catalog::commit` が metadata ファイルを書いたあと、`load_table` のときの ETag を `If-Match` に付けてポインタを PUT します。
4. 他の書き手が先に成功していれば PUT は 412 で失敗し（`Error::CommitConflict`）、少し待ってループの先頭に戻ります。

手順3の `If-Match` が、仕様の check-and-put にあたります。ポインタが読んだ時点から変わっていれば更新できないので、2つの書き手が同じ親から枝分かれすることはありません。

entry の sequence number を null にしておくのは、再試行で manifest を書き直さないためです。sequence number はコミットが成功するまで確定しません。もし manifest に書き込むなら、再試行のたびに manifest も作り直すことになります。null にしておけば、値は manifest list の `sequence_number` だけが持ち、読み手が `read_manifest` で補います（Sequence Number Inheritance）。再試行で作り直すのは manifest list と metadata ファイルの2つで済みます。

ループの先頭にある `base.metadata.snapshot(snapshot_id)` の確認は、見落としやすい処理です。HTTP クライアントが PUT を内部で再送すると、1回目が成功していても2回目が 412 を返すことがあります。その場合に再試行すると同じデータを二重にコミットしてしまうので、最新の metadata に自分の snapshot があれば成功として返します。

コミットできなかった試行の manifest list と metadata ファイル、途中で失敗した書き込みの data file はストレージに残ります。ポインタから到達できないので読み手には見えません。この実装はこれらを片付けません。

## 読み取り: 3段階の pruning

`TableScan::plan` は、読む必要のないファイルを3段階で除外して `FileScanTask` の一覧を作ります。各段階の判定には、上の階層にある要約情報を使います。data file を開く前に、metadata だけで対象を絞るためです。

| 段階 | 関数 | 単位 | 判定に使う情報 |
|---|---|---|---|
| 1 | `manifest_may_match` | manifest | manifest list のパーティション要約（`FieldSummary`） |
| 2 | `partition_matches` | data file | manifest entry のパーティション値 |
| 3 | `metrics_may_match` | data file | manifest entry の列統計（上下限、null 数） |

最初に作ったテーブルに対して、次の `EXPLAIN` を実行します。

```sh
mise run sql "EXPLAIN SELECT * FROM tour.events WHERE ts >= TIMESTAMP '2024-01-03 11:00:00'"
```

```text
FilterExec: ts@1 >= 1704279600000000
  ...
    IcebergScanExec: table=tour.events, manifests=2/3, files=1/3, pruned_by_partition=1, pruned_by_metrics=1
```

段階1と2は列の値ではなくパーティション値を比べるので、述語を `ts` から `ts_day` に変換する必要があります。この変換が `project` で、`ts >= 2024-01-03 11:00:00` は `ts_day >= 2024-01-03` になります。各段階の判定は次のとおりです。

| manifest | data file | 結果 |
|---|---|---|
| sequence 1 | 01-01 の1個 | 段階1で除外。要約の上限が 01-01 |
| sequence 2 | 01-02 の1個 | 段階2で除外。要約の範囲 01-02〜01-03 は条件に重なるが、ファイルのパーティション値は 01-02 |
| sequence 2 | 01-03 の1個 | 段階3で除外。`ts` の上限が 10:00 |
| sequence 3 | 01-03 の1個 | 残る。`ts` の上限が 12:00 |

`files=1/3` の分母は、段階1を通過した manifest に含まれるファイルの数です。

### pruning が守る条件

3段階のどの判定も、「条件を満たす行を含みうるファイルを除外しない」ことだけを保証します。関係のないファイルを残すのは許容します。残ったファイルの行は DataFusion が絞り込むからです。`supports_filters_pushdown` が `Inexact` を返しているので、プランに `FilterExec` が残ります。上の出力の1行目がそれです。

この条件から、実装の細部の判断が導かれます。

- `to_predicate` は変換できない式を捨てます。ただし捨ててよいのは AND の項だけです。AND の項を捨てると条件が緩みますが、OR の項を捨てると条件が厳しくなり、該当するファイルを除外するおそれがあります。`convert` が否定を先に内側へ押し込むのは、ド・モルガンの法則を適用したあとの接続詞で判断するためです。
- `project` は変換後の述語を元の述語より緩くします（仕様の inclusive projection）。`bucket` はハッシュなので大小関係を保てず、範囲比較は `True` になります。
- `project_cmp` は `x < v` を `x <= v - 1` に直してから変換します。`ts < 2024-01-02 00:00:00` をそのまま変換すると `ts_day <= 2024-01-02` になり、1日分余計に残るからです。これは条件を守ったまま除外を増やす最適化です。
- `float` と `double` の述語は pruning に使いません。理由は `project` のコメントにあります。

### data file の読み出し

`read_task` は Parquet の列を名前ではなく field id で探します。`write_data_files` が使う `arrow_field` が、Parquet のスキーマに field id を書き込んでいます。列の同一性を名前ではなく ID で持つので、列名の変更がファイルの書き換えなしで成り立ちます。この実装にスキーマ進化はありませんが、読み取りは他の実装が書いたファイルに対応する必要があるので、仕様どおりに解決します。

ファイルにない列は、identity パーティションの値か null で埋めます（`identity_value`）。

## パーティション

パーティションは、テーブルの列に transform を適用した値です。`day(ts)` なら `ts` から日付を計算します。`write_data_files` は行ごとに transform を適用してファイルを分けるので、1個の data file の行はパーティション値がすべて同じです。段階2でファイル単位の判定ができるのはこのためです。

Hive のパーティションと異なり、利用者は `ts_day` のような列を意識しません。`WHERE ts >= ...` と書けば `project` が変換します。パーティション値は manifest に記録するので、`ts_day=2024-01-01/` というディレクトリ名は読みやすさのためだけにあります。読み手はパスを解析しません（`partition_dir` のコメントを参照してください）。

| 型 | 場所 | 内容 |
|---|---|---|
| `Transform` | `transform.rs` | 値の変換。`bucket` は仕様 Appendix B の Murmur3 ハッシュ |
| `PartitionSpec` | `partition.rs` | `day(ts), bucket(4, id)` の解析と、パーティションフィールドの ID・名前の決定 |
| `Datum` | `values.rs` | 単一の値。統計の上下限は `to_bytes` で仕様 Appendix D のバイト列に変換する |

`transform.rs` の `hash` は、他の実装と同じ値を返す必要があります。異なる値を返すと、同じ `bucket` のはずの行を他の実装が別のファイルに探すことになるからです。仕様 Appendix B の値との照合は `crates/integration_tests/tests/transform.rs` の `appendix_b_hashes` にあります。

## v3 の row lineage

v3 の row lineage は、各行に ID を割り当てる仕組みです。行の ID は、data file の `first_row_id` にファイル内の位置を足した値です。`first_row_id` も sequence number と同じく、コミット時に確定して読み手が補います。

1. `TableMetadata::next_row_id` が次に使える ID を持ちます。
2. `Table::apply_append` が、`first_row_id` を持たない data manifest に範囲を割り当て、`next_row_id` を進めます。
3. manifest entry には書かず、`read_manifest` が manifest の `first_row_id` に `record_count` を足しながら各ファイルの値を求めます（First Row ID Inheritance）。

`Table::upgrade_to_v3` は `format_version` を 3 に、`next_row_id` を 0 にするだけです。既存の manifest は `first_row_id` を持たないので、次のコミットの手順2でまとめて範囲を割り当てられます。

この実装が扱うのは ID の割り当てまでです。`_row_id` 列の読み出しは実装していません。割り当てた値はメタデータテーブルの `first_row_id` 列で確認できます。

## DataFusion との接続

`crates/datafusion` は core の API を DataFusion の trait に対応づける層です。Iceberg の仕様に関わる処理は、ほぼ core にあります。

| SQL | DataFusion の入口 | 呼び出す core の処理 |
|---|---|---|
| `CREATE EXTERNAL TABLE` | `IcebergTableFactory::create` | `Catalog::create_table` |
| テーブル名の解決 | `IcebergSchemaProvider::table` | `Catalog::load_table`。`$` はメタデータテーブル、`@` はタイムトラベル |
| `SELECT` | `IcebergTable::scan`、`IcebergScanExec::execute` | `TableScan::plan`、`read_task` |
| `INSERT` | `IcebergTable::insert_into`、`IcebergSink::write_all` | `Table::append` |
| `DROP TABLE` | `IcebergSession::sql` | `Catalog::drop_table` |

`DROP TABLE` だけ `IcebergSession` で処理する理由は、`IcebergSession` のコメントにあります。

`IcebergScanExec::new` は `FileScanTask` をラウンドロビンで DataFusion のパーティションに分配します。`EXPLAIN` の `IcebergScanExec` 行は `DisplayAs` の実装です。

## テストを読む

テスト名がそのまま実装の性質の説明になっています。`rustfs.rs` と `conditional_put.rs` は RustFS が必要です。`regressions.rs` と `manifest.rs` はメモリ上で完結します。

| 確かめたい性質 | テスト | ファイル |
|---|---|---|
| 3段階の pruning が行を失わない | `every_pruning_stage_removes_work_without_losing_rows` | `rustfs.rs` |
| 並行コミットが直線の履歴になる | `concurrent_appends_all_land_in_one_linear_history` | `rustfs.rs` |
| 再送された PUT で二重コミットしない | `a_commit_that_applied_despite_a_conflict_is_not_applied_twice` | `regressions.rs` |
| 作り直されたテーブルにコミットしない | `a_stale_handle_cannot_commit_to_a_recreated_table` | `regressions.rs` |
| RustFS が条件付き PUT を守る | `rustfs_enforces_if_none_match_and_if_match` | `conditional_put.rs` |
| sequence number の継承 | `added_entries_inherit_from_the_manifest_list` | `manifest.rs` |
| 列を field id で解決する | `columns_resolve_by_field_id_not_by_name` | `rustfs.rs` |
| v3 へのアップグレード後の row id | `upgrading_to_v3_assigns_row_ids_to_existing_files_on_the_next_commit` | `rustfs.rs` |
| 他の実装が読めるファイルを書く | `iceberg_rust_reads_a_v2_table_written_here` | `rustfs.rs` |
