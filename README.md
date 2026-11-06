# toy-iceberg

Apache Iceberg のテーブルフォーマットを学ぶための小さな Rust 実装です。クエリエンジンに Apache DataFusion、ストレージに RustFS を使います。

実装の読み方は [docs/walkthrough.md](docs/walkthrough.md) にまとめています。

## 構成

| クレート | 役割 |
|---|---|
| `crates/core` | metadata、manifest、スキャン計画、書き込み、Catalog。DataFusion に依存しない |
| `crates/datafusion` | `TableProvider`、`INSERT`、`CREATE EXTERNAL TABLE`、メタデータテーブル |
| `crates/cli` | SQL を1文実行する `toy-iceberg` バイナリ |
| `crates/integration_tests` | 公式 fixtures、`iceberg-rust` からの移植テスト、RustFS 上の結合テスト |

## 使い方

```sh
mise run up
mise run sql "CREATE EXTERNAL TABLE demo.events (id BIGINT NOT NULL, ts TIMESTAMP, name VARCHAR)
  STORED AS ICEBERG LOCATION '' OPTIONS ('partitioning' 'day(ts), bucket(4, id)')"
mise run sql "INSERT INTO demo.events VALUES (1, TIMESTAMP '2024-01-01 10:00:00', 'a')"
mise run sql "SELECT * FROM demo.events"
mise run test
```

`LOCATION ''` は既定の場所 `s3://warehouse/<namespace>/<table>` を選びます。`OPTIONS` の `'format-version' '3'` で v3 のテーブルを作成できます。既定は v2 です。

接続先は `TOY_ICEBERG_ENDPOINT`、`TOY_ICEBERG_BUCKET`、`TOY_ICEBERG_REGION`、`TOY_ICEBERG_ACCESS_KEY`、`TOY_ICEBERG_SECRET_KEY` で変更できます。既定値は `compose.yaml` の RustFS を指します。

スナップショットと内部構造は、テーブル名に接尾辞を付けて参照します。識別子はダブルクォートで囲みます。

```sql
SELECT * FROM demo."events$snapshots";
SELECT * FROM demo."events$manifests";
SELECT * FROM demo."events$files";
SELECT * FROM demo."events@<snapshot-id>";
```

`EXPLAIN` の `IcebergScanExec` 行に、各段階で除外した manifest とファイルの数が出ます。

## コミットの仕組み

Catalog は `_catalog/<namespace>/<table>.json` のポインタオブジェクトです。中身は現在の metadata ファイルの場所だけです。コミットは新しい metadata ファイルを書いたあと、ポインタを `If-Match` 付きの PUT で更新します。これは仕様の Metastore Tables 方式の check-and-put にあたります。別の書き込みが先に成功すると PUT は 412 で失敗し、最新の metadata の上に manifest list と metadata を作り直して再試行します。

## 対応範囲

- フォーマット: v2、v3 の row lineage、v2 から v3 へのアップグレード（core の API のみ）
- 型: boolean、int、long、float、double、date、timestamp、timestamptz、string、binary
- パーティション: identity、bucket、truncate、year、month、day、hour
- pruning: manifest list のパーティション要約、manifest のパーティション値、data file の列統計

テーブルを作成できるのは `CREATE EXTERNAL TABLE ... STORED AS ICEBERG` だけです。`CREATE TABLE`、`CREATE VIEW`、`CREATE OR REPLACE` はエラーになります。`DROP TABLE` は `session()` が返す `IcebergSession` の `sql` で実行します。削除するのはポインタだけで、データファイルと metadata は残ります。

行削除、スキーマ進化、パーティション進化、compaction、snapshot の expire、branch と tag、v3 の新しい型、decimal、uuid、ネスト型は対象外です。delete file を含むテーブルの読み取りはエラーになります。

`float` と `double` の述語は pruning に使いません。統計の上下限は NaN を含まない一方、DataFusion は NaN を最大の値として扱うため、範囲で除外すると該当行を失うからです。
