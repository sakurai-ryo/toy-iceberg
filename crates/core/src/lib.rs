//! Apache Iceberg のテーブルフォーマットの小さな実装。
//!
//! Iceberg のテーブルは、オブジェクトストレージ上のファイルの木として表現される。
//! 上の階層が下の階層のパスを持つ。
//!
//! ```text
//! ポインタ → metadata ファイル → manifest list → manifest → data file
//! ```
//!
//! ポインタ以外のファイルは一度書いたら変更しない。そのため、ポインタの差し替えだけで
//! テーブル全体を原子的に更新でき、読み手は常に1つの snapshot の整合した状態を見る。
//!
//! 各モジュールが実装する仕様の範囲は次のとおり。仕様の節名は Iceberg Table Spec の見出しを指す。
//!
//! - [`catalog`]: コミットの原子性。「Metastore Tables」
//! - [`metadata`]: metadata ファイルと snapshot。「Table Metadata」「Snapshots」
//! - [`manifest`]: manifest と manifest list の Avro 表現。「Manifests」「Manifest Lists」
//! - [`table`]: 追記のコミット手順。「Commit Conflict Resolution and Retry」
//! - `writer`: data file の書き込みと列統計。「Manifest Entry Fields」
//! - [`scan`]: 読むファイルの選択と Parquet の読み出し。「Scan Planning」
//! - [`expr`]: pruning に使う述語の評価。「Scan Planning」
//! - [`partition`]、[`transform`]: 隠しパーティション。「Partitioning」
//! - [`types`]、[`values`]: スキーマと値の表現。「Schemas and Data Types」「Appendix D」

pub mod arrow_schema;
pub mod catalog;
mod error;
pub mod expr;
pub mod manifest;
pub mod metadata;
pub mod partition;
pub mod scan;
pub mod table;
pub mod transform;
pub mod types;
pub mod values;
mod writer;

pub use error::{Error, Result};
