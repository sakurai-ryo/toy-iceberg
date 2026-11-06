//! Catalog の実装。仕様の「Metastore Tables」方式のコミットを、オブジェクトストレージだけで行う。
//!
//! テーブルの実体は不変のファイルなので、「現在の metadata ファイルはどれか」という1つの参照の
//! 更新がコミットになる。仕様は、この差し替えを check-and-put で更新するポインタで実現できると述べている。通常は
//! Hive Metastore などが受け持つ処理で、ここではテーブルごとに
//! `_catalog/<namespace>/<table>.json` というポインタオブジェクトを置き、
//! S3 の条件付き PUT で代用する。
//!
//! ファイル名の rename でコミットする「File System Tables」方式は使わない。
//! 仕様が、オブジェクトストレージでは安全でないとして非推奨にしている。

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result, invalid};
use crate::metadata::{MetadataLogEntry, TableMetadata};
use crate::partition::PartitionSpec;
use crate::table::Table;
use crate::types::Schema;

// テーブルの location は自由に指定できるので、ポインタは location の外の固定の場所にまとめる。
// 一覧の取得はこのプレフィックスのリストで済む。
const POINTER_ROOT: &str = "_catalog";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdent {
    pub namespace: String,
    pub name: String,
}

impl TableIdent {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        TableIdent {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    fn pointer_path(&self) -> Path {
        Path::from(format!(
            "{POINTER_ROOT}/{}/{}.json",
            self.namespace, self.name
        ))
    }
}

impl std::fmt::Display for TableIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

/// 現在の metadata ファイルの場所だけを持つ。この形式はこの実装の独自のもので、仕様にはない。
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Pointer {
    metadata_location: String,
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

impl S3Config {
    /// `TOY_ICEBERG_*` の環境変数を読む。既定値は `compose.yaml` の RustFS を指す。
    pub fn from_env() -> Self {
        let var = |name: &str, default: &str| {
            std::env::var(format!("TOY_ICEBERG_{name}")).unwrap_or_else(|_| default.to_owned())
        };
        S3Config {
            endpoint: var("ENDPOINT", "http://localhost:9000"),
            bucket: var("BUCKET", "warehouse"),
            region: var("REGION", "us-east-1"),
            access_key: var("ACCESS_KEY", "rustfsadmin"),
            secret_key: var("SECRET_KEY", "rustfsadmin"),
        }
    }
}

/// ポインタの読み書きと、ファイルの入出力の窓口。
#[derive(Debug)]
pub struct Catalog {
    store: Arc<dyn ObjectStore>,
    bucket: String,
}

impl Catalog {
    pub fn new(store: Arc<dyn ObjectStore>, bucket: impl Into<String>) -> Arc<Self> {
        Arc::new(Catalog {
            store,
            bucket: bucket.into(),
        })
    }

    pub fn s3(config: &S3Config) -> Result<Arc<Self>> {
        let store = AmazonS3Builder::new()
            .with_endpoint(&config.endpoint)
            .with_allow_http(true)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_access_key_id(&config.access_key)
            .with_secret_access_key(&config.secret_key)
            .build()?;
        Ok(Self::new(Arc::new(store), &config.bucket))
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// metadata に書かれた `s3://...` の完全なパスを、バケット内のキーに変換する。
    /// Iceberg の metadata は他のファイルを完全なパスで参照するので、読み手はこの変換を毎回行う。
    pub fn path(&self, location: &str) -> Result<Path> {
        let key = location
            .strip_prefix("s3://")
            .or_else(|| location.strip_prefix("s3a://"))
            .and_then(|rest| rest.strip_prefix(&self.bucket))
            .and_then(|rest| rest.strip_prefix('/'));
        match key {
            Some(key) => Ok(Path::parse(key).map_err(object_store::Error::from)?),
            None => invalid(format!("{location} is outside bucket {}", self.bucket)),
        }
    }

    pub async fn read(&self, location: &str) -> Result<bytes::Bytes> {
        Ok(self.store.get(&self.path(location)?).await?.bytes().await?)
    }

    pub async fn write(&self, location: &str, bytes: Vec<u8>) -> Result<()> {
        self.store
            .put(&self.path(location)?, PutPayload::from(bytes))
            .await?;
        Ok(())
    }

    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        let root = Path::from(POINTER_ROOT);
        let listing = self.store.list_with_delimiter(Some(&root)).await?;
        Ok(listing
            .common_prefixes
            .iter()
            .filter_map(|p| p.filename().map(decode_segment))
            .collect())
    }

    pub async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let prefix = Path::from(format!("{POINTER_ROOT}/{namespace}"));
        let objects: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
        Ok(objects
            .iter()
            .filter_map(|o| {
                o.location
                    .filename()?
                    .strip_suffix(".json")
                    .map(decode_segment)
            })
            .collect())
    }

    /// テーブルを作成する。最初の metadata ファイルを書いてから、ポインタを `PutMode::Create`
    /// （`If-None-Match: *`）で作る。同名のテーブルを同時に作っても、成功するのは1つだけである。
    pub async fn create_table(
        self: &Arc<Self>,
        ident: &TableIdent,
        schema: Schema,
        spec: PartitionSpec,
        format_version: i32,
        properties: BTreeMap<String, String>,
        location: Option<String>,
    ) -> Result<Table> {
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
    }

    /// ポインタをたどって現在の metadata を読む。ポインタの ETag も覚えておく。
    /// `Catalog::commit` は、この ETag から変わっていないことを条件にポインタを更新する。
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

    /// テーブルを Catalog から外す。data file と metadata ファイルは残す。
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

    /// ポインタが `base` の状態のままなら `metadata` を公開する。変わっていれば
    /// [`Error::CommitConflict`] を返すので、呼び出し側は最新の metadata から作り直す。
    ///
    /// 仕様の「Metastore Tables」の手順そのもので、次の順に処理する。
    ///
    /// 1. 新しい metadata ファイルを一意な名前で書く。
    /// 2. ポインタを `If-Match` 付きの PUT で差し替える。これがコミットの瞬間である。
    /// 3. PUT が 412 を返したら失敗とする。手順1のファイルはどこからも参照されないまま残る。
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

    /// 仕様の命名規則 `<V>-<random-uuid>.metadata.json` で書く。UUID があるので、同じバージョンを
    /// 同時に書く書き手どうしが上書きし合うことはない。
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
}

/// `Path` が返すセグメントは、非 ASCII 文字などがパーセントエンコードされている。
fn decode_segment(segment: &str) -> String {
    percent_decode_str(segment).decode_utf8_lossy().into_owned()
}

pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
