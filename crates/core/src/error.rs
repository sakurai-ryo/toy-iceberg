pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid: {0}")]
    Invalid(String),
    /// 仕様上は正しいが、この実装が対応しない入力。無視して読み進めると結果が不正確になる場合に返す。
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("table already exists: {0}")]
    TableExists(String),
    /// 別の書き手が先にポインタを更新した。楽観的並行性制御の通常の結果なので、
    /// 呼び出し側は最新の metadata の上でコミットを作り直して再試行する。
    #[error("commit conflict on {0}")]
    CommitConflict(String),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Avro(#[from] Box<apache_avro::Error>),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
}

impl From<apache_avro::Error> for Error {
    fn from(e: apache_avro::Error) -> Self {
        Box::new(e).into()
    }
}

pub(crate) fn invalid<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Invalid(msg.into()))
}
