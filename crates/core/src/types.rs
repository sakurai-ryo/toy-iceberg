//! テーブルのスキーマ。仕様の「Schemas and Data Types」と、JSON 表現の「Appendix C: JSON serialization」を実装する。
//!
//! Iceberg のスキーマの特徴は、列を名前ではなく field id で識別することにある。
//! ファイルの統計やパーティション定義はすべて field id で列を参照するので、
//! 列名を変えても既存のファイルを書き換えずに済む。

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result, invalid};

/// この実装が対応するプリミティブ型。仕様の「Primitive Types」の一部にあたる。
/// decimal、uuid、ネスト型など、それ以外の有効な型の解析は [`Error::Unsupported`] で失敗する。
/// 知らない型の列を読み飛ばすと、そのテーブルを正しく扱えないまま処理が進むためである。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Type {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Date,
    Timestamp,
    Timestamptz,
    String,
    Binary,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Type::Boolean => "boolean",
            Type::Int => "int",
            Type::Long => "long",
            Type::Float => "float",
            Type::Double => "double",
            Type::Date => "date",
            Type::Timestamp => "timestamp",
            Type::Timestamptz => "timestamptz",
            Type::String => "string",
            Type::Binary => "binary",
        })
    }
}

impl FromStr for Type {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "boolean" => Type::Boolean,
            "int" => Type::Int,
            "long" => Type::Long,
            "float" => Type::Float,
            "double" => Type::Double,
            "date" => Type::Date,
            "timestamp" => Type::Timestamp,
            "timestamptz" => Type::Timestamptz,
            "string" => Type::String,
            "binary" => Type::Binary,
            other => return Err(Error::Unsupported(format!("type {other:?}"))),
        })
    }
}

impl TryFrom<String> for Type {
    type Error = Error;

    fn try_from(s: String) -> Result<Self> {
        s.parse()
    }
}

impl From<Type> for String {
    fn from(t: Type) -> Self {
        t.to_string()
    }
}

/// スキーマの1列。`id` が列の同一性を表し、`name` は変更できる表示名にあたる。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub ty: Type,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// `initial-default` と `write-default` など、この実装が解釈しないフィールドを保持する。
    /// 捨てると、コミットのたびに他の書き手が保存した情報が metadata から消えてしまう。
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// 1つのバージョンのスキーマ。metadata ファイルは `schema_id` で区別した複数のスキーマを持てる。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawSchema", into = "RawSchema")]
pub struct Schema {
    pub schema_id: i32,
    pub identifier_field_ids: Vec<i32>,
    pub fields: Vec<Field>,
}

/// JSON 上の形。仕様のスキーマは `"type": "struct"` の struct 型として書かれるが、
/// この実装ではトップレベルの struct しか扱わないので、`type` は検査してから捨てる。
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RawSchema {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    schema_id: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    identifier_field_ids: Vec<i32>,
    fields: Vec<Field>,
}

impl TryFrom<RawSchema> for Schema {
    type Error = Error;

    fn try_from(raw: RawSchema) -> Result<Self> {
        if raw.kind != "struct" {
            return invalid(format!("schema type must be struct, got {:?}", raw.kind));
        }
        let schema = Schema {
            schema_id: raw.schema_id,
            identifier_field_ids: raw.identifier_field_ids,
            fields: raw.fields,
        };
        schema.validate()?;
        Ok(schema)
    }
}

impl From<Schema> for RawSchema {
    fn from(s: Schema) -> Self {
        RawSchema {
            kind: "struct".into(),
            schema_id: s.schema_id,
            identifier_field_ids: s.identifier_field_ids,
            fields: s.fields,
        }
    }
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Result<Self> {
        let schema = Schema {
            schema_id: 0,
            identifier_field_ids: vec![],
            fields,
        };
        schema.validate()?;
        Ok(schema)
    }

    pub fn field(&self, id: i32) -> Option<&Field> {
        self.fields.iter().find(|f| f.id == id)
    }

    pub fn field_by_name(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn highest_field_id(&self) -> i32 {
        self.fields.iter().map(|f| f.id).max().unwrap_or(0)
    }

    /// 仕様の制約を検査する。identifier field は行を識別する列なので、「Identifier Field IDs」が
    /// required で float、double でないことを求めている。値の等価性で行を同定するので、
    /// null や NaN を持つ列は使えない。
    fn validate(&self) -> Result<()> {
        let mut ids = HashSet::new();
        let mut names = HashSet::new();
        for f in &self.fields {
            if !ids.insert(f.id) {
                return invalid(format!("duplicate field id {}", f.id));
            }
            if !names.insert(f.name.as_str()) {
                return invalid(format!("duplicate field name {:?}", f.name));
            }
        }
        for id in &self.identifier_field_ids {
            let Some(f) = self.field(*id) else {
                return invalid(format!("identifier field {id} does not exist"));
            };
            if !f.required || matches!(f.ty, Type::Float | Type::Double) {
                return invalid(format!("field {:?} cannot be an identifier", f.name));
            }
        }
        Ok(())
    }
}
