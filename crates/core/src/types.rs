use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result, invalid};

/// The primitive types this implementation supports. Parsing any other valid
/// Iceberg type fails with [`Error::Unsupported`].
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub ty: Type,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Kept so that a commit does not drop `initial-default` and `write-default`, which this
    /// implementation does not interpret.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawSchema", into = "RawSchema")]
pub struct Schema {
    pub schema_id: i32,
    pub identifier_field_ids: Vec<i32>,
    pub fields: Vec<Field>,
}

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
