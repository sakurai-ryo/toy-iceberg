use serde::{Deserialize, Serialize};

use crate::error::{Error, Result, invalid};
use crate::transform::Transform;
use crate::types::{Schema, Type};

const FIRST_PARTITION_FIELD_ID: i32 = 1000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    pub transform: Transform,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

impl PartitionSpec {
    pub fn unpartitioned() -> Self {
        PartitionSpec {
            spec_id: 0,
            fields: vec![],
        }
    }

    /// Parses a comma-separated list such as `day(ts), bucket(16, id), truncate(4, s), region`.
    pub fn parse(expr: &str, schema: &Schema) -> Result<Self> {
        let mut fields: Vec<PartitionField> = Vec::new();
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
            // Java refuses to load a table whose partition name shadows a different column.
            let shadows = schema
                .field_by_name(&name)
                .is_some_and(|c| c.id != source.id || transform != Transform::Identity);
            if shadows {
                return invalid(format!("partition field {name:?} conflicts with a column"));
            }
            if fields.iter().any(|f| f.name == name) {
                return invalid(format!("duplicate partition field {name:?}"));
            }
            fields.push(PartitionField {
                source_id: source.id,
                field_id: FIRST_PARTITION_FIELD_ID + fields.len() as i32,
                name,
                transform,
            });
        }
        Ok(PartitionSpec { spec_id: 0, fields })
    }

    pub fn partition_type(&self, schema: &Schema) -> Result<Vec<Type>> {
        self.fields
            .iter()
            .map(|f| {
                let source = schema.field(f.source_id).ok_or_else(|| {
                    Error::Invalid(format!("partition source {} does not exist", f.source_id))
                })?;
                f.transform.result_type(source.ty)
            })
            .collect()
    }

    pub fn last_field_id(&self) -> i32 {
        self.fields
            .iter()
            .map(|f| f.field_id)
            .max()
            .unwrap_or(FIRST_PARTITION_FIELD_ID - 1)
    }
}

fn split_top_level(expr: &str) -> Vec<&str> {
    let mut terms = Vec::new();
    let (mut depth, mut start) = (0, 0);
    for (i, c) in expr.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                terms.push(&expr[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    terms.push(&expr[start..]);
    terms
        .into_iter()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .collect()
}

fn parse_term(term: &str) -> Result<(Transform, &str)> {
    let Some((name, rest)) = term.split_once('(') else {
        return Ok((Transform::Identity, term));
    };
    let Some(args) = rest.strip_suffix(')') else {
        return invalid(format!("bad partition term {term:?}"));
    };
    let args: Vec<&str> = args.split(',').map(str::trim).collect();
    let width = |s: &str| -> Result<u32> {
        match s.parse() {
            Ok(n) if n > 0 => Ok(n),
            _ => invalid(format!("bad width in partition term {term:?}")),
        }
    };
    Ok(match (name.trim(), args.as_slice()) {
        ("identity", [c]) => (Transform::Identity, *c),
        ("year" | "years", [c]) => (Transform::Year, *c),
        ("month" | "months", [c]) => (Transform::Month, *c),
        ("day" | "days", [c]) => (Transform::Day, *c),
        ("hour" | "hours", [c]) => (Transform::Hour, *c),
        ("bucket", [n, c]) => (Transform::Bucket(width(n)?), *c),
        ("truncate", [w, c]) => (Transform::Truncate(width(w)?), *c),
        _ => return invalid(format!("bad partition term {term:?}")),
    })
}
