use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use apache_avro::types::Value;
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, new_null_array,
};
use arrow::datatypes::{
    Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
};
use chrono::{DateTime, NaiveDate};

use crate::arrow_schema::arrow_type;
use crate::error::{Result, invalid};
use crate::types::Type;

pub(crate) const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// A single non-null value. Dates are days and timestamps are microseconds from the Unix epoch.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Date(i32),
    Timestamp(i64),
    Timestamptz(i64),
    String(String),
    Binary(Vec<u8>),
}

impl Datum {
    pub fn ty(&self) -> Type {
        match self {
            Datum::Boolean(_) => Type::Boolean,
            Datum::Int(_) => Type::Int,
            Datum::Long(_) => Type::Long,
            Datum::Float(_) => Type::Float,
            Datum::Double(_) => Type::Double,
            Datum::Date(_) => Type::Date,
            Datum::Timestamp(_) => Type::Timestamp,
            Datum::Timestamptz(_) => Type::Timestamptz,
            Datum::String(_) => Type::String,
            Datum::Binary(_) => Type::Binary,
        }
    }

    /// Binary single-value serialization (spec Appendix D).
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Datum::Boolean(v) => vec![*v as u8],
            Datum::Int(v) | Datum::Date(v) => v.to_le_bytes().to_vec(),
            Datum::Long(v) | Datum::Timestamp(v) | Datum::Timestamptz(v) => {
                v.to_le_bytes().to_vec()
            }
            Datum::Float(v) => v.to_le_bytes().to_vec(),
            Datum::Double(v) => v.to_le_bytes().to_vec(),
            Datum::String(v) => v.as_bytes().to_vec(),
            Datum::Binary(v) => v.clone(),
        }
    }

    pub fn from_bytes(ty: Type, b: &[u8]) -> Result<Self> {
        fn fixed<const N: usize>(ty: Type, b: &[u8]) -> Result<[u8; N]> {
            match b.try_into() {
                Ok(a) => Ok(a),
                Err(_) => invalid(format!("{ty} value must be {N} bytes, got {}", b.len())),
            }
        }
        Ok(match ty {
            Type::Boolean => Datum::Boolean(fixed::<1>(ty, b)?[0] != 0),
            Type::Int => Datum::Int(i32::from_le_bytes(fixed(ty, b)?)),
            // Schema promotion lets a bound written as int be read for a long column.
            Type::Long if b.len() == 4 => Datum::Long(i32::from_le_bytes(fixed(ty, b)?) as i64),
            Type::Long => Datum::Long(i64::from_le_bytes(fixed(ty, b)?)),
            Type::Float => Datum::Float(f32::from_le_bytes(fixed(ty, b)?)),
            Type::Double if b.len() == 4 => Datum::Double(f32::from_le_bytes(fixed(ty, b)?) as f64),
            Type::Double => Datum::Double(f64::from_le_bytes(fixed(ty, b)?)),
            Type::Date => Datum::Date(i32::from_le_bytes(fixed(ty, b)?)),
            Type::Timestamp => Datum::Timestamp(i64::from_le_bytes(fixed(ty, b)?)),
            Type::Timestamptz => Datum::Timestamptz(i64::from_le_bytes(fixed(ty, b)?)),
            Type::String => match std::str::from_utf8(b) {
                Ok(s) => Datum::String(s.to_owned()),
                Err(e) => return invalid(format!("bound is not UTF-8: {e}")),
            },
            Type::Binary => Datum::Binary(b.to_vec()),
        })
    }

    /// `None` when the two values have different types. Floats use IEEE 754 totalOrder,
    /// which the spec requires so that `-0.0` sorts before `+0.0` in bounds.
    pub fn compare(&self, other: &Datum) -> Option<Ordering> {
        Some(match (self, other) {
            (Datum::Boolean(a), Datum::Boolean(b)) => a.cmp(b),
            (Datum::Int(a), Datum::Int(b)) | (Datum::Date(a), Datum::Date(b)) => a.cmp(b),
            (Datum::Long(a), Datum::Long(b))
            | (Datum::Timestamp(a), Datum::Timestamp(b))
            | (Datum::Timestamptz(a), Datum::Timestamptz(b)) => a.cmp(b),
            (Datum::Float(a), Datum::Float(b)) => a.total_cmp(b),
            (Datum::Double(a), Datum::Double(b)) => a.total_cmp(b),
            (Datum::String(a), Datum::String(b)) => a.cmp(b),
            (Datum::Binary(a), Datum::Binary(b)) => a.cmp(b),
            _ => return None,
        })
    }

    pub(crate) fn to_avro(&self) -> Value {
        match self {
            Datum::Boolean(v) => Value::Boolean(*v),
            Datum::Int(v) => Value::Int(*v),
            Datum::Long(v) => Value::Long(*v),
            Datum::Float(v) => Value::Float(*v),
            Datum::Double(v) => Value::Double(*v),
            Datum::Date(v) => Value::Date(*v),
            Datum::Timestamp(v) | Datum::Timestamptz(v) => Value::TimestampMicros(*v),
            Datum::String(v) => Value::String(v.clone()),
            Datum::Binary(v) => Value::Bytes(v.clone()),
        }
    }

    pub(crate) fn from_avro(ty: Type, v: &Value) -> Result<Self> {
        Ok(match (ty, v) {
            (Type::Boolean, Value::Boolean(v)) => Datum::Boolean(*v),
            (Type::Int, Value::Int(v)) => Datum::Int(*v),
            (Type::Long, Value::Long(v)) => Datum::Long(*v),
            (Type::Float, Value::Float(v)) => Datum::Float(*v),
            (Type::Double, Value::Double(v)) => Datum::Double(*v),
            // The spec requires readers to accept int for the day transform's date result.
            (Type::Date, Value::Date(v) | Value::Int(v)) => Datum::Date(*v),
            (Type::Timestamp, Value::TimestampMicros(v) | Value::Long(v)) => Datum::Timestamp(*v),
            (Type::Timestamptz, Value::TimestampMicros(v) | Value::Long(v)) => {
                Datum::Timestamptz(*v)
            }
            (Type::String, Value::String(v)) => Datum::String(v.clone()),
            (Type::Binary, Value::Bytes(v)) => Datum::Binary(v.clone()),
            (ty, v) => return invalid(format!("cannot read {v:?} as {ty}")),
        })
    }

    pub(crate) fn from_array(a: &dyn Array, row: usize, ty: Type) -> Option<Self> {
        if a.is_null(row) {
            return None;
        }
        Some(match ty {
            Type::Boolean => Datum::Boolean(a.as_boolean().value(row)),
            Type::Int => Datum::Int(a.as_primitive::<Int32Type>().value(row)),
            Type::Long => Datum::Long(a.as_primitive::<Int64Type>().value(row)),
            Type::Float => Datum::Float(a.as_primitive::<Float32Type>().value(row)),
            Type::Double => Datum::Double(a.as_primitive::<Float64Type>().value(row)),
            Type::Date => Datum::Date(a.as_primitive::<Date32Type>().value(row)),
            Type::Timestamp => {
                Datum::Timestamp(a.as_primitive::<TimestampMicrosecondType>().value(row))
            }
            Type::Timestamptz => {
                Datum::Timestamptz(a.as_primitive::<TimestampMicrosecondType>().value(row))
            }
            Type::String => Datum::String(a.as_string::<i32>().value(row).to_owned()),
            Type::Binary => Datum::Binary(a.as_binary::<i32>().value(row).to_vec()),
        })
    }

    pub(crate) fn constant_array(value: Option<&Datum>, ty: Type, len: usize) -> ArrayRef {
        let Some(value) = value else {
            return new_null_array(&arrow_type(ty), len);
        };
        match value {
            Datum::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; len])),
            Datum::Int(v) => Arc::new(Int32Array::from(vec![*v; len])),
            Datum::Long(v) => Arc::new(Int64Array::from(vec![*v; len])),
            Datum::Float(v) => Arc::new(Float32Array::from(vec![*v; len])),
            Datum::Double(v) => Arc::new(Float64Array::from(vec![*v; len])),
            Datum::Date(v) => Arc::new(Date32Array::from(vec![*v; len])),
            Datum::Timestamp(v) => Arc::new(TimestampMicrosecondArray::from(vec![*v; len])),
            Datum::Timestamptz(v) => {
                Arc::new(TimestampMicrosecondArray::from(vec![*v; len]).with_timezone("UTC"))
            }
            Datum::String(v) => Arc::new(StringArray::from(vec![v.as_str(); len])),
            Datum::Binary(v) => Arc::new(BinaryArray::from(vec![v.as_slice(); len])),
        }
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Datum::Boolean(v) => write!(f, "{v}"),
            Datum::Int(v) => write!(f, "{v}"),
            Datum::Long(v) => write!(f, "{v}"),
            Datum::Float(v) => write!(f, "{v}"),
            Datum::Double(v) => write!(f, "{v}"),
            Datum::Date(v) => {
                match v
                    .checked_add(UNIX_EPOCH_DAYS_FROM_CE)
                    .and_then(NaiveDate::from_num_days_from_ce_opt)
                {
                    Some(d) => write!(f, "{d}"),
                    None => write!(f, "{v}"),
                }
            }
            Datum::Timestamp(v) | Datum::Timestamptz(v) => {
                match DateTime::from_timestamp_micros(*v) {
                    Some(t) => write!(f, "{}", t.format("%Y-%m-%dT%H:%M:%S%.6f")),
                    None => write!(f, "{v}"),
                }
            }
            Datum::String(v) => f.write_str(v),
            Datum::Binary(v) => v.iter().try_for_each(|b| write!(f, "{b:02x}")),
        }
    }
}
