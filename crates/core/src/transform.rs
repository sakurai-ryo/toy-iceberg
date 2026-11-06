use std::fmt;
use std::str::FromStr;

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result, invalid};
use crate::types::Type;
use crate::values::{Datum, UNIX_EPOCH_DAYS_FROM_CE};

const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Transform {
    Identity,
    Bucket(u32),
    Truncate(u32),
    Year,
    Month,
    Day,
    Hour,
    Void,
}

impl fmt::Display for Transform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transform::Identity => f.write_str("identity"),
            Transform::Bucket(n) => write!(f, "bucket[{n}]"),
            Transform::Truncate(w) => write!(f, "truncate[{w}]"),
            Transform::Year => f.write_str("year"),
            Transform::Month => f.write_str("month"),
            Transform::Day => f.write_str("day"),
            Transform::Hour => f.write_str("hour"),
            Transform::Void => f.write_str("void"),
        }
    }
}

impl FromStr for Transform {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let param = |prefix: &str| -> Option<Result<u32>> {
            let n = s.strip_prefix(prefix)?.strip_suffix(']')?;
            Some(match n.parse() {
                Ok(n) if n > 0 => Ok(n),
                _ => invalid(format!("bad transform parameter in {s:?}")),
            })
        };
        Ok(match s {
            "identity" => Transform::Identity,
            "year" => Transform::Year,
            "month" => Transform::Month,
            "day" => Transform::Day,
            "hour" => Transform::Hour,
            "void" => Transform::Void,
            _ => {
                if let Some(n) = param("bucket[") {
                    Transform::Bucket(n?)
                } else if let Some(w) = param("truncate[") {
                    Transform::Truncate(w?)
                } else {
                    return Err(Error::Unsupported(format!("transform {s:?}")));
                }
            }
        })
    }
}

impl TryFrom<String> for Transform {
    type Error = Error;

    fn try_from(s: String) -> Result<Self> {
        s.parse()
    }
}

impl From<Transform> for String {
    fn from(t: Transform) -> Self {
        t.to_string()
    }
}

impl Transform {
    pub fn result_type(&self, source: Type) -> Result<Type> {
        use Type::*;
        let timestamp = matches!(source, Timestamp | Timestamptz);
        let ok = match self {
            Transform::Identity | Transform::Void => true,
            Transform::Bucket(_) => !matches!(source, Boolean | Float | Double),
            Transform::Truncate(_) => matches!(source, Int | Long | String | Binary),
            Transform::Year | Transform::Month | Transform::Day => timestamp || source == Date,
            Transform::Hour => timestamp,
        };
        if !ok {
            return invalid(format!("transform {self} cannot be applied to {source}"));
        }
        Ok(match self {
            Transform::Identity | Transform::Void | Transform::Truncate(_) => source,
            Transform::Day => Date,
            Transform::Bucket(_) | Transform::Year | Transform::Month | Transform::Hour => Int,
        })
    }

    pub fn apply(&self, v: &Datum) -> Result<Option<Datum>> {
        self.result_type(v.ty())?;
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
            Transform::Hour => match v {
                Datum::Timestamp(t) | Datum::Timestamptz(t) => {
                    let hour = t.div_euclid(MICROS_PER_HOUR);
                    let hour = i32::try_from(hour)
                        .map_err(|_| Error::Invalid(format!("hour out of range: {hour}")))?;
                    Some(Datum::Int(hour))
                }
                _ => unreachable!("rejected by result_type"),
            },
        })
    }
}

fn days_of(v: &Datum) -> i32 {
    match v {
        Datum::Date(d) => *d,
        Datum::Timestamp(t) | Datum::Timestamptz(t) => t.div_euclid(MICROS_PER_DAY) as i32,
        _ => unreachable!("rejected by result_type"),
    }
}

fn date_of(v: &Datum) -> Result<NaiveDate> {
    let days = days_of(v);
    days.checked_add(UNIX_EPOCH_DAYS_FROM_CE)
        .and_then(NaiveDate::from_num_days_from_ce_opt)
        .ok_or_else(|| Error::Invalid(format!("date out of range: {days}")))
}

fn truncate(v: &Datum, w: u32) -> Datum {
    match v {
        // Near the type's minimum the exact result is unrepresentable. Saturating keeps the
        // transform monotonic, which inclusive projection relies on.
        Datum::Int(x) => Datum::Int(x.saturating_sub(x.rem_euclid(w as i32))),
        Datum::Long(x) => Datum::Long(x.saturating_sub(x.rem_euclid(w as i64))),
        Datum::String(s) => Datum::String(s.chars().take(w as usize).collect()),
        Datum::Binary(b) => Datum::Binary(b.iter().take(w as usize).copied().collect()),
        _ => unreachable!("rejected by result_type"),
    }
}

/// The 32-bit hash from spec Appendix B. `int` hashes as `long` so that a column
/// promoted from int to long keeps its bucket.
pub fn hash(v: &Datum) -> i32 {
    let long = |x: i64| murmur3_32(&x.to_le_bytes());
    match v {
        Datum::Int(x) | Datum::Date(x) => long(*x as i64),
        Datum::Long(x) | Datum::Timestamp(x) | Datum::Timestamptz(x) => long(*x),
        Datum::String(s) => murmur3_32(s.as_bytes()),
        Datum::Binary(b) => murmur3_32(b),
        Datum::Boolean(_) | Datum::Float(_) | Datum::Double(_) => {
            unreachable!("rejected by result_type")
        }
    }
}

fn murmur3_32(data: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mix = |k: u32| k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);

    let mut h = 0u32;
    let (chunks, tail) = data.as_chunks::<4>();
    for c in chunks {
        h ^= mix(u32::from_le_bytes(*c));
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let k = tail.iter().rev().fold(0u32, |k, b| (k << 8) | *b as u32);
        h ^= mix(k);
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h as i32
}
