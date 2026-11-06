use std::cmp::Ordering;

use crate::manifest::{DataFile, FieldSummary};
use crate::partition::PartitionSpec;
use crate::transform::Transform;
use crate::types::{Schema, Type};
use crate::values::Datum;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// A filter in negation normal form. The `i32` is a schema field id, except in the output of
/// [`project`], where it is a position in the partition tuple.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    True,
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Cmp(i32, Op, Datum),
    In(i32, Vec<Datum>),
    IsNull(i32),
    NotNull(i32),
}

/// Inclusive projection (spec "Scan Planning"): if a row matches `pred`, the row's partition
/// tuple matches the result. Floats project to `True` because engines order NaN above every
/// number, while bounds exclude NaN, so range checks could drop rows the engine would keep.
pub fn project(pred: &Predicate, spec: &PartitionSpec, schema: &Schema) -> Predicate {
    let per_field = |id: i32, f: &dyn Fn(usize, Transform) -> Predicate| -> Predicate {
        let terms: Vec<_> = spec
            .fields
            .iter()
            .enumerate()
            .filter(|(_, p)| p.source_id == id)
            .map(|(pos, p)| f(pos, p.transform))
            .filter(|p| *p != Predicate::True)
            .collect();
        and(terms)
    };
    let is_float = |id: i32| {
        matches!(
            schema.field(id).map(|f| f.ty),
            Some(Type::Float | Type::Double)
        )
    };
    match pred {
        Predicate::True => Predicate::True,
        Predicate::And(ps) => and(ps.iter().map(|p| project(p, spec, schema)).collect()),
        Predicate::Or(ps) => or(ps.iter().map(|p| project(p, spec, schema)).collect()),
        Predicate::Cmp(id, _, _) | Predicate::In(id, _) if is_float(*id) => Predicate::True,
        Predicate::Cmp(id, op, v) => per_field(*id, &|pos, t| project_cmp(pos, t, *op, v)),
        Predicate::In(id, vs) => per_field(*id, &|pos, t| {
            if t == Transform::Void {
                return Predicate::True;
            }
            let mapped: Option<Vec<_>> = vs.iter().map(|v| t.apply(v).ok().flatten()).collect();
            mapped.map_or(Predicate::True, |vs| Predicate::In(pos as i32, vs))
        }),
        Predicate::IsNull(id) => per_field(*id, &|pos, t| match t {
            Transform::Void => Predicate::True,
            _ => Predicate::IsNull(pos as i32),
        }),
        Predicate::NotNull(id) => per_field(*id, &|pos, t| match t {
            Transform::Void => Predicate::True,
            _ => Predicate::NotNull(pos as i32),
        }),
    }
}

fn project_cmp(pos: usize, t: Transform, op: Op, v: &Datum) -> Predicate {
    // Discrete types have a neighbour, so `x < v` is `x <= v - 1`. Transforming the neighbour
    // is tighter: `day(x) <= day(v)` would keep a whole extra day when v is at midnight.
    let tightened = match (t, op) {
        (Transform::Identity, _) => None,
        (_, Op::Lt) => successor(v, -1).map(|v| (Op::LtEq, v)),
        (_, Op::Gt) => successor(v, 1).map(|v| (Op::GtEq, v)),
        _ => None,
    };
    let (op, v) = tightened.unwrap_or((op, v.clone()));
    let Ok(Some(tv)) = t.apply(&v) else {
        return Predicate::True;
    };
    let pos = pos as i32;
    match (t, op) {
        (Transform::Identity, _) => Predicate::Cmp(pos, op, tv),
        (_, Op::Eq) => Predicate::Cmp(pos, Op::Eq, tv),
        (_, Op::NotEq) | (Transform::Bucket(_) | Transform::Void, _) => Predicate::True,
        // The remaining transforms are monotonic but lossy: strict bounds must relax.
        (_, Op::Lt | Op::LtEq) => Predicate::Cmp(pos, Op::LtEq, tv),
        (_, Op::Gt | Op::GtEq) => Predicate::Cmp(pos, Op::GtEq, tv),
    }
}

fn successor(v: &Datum, delta: i32) -> Option<Datum> {
    Some(match v {
        Datum::Int(x) => Datum::Int(x.checked_add(delta)?),
        Datum::Date(x) => Datum::Date(x.checked_add(delta)?),
        Datum::Long(x) => Datum::Long(x.checked_add(delta as i64)?),
        Datum::Timestamp(x) => Datum::Timestamp(x.checked_add(delta as i64)?),
        Datum::Timestamptz(x) => Datum::Timestamptz(x.checked_add(delta as i64)?),
        _ => return None,
    })
}

fn and(mut ps: Vec<Predicate>) -> Predicate {
    ps.retain(|p| *p != Predicate::True);
    match ps.len() {
        0 => Predicate::True,
        1 => ps.remove(0),
        _ => Predicate::And(ps),
    }
}

fn or(mut ps: Vec<Predicate>) -> Predicate {
    if ps.is_empty() || ps.contains(&Predicate::True) {
        return Predicate::True;
    }
    if ps.len() == 1 {
        ps.remove(0)
    } else {
        Predicate::Or(ps)
    }
}

/// Whether `lower..=upper` can contain a value satisfying `op v`. Unknown bounds always can.
fn range_may_match(lower: Option<&Datum>, upper: Option<&Datum>, op: Op, v: &Datum) -> bool {
    let lo = lower.and_then(|b| b.compare(v));
    let hi = upper.and_then(|b| b.compare(v));
    match op {
        Op::NotEq => true,
        Op::Lt => lo != Some(Ordering::Greater) && lo != Some(Ordering::Equal),
        Op::LtEq => lo != Some(Ordering::Greater),
        Op::Gt => hi != Some(Ordering::Less) && hi != Some(Ordering::Equal),
        Op::GtEq => hi != Some(Ordering::Less),
        Op::Eq => lo != Some(Ordering::Greater) && hi != Some(Ordering::Less),
    }
}

/// Stage 1: tests a projected predicate against a manifest's partition summaries.
pub fn manifest_may_match(pred: &Predicate, summaries: &[FieldSummary], types: &[Type]) -> bool {
    let bounds = |pos: i32| {
        let s = summaries.get(pos as usize)?;
        let ty = *types.get(pos as usize)?;
        let decode = |b: &Option<Vec<u8>>| b.as_ref().and_then(|b| Datum::from_bytes(ty, b).ok());
        Some((s, decode(&s.lower_bound), decode(&s.upper_bound)))
    };
    match pred {
        Predicate::True => true,
        Predicate::And(ps) => ps.iter().all(|p| manifest_may_match(p, summaries, types)),
        Predicate::Or(ps) => ps.iter().any(|p| manifest_may_match(p, summaries, types)),
        Predicate::Cmp(pos, op, v) => bounds(*pos).is_none_or(|(s, lo, hi)| {
            !only_nulls(s) && range_may_match(lo.as_ref(), hi.as_ref(), *op, v)
        }),
        Predicate::In(pos, vs) => bounds(*pos).is_none_or(|(s, lo, hi)| {
            !only_nulls(s)
                && vs
                    .iter()
                    .any(|v| range_may_match(lo.as_ref(), hi.as_ref(), Op::Eq, v))
        }),
        Predicate::IsNull(pos) => summaries.get(*pos as usize).is_none_or(|s| s.contains_null),
        Predicate::NotNull(pos) => summaries.get(*pos as usize).is_none_or(|s| !only_nulls(s)),
    }
}

fn only_nulls(s: &FieldSummary) -> bool {
    s.contains_null && s.lower_bound.is_none() && s.contains_nan != Some(true)
}

/// Stage 2: tests a projected predicate against one file's partition tuple.
pub fn partition_matches(pred: &Predicate, partition: &[Option<Datum>]) -> bool {
    let value = |pos: i32| partition.get(pos as usize).and_then(Option::as_ref);
    match pred {
        Predicate::True => true,
        Predicate::And(ps) => ps.iter().all(|p| partition_matches(p, partition)),
        Predicate::Or(ps) => ps.iter().any(|p| partition_matches(p, partition)),
        Predicate::Cmp(pos, op, v) => value(*pos).is_some_and(|x| {
            let Some(ord) = x.compare(v) else {
                return true;
            };
            match op {
                Op::Eq => ord.is_eq(),
                Op::NotEq => ord.is_ne(),
                Op::Lt => ord.is_lt(),
                Op::LtEq => ord.is_le(),
                Op::Gt => ord.is_gt(),
                Op::GtEq => ord.is_ge(),
            }
        }),
        Predicate::In(pos, vs) => {
            value(*pos).is_some_and(|x| vs.iter().any(|v| x.compare(v).is_none_or(Ordering::is_eq)))
        }
        Predicate::IsNull(pos) => value(*pos).is_none(),
        Predicate::NotNull(pos) => value(*pos).is_some(),
    }
}

/// Stage 3: tests an unprojected predicate against one file's column metrics.
pub fn metrics_may_match(pred: &Predicate, file: &DataFile, schema: &Schema) -> bool {
    let all_null = |id: i32| match (file.null_value_counts.get(&id), file.value_counts.get(&id)) {
        (Some(nulls), Some(values)) => nulls == values,
        _ => false,
    };
    let bounds = |id: i32| {
        let ty = schema.field(id)?.ty;
        if matches!(ty, Type::Float | Type::Double) {
            return None;
        }
        let decode = |b: Option<&Vec<u8>>| b.and_then(|b| Datum::from_bytes(ty, b).ok());
        Some((
            decode(file.lower_bounds.get(&id)),
            decode(file.upper_bounds.get(&id)),
        ))
    };
    match pred {
        Predicate::True => true,
        Predicate::And(ps) => ps.iter().all(|p| metrics_may_match(p, file, schema)),
        Predicate::Or(ps) => ps.iter().any(|p| metrics_may_match(p, file, schema)),
        Predicate::Cmp(id, op, v) => {
            !all_null(*id)
                && bounds(*id)
                    .is_none_or(|(lo, hi)| range_may_match(lo.as_ref(), hi.as_ref(), *op, v))
        }
        Predicate::In(id, vs) => {
            !all_null(*id)
                && bounds(*id).is_none_or(|(lo, hi)| {
                    vs.iter()
                        .any(|v| range_may_match(lo.as_ref(), hi.as_ref(), Op::Eq, v))
                })
        }
        Predicate::IsNull(id) => file.null_value_counts.get(id) != Some(&0),
        Predicate::NotNull(id) => !all_null(*id),
    }
}
