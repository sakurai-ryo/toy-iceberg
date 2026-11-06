//! DataFusion の式を、pruning 用の [`Predicate`] に変換する。

use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use toy_iceberg_core::expr::{Op, Predicate};
use toy_iceberg_core::types::{Field, Schema, Type};
use toy_iceberg_core::values::Datum;

/// 押し下げられたフィルタを、ファイルの pruning 用に変換する。DataFusion は読んだ行に
/// 同じフィルタを再度適用するので、変換できない式があっても除外が減るだけで済む。
pub(crate) fn to_predicate(filters: &[Expr], schema: &Schema) -> Predicate {
    let terms: Vec<_> = filters
        .iter()
        .filter_map(|e| convert(e, schema, false))
        .collect();
    match terms.len() {
        0 => Predicate::True,
        _ => Predicate::And(terms),
    }
}

/// AND の項を捨てるとフィルタは緩むので安全だが、OR の項を捨てるとフィルタが厳しくなるので
/// 安全でない。否定を変換の途中で内側へ押し込むのは、ド・モルガンの法則を適用したあとに残る
/// 接続詞でこの規則を判断するためである。
fn convert(e: &Expr, schema: &Schema, negated: bool) -> Option<Predicate> {
    match e {
        Expr::Not(inner) => convert(inner, schema, !negated),
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => connective(left, right, schema, negated, !negated),
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => connective(left, right, schema, negated, negated),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (field, op, value) = match (column(left, schema), column(right, schema)) {
                (Some(f), None) => (f, to_op(*op)?, right),
                (None, Some(f)) => (f, flip(to_op(*op)?), left),
                _ => return None,
            };
            let op = if negated { negate(op) } else { op };
            Some(Predicate::Cmp(field.id, op, literal(value, field.ty)?))
        }
        Expr::IsNull(inner) => {
            let id = column(inner, schema)?.id;
            Some(if negated {
                Predicate::NotNull(id)
            } else {
                Predicate::IsNull(id)
            })
        }
        Expr::IsNotNull(inner) => {
            let id = column(inner, schema)?.id;
            Some(if negated {
                Predicate::IsNull(id)
            } else {
                Predicate::NotNull(id)
            })
        }
        Expr::InList(InList {
            expr,
            list,
            negated: not_in,
        }) => {
            let field = column(expr, schema)?;
            let values: Option<Vec<_>> = list.iter().map(|v| literal(v, field.ty)).collect();
            let values = values?;
            if *not_in != negated {
                let terms = values
                    .into_iter()
                    .map(|v| Predicate::Cmp(field.id, Op::NotEq, v));
                Some(Predicate::And(terms.collect()))
            } else {
                Some(Predicate::In(field.id, values))
            }
        }
        Expr::Between(b) => {
            let field = column(&b.expr, schema)?;
            let low = literal(&b.low, field.ty)?;
            let high = literal(&b.high, field.ty)?;
            if b.negated != negated {
                Some(Predicate::Or(vec![
                    Predicate::Cmp(field.id, Op::Lt, low),
                    Predicate::Cmp(field.id, Op::Gt, high),
                ]))
            } else {
                Some(Predicate::And(vec![
                    Predicate::Cmp(field.id, Op::GtEq, low),
                    Predicate::Cmp(field.id, Op::LtEq, high),
                ]))
            }
        }
        _ => None,
    }
}

fn connective(
    left: &Expr,
    right: &Expr,
    schema: &Schema,
    negated: bool,
    conjunction: bool,
) -> Option<Predicate> {
    let l = convert(left, schema, negated);
    let r = convert(right, schema, negated);
    if conjunction {
        match (l, r) {
            (Some(l), Some(r)) => Some(Predicate::And(vec![l, r])),
            (l, r) => l.or(r),
        }
    } else {
        Some(Predicate::Or(vec![l?, r?]))
    }
}

fn column<'a>(e: &Expr, schema: &'a Schema) -> Option<&'a Field> {
    match e {
        Expr::Column(c) => schema.field_by_name(&c.name),
        _ => None,
    }
}

fn to_op(op: Operator) -> Option<Op> {
    Some(match op {
        Operator::Eq => Op::Eq,
        Operator::NotEq => Op::NotEq,
        Operator::Lt => Op::Lt,
        Operator::LtEq => Op::LtEq,
        Operator::Gt => Op::Gt,
        Operator::GtEq => Op::GtEq,
        _ => return None,
    })
}

fn flip(op: Op) -> Op {
    match op {
        Op::Lt => Op::Gt,
        Op::LtEq => Op::GtEq,
        Op::Gt => Op::Lt,
        Op::GtEq => Op::LtEq,
        other => other,
    }
}

fn negate(op: Op) -> Op {
    match op {
        Op::Eq => Op::NotEq,
        Op::NotEq => Op::Eq,
        Op::Lt => Op::GtEq,
        Op::LtEq => Op::Gt,
        Op::Gt => Op::LtEq,
        Op::GtEq => Op::Lt,
    }
}

fn literal(e: &Expr, ty: Type) -> Option<Datum> {
    let Expr::Literal(v, _) = e else {
        return None;
    };
    let integer = match v {
        ScalarValue::Int8(Some(x)) => Some(*x as i64),
        ScalarValue::Int16(Some(x)) => Some(*x as i64),
        ScalarValue::Int32(Some(x)) => Some(*x as i64),
        ScalarValue::Int64(Some(x)) => Some(*x),
        _ => None,
    };
    Some(match (ty, v) {
        (Type::Boolean, ScalarValue::Boolean(Some(x))) => Datum::Boolean(*x),
        (Type::Int, _) => Datum::Int(i32::try_from(integer?).ok()?),
        (Type::Long, _) => Datum::Long(integer?),
        (Type::Float, ScalarValue::Float32(Some(x))) => Datum::Float(*x),
        (Type::Double, ScalarValue::Float64(Some(x))) => Datum::Double(*x),
        (Type::Date, ScalarValue::Date32(Some(x))) => Datum::Date(*x),
        (Type::Timestamp, ScalarValue::TimestampMicrosecond(Some(x), None)) => Datum::Timestamp(*x),
        (Type::Timestamptz, ScalarValue::TimestampMicrosecond(Some(x), Some(_))) => {
            Datum::Timestamptz(*x)
        }
        (
            Type::String,
            ScalarValue::Utf8(Some(x))
            | ScalarValue::LargeUtf8(Some(x))
            | ScalarValue::Utf8View(Some(x)),
        ) => Datum::String(x.clone()),
        (
            Type::Binary,
            ScalarValue::Binary(Some(x))
            | ScalarValue::LargeBinary(Some(x))
            | ScalarValue::BinaryView(Some(x)),
        ) => Datum::Binary(x.clone()),
        _ => return None,
    })
}
