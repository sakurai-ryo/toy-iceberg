// Ported from apache/iceberg-rust, licensed under the Apache License, Version 2.0.
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::BTreeMap;

use toy_iceberg_core::expr::{
    Op, Predicate, manifest_may_match, metrics_may_match, partition_matches, project,
};
use toy_iceberg_core::manifest::{DataFile, FieldSummary};
use toy_iceberg_core::partition::PartitionSpec;
use toy_iceberg_core::types::{Field, Schema, Type};
use toy_iceberg_core::values::Datum;

const INT_MIN_VALUE: i32 = 30;
const INT_MAX_VALUE: i32 = 79;

fn field(id: i32, name: &str, ty: Type, required: bool) -> Field {
    Field {
        id,
        name: name.into(),
        ty,
        required,
        doc: None,
        extra: Default::default(),
    }
}

fn schema() -> Schema {
    Schema::new(vec![
        field(1, "id", Type::Int, true),
        field(2, "no_stats", Type::Int, false),
        field(3, "required", Type::String, true),
        field(4, "all_nulls", Type::String, false),
        field(5, "some_nulls", Type::String, false),
        field(6, "no_nulls", Type::String, false),
        field(7, "value", Type::Double, false),
    ])
    .unwrap()
}

fn file() -> DataFile {
    let counts = |pairs: &[(i32, i64)]| pairs.iter().copied().collect::<BTreeMap<_, _>>();
    DataFile {
        content: 0,
        file_path: "/test/path".into(),
        file_format: "PARQUET".into(),
        partition: vec![],
        record_count: 50,
        file_size_in_bytes: 10,
        value_counts: counts(&[(4, 50), (5, 50), (6, 50), (7, 50)]),
        null_value_counts: counts(&[(4, 50), (5, 10), (6, 0)]),
        nan_value_counts: counts(&[(7, 10)]),
        lower_bounds: [
            (1, Datum::Int(INT_MIN_VALUE).to_bytes()),
            (7, 1f64.to_le_bytes().to_vec()),
        ]
        .into(),
        upper_bounds: [
            (1, Datum::Int(INT_MAX_VALUE).to_bytes()),
            (7, 2f64.to_le_bytes().to_vec()),
        ]
        .into(),
        first_row_id: None,
    }
}

fn may_match(p: Predicate) -> bool {
    metrics_may_match(&p, &file(), &schema())
}

fn id(op: Op, v: i32) -> Predicate {
    Predicate::Cmp(1, op, Datum::Int(v))
}

fn string(field: i32, op: Op) -> Predicate {
    Predicate::Cmp(field, op, Datum::String("a".into()))
}

#[test]
fn all_nulls() {
    assert!(
        !may_match(Predicate::NotNull(4)),
        "no non-null value in all null column"
    );
    assert!(!may_match(string(4, Op::Lt)));
    assert!(!may_match(string(4, Op::LtEq)));
    assert!(!may_match(string(4, Op::Gt)));
    assert!(!may_match(string(4, Op::GtEq)));
    assert!(!may_match(string(4, Op::Eq)));
    assert!(may_match(Predicate::NotNull(5)));
    assert!(may_match(Predicate::NotNull(6)));
}

#[test]
fn no_nulls() {
    assert!(may_match(Predicate::IsNull(4)));
    assert!(may_match(Predicate::IsNull(5)));
    assert!(!may_match(Predicate::IsNull(6)), "null count is zero");
}

#[test]
fn missing_stats_always_read() {
    assert!(may_match(Predicate::Cmp(2, Op::Lt, Datum::Int(5))));
    assert!(may_match(Predicate::IsNull(2)));
    assert!(may_match(Predicate::NotNull(2)));
}

#[test]
fn integer_lt() {
    assert!(!may_match(id(Op::Lt, INT_MIN_VALUE - 25)), "5 < 30");
    assert!(!may_match(id(Op::Lt, INT_MIN_VALUE)), "30 is not < 30");
    assert!(may_match(id(Op::Lt, INT_MIN_VALUE + 1)), "one possible id");
    assert!(may_match(id(Op::Lt, INT_MAX_VALUE)), "many possible ids");
}

#[test]
fn integer_lt_eq() {
    assert!(!may_match(id(Op::LtEq, INT_MIN_VALUE - 25)));
    assert!(!may_match(id(Op::LtEq, INT_MIN_VALUE - 1)));
    assert!(may_match(id(Op::LtEq, INT_MIN_VALUE)));
    assert!(may_match(id(Op::LtEq, INT_MAX_VALUE)));
}

#[test]
fn integer_gt() {
    assert!(!may_match(id(Op::Gt, INT_MAX_VALUE + 6)));
    assert!(!may_match(id(Op::Gt, INT_MAX_VALUE)), "79 is not > 79");
    assert!(may_match(id(Op::Gt, INT_MAX_VALUE - 1)));
    assert!(may_match(id(Op::Gt, INT_MAX_VALUE - 4)));
}

#[test]
fn integer_gt_eq() {
    assert!(!may_match(id(Op::GtEq, INT_MAX_VALUE + 6)));
    assert!(!may_match(id(Op::GtEq, INT_MAX_VALUE + 1)));
    assert!(may_match(id(Op::GtEq, INT_MAX_VALUE)));
    assert!(may_match(id(Op::GtEq, INT_MAX_VALUE - 4)));
}

#[test]
fn integer_eq() {
    assert!(!may_match(id(Op::Eq, INT_MIN_VALUE - 25)));
    assert!(!may_match(id(Op::Eq, INT_MIN_VALUE - 1)));
    assert!(may_match(id(Op::Eq, INT_MIN_VALUE)));
    assert!(may_match(id(Op::Eq, INT_MAX_VALUE - 4)));
    assert!(may_match(id(Op::Eq, INT_MAX_VALUE)));
    assert!(!may_match(id(Op::Eq, INT_MAX_VALUE + 1)));
    assert!(!may_match(id(Op::Eq, INT_MAX_VALUE + 6)));
}

#[test]
fn integer_not_eq_always_reads() {
    assert!(may_match(id(Op::NotEq, INT_MIN_VALUE - 25)));
    assert!(may_match(id(Op::NotEq, INT_MIN_VALUE)));
    assert!(may_match(id(Op::NotEq, INT_MAX_VALUE + 6)));
}

#[test]
fn integer_in() {
    let list = |vs: &[i32]| Predicate::In(1, vs.iter().map(|v| Datum::Int(*v)).collect());
    assert!(
        !may_match(list(&[INT_MIN_VALUE - 25, INT_MIN_VALUE - 24])),
        "below lower bound"
    );
    assert!(may_match(list(&[INT_MIN_VALUE - 1, INT_MIN_VALUE])));
    assert!(may_match(list(&[INT_MAX_VALUE, INT_MAX_VALUE + 1])));
    assert!(
        !may_match(list(&[INT_MAX_VALUE + 1, INT_MAX_VALUE + 2])),
        "above upper bound"
    );
    assert!(
        !may_match(list(&[INT_MIN_VALUE - 25, INT_MAX_VALUE + 25])),
        "straddles the bounds"
    );
}

#[test]
fn and_or() {
    let skip = id(Op::Lt, INT_MIN_VALUE - 25);
    let read = id(Op::GtEq, INT_MIN_VALUE);
    assert!(!may_match(Predicate::And(vec![skip.clone(), read.clone()])));
    assert!(may_match(Predicate::Or(vec![skip.clone(), read])));
    assert!(!may_match(Predicate::Or(vec![skip.clone(), skip])));
}

#[test]
fn floats_are_never_pruned_because_nan_is_outside_the_bounds() {
    assert!(may_match(Predicate::Cmp(7, Op::Gt, Datum::Double(100.0))));
    let spec = PartitionSpec::parse("value", &schema()).unwrap();
    let pred = Predicate::Cmp(7, Op::Gt, Datum::Double(100.0));
    assert_eq!(project(&pred, &spec, &schema()), Predicate::True);
}

fn date_schema() -> Schema {
    Schema::new(vec![field(1, "value", Type::Date, true)]).unwrap()
}

fn date(s: &str) -> Datum {
    let (y, rest) = s.split_at(4);
    let (y, m, d): (i64, i64, i64) = (
        y.parse().unwrap(),
        rest[1..3].parse().unwrap(),
        rest[4..6].parse().unwrap(),
    );
    let days = (y - 1970) * 365
        + (y - 1969).div_euclid(4)
        + [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334][m as usize - 1]
        + d
        - 1;
    Datum::Date(days as i32)
}

fn project_date(term: &str, op: Op, value: &str) -> Predicate {
    let spec = PartitionSpec::parse(term, &date_schema()).unwrap();
    project(&Predicate::Cmp(1, op, date(value)), &spec, &date_schema())
}

#[test]
fn projection_date_day_negative() {
    let v = "1969-12-30";
    let cmp = |op, s| Predicate::Cmp(0, op, date(s));
    assert_eq!(
        project_date("day(value)", Op::Lt, v),
        cmp(Op::LtEq, "1969-12-29")
    );
    assert_eq!(
        project_date("day(value)", Op::LtEq, v),
        cmp(Op::LtEq, "1969-12-30")
    );
    assert_eq!(
        project_date("day(value)", Op::Gt, v),
        cmp(Op::GtEq, "1969-12-31")
    );
    assert_eq!(
        project_date("day(value)", Op::GtEq, v),
        cmp(Op::GtEq, "1969-12-30")
    );
    assert_eq!(
        project_date("day(value)", Op::Eq, v),
        cmp(Op::Eq, "1969-12-30")
    );
    assert_eq!(project_date("day(value)", Op::NotEq, v), Predicate::True);
}

#[test]
fn projection_date_month_uses_the_neighbouring_month_at_a_boundary() {
    let cmp = |op, v| Predicate::Cmp(0, op, Datum::Int(v));
    assert_eq!(
        project_date("month(value)", Op::Lt, "2017-12-01"),
        cmp(Op::LtEq, 574)
    );
    assert_eq!(
        project_date("month(value)", Op::LtEq, "2017-12-01"),
        cmp(Op::LtEq, 575)
    );
    assert_eq!(
        project_date("month(value)", Op::Gt, "2017-12-31"),
        cmp(Op::GtEq, 576)
    );
    assert_eq!(
        project_date("month(value)", Op::GtEq, "2017-12-31"),
        cmp(Op::GtEq, 575)
    );
}

#[test]
fn projection_bucket_supports_only_equality() {
    let schema = Schema::new(vec![field(1, "value", Type::Int, true)]).unwrap();
    let spec = PartitionSpec::parse("bucket(10, value)", &schema).unwrap();
    let cmp = |op| project(&Predicate::Cmp(1, op, Datum::Int(100)), &spec, &schema);
    assert_eq!(cmp(Op::Eq), Predicate::Cmp(0, Op::Eq, Datum::Int(6)));
    for op in [Op::NotEq, Op::Lt, Op::LtEq, Op::Gt, Op::GtEq] {
        assert_eq!(cmp(op), Predicate::True);
    }
    let list = Predicate::In(1, vec![Datum::Int(99), Datum::Int(100), Datum::Int(101)]);
    let Predicate::In(0, buckets) = project(&list, &spec, &schema) else {
        panic!("expected an IN predicate on position 0");
    };
    let mut buckets: Vec<_> = buckets
        .iter()
        .map(|v| match v {
            Datum::Int(b) => *b,
            other => panic!("{other:?}"),
        })
        .collect();
    buckets.sort_unstable();
    assert_eq!(buckets, vec![6, 7, 8]);
}

#[test]
fn projection_truncate_string() {
    let schema = Schema::new(vec![field(1, "value", Type::String, true)]).unwrap();
    let spec = PartitionSpec::parse("truncate(5, value)", &schema).unwrap();
    let v = || Datum::String("abcdefg".into());
    let trunc = || Datum::String("abcde".into());
    let cmp = |op| project(&Predicate::Cmp(1, op, v()), &spec, &schema);
    assert_eq!(cmp(Op::Lt), Predicate::Cmp(0, Op::LtEq, trunc()));
    assert_eq!(cmp(Op::Gt), Predicate::Cmp(0, Op::GtEq, trunc()));
    assert_eq!(cmp(Op::Eq), Predicate::Cmp(0, Op::Eq, trunc()));
    assert_eq!(cmp(Op::NotEq), Predicate::True);
}

fn summary(lower: i32, upper: i32, contains_null: bool) -> FieldSummary {
    FieldSummary {
        contains_null,
        contains_nan: None,
        lower_bound: Some(Datum::Int(lower).to_bytes()),
        upper_bound: Some(Datum::Int(upper).to_bytes()),
    }
}

#[test]
fn manifest_summaries() {
    let s = [summary(10, 20, false)];
    let t = [Type::Int];
    let may = |p: Predicate| manifest_may_match(&p, &s, &t);
    assert!(may(Predicate::Cmp(0, Op::Eq, Datum::Int(10))));
    assert!(!may(Predicate::Cmp(0, Op::Eq, Datum::Int(21))));
    assert!(!may(Predicate::Cmp(0, Op::Lt, Datum::Int(10))));
    assert!(!may(Predicate::Cmp(0, Op::Gt, Datum::Int(20))));
    assert!(!may(Predicate::IsNull(0)));
    assert!(may(Predicate::NotNull(0)));

    let only_nulls = [FieldSummary {
        contains_null: true,
        ..Default::default()
    }];
    assert!(!manifest_may_match(&Predicate::NotNull(0), &only_nulls, &t));
    assert!(manifest_may_match(&Predicate::IsNull(0), &only_nulls, &t));
    assert!(!manifest_may_match(
        &Predicate::Cmp(0, Op::Eq, Datum::Int(1)),
        &only_nulls,
        &t
    ));
}

#[test]
fn partition_tuples() {
    let row = [Some(Datum::Int(5)), None];
    assert!(partition_matches(
        &Predicate::Cmp(0, Op::LtEq, Datum::Int(5)),
        &row
    ));
    assert!(!partition_matches(
        &Predicate::Cmp(0, Op::Lt, Datum::Int(5)),
        &row
    ));
    assert!(!partition_matches(
        &Predicate::Cmp(1, Op::Eq, Datum::Int(5)),
        &row
    ));
    assert!(partition_matches(&Predicate::IsNull(1), &row));
    assert!(!partition_matches(&Predicate::NotNull(1), &row));
}
