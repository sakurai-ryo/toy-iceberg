// Ported from apache/iceberg-rust, licensed under the Apache License, Version 2.0.
// http://www.apache.org/licenses/LICENSE-2.0

use toy_iceberg_core::transform::{Transform, hash};
use toy_iceberg_core::types::Type;
use toy_iceberg_core::values::Datum;

const TS: i64 = 1_510_871_468_000_000; // 2017-11-16T22:31:08Z

fn days(y: i32, m: u32, d: u32) -> i32 {
    let date = chrono_date(y, m, d);
    (date - chrono_date(1970, 1, 1)) as i32
}

/// Days from civil, so that this test does not share date code with the implementation.
fn chrono_date(y: i32, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y as i64 - 1 } else { y as i64 };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy
}

#[test]
fn appendix_b_hashes() {
    assert_eq!(hash(&Datum::Int(34)), 2017239379);
    assert_eq!(hash(&Datum::Long(34)), 2017239379);
    assert_eq!(hash(&Datum::Date(days(2017, 11, 16))), -653330422);
    assert_eq!(hash(&Datum::Timestamp(TS)), -2047944441);
    assert_eq!(hash(&Datum::Timestamp(TS + 1)), -1207196810);
    assert_eq!(hash(&Datum::Timestamptz(TS)), -2047944441);
    assert_eq!(hash(&Datum::Timestamptz(TS + 1)), -1207196810);
    assert_eq!(hash(&Datum::String("iceberg".into())), 1210000089);
    assert_eq!(hash(&Datum::Binary(vec![0, 1, 2, 3])), -188683207);
}

#[test]
fn bucket_literals() {
    let bucket = |v: Datum| Transform::Bucket(100).apply(&v).unwrap().unwrap();
    assert_eq!(bucket(Datum::Int(34)), Datum::Int(79));
    assert_eq!(bucket(Datum::Long(34)), Datum::Int(79));
    assert_eq!(bucket(Datum::Date(days(2017, 11, 16))), Datum::Int(26));
    assert_eq!(bucket(Datum::Timestamp(TS)), Datum::Int(7));
    assert_eq!(bucket(Datum::String("iceberg".into())), Datum::Int(89));
    assert_eq!(bucket(Datum::Binary(vec![0, 1, 2, 3])), Datum::Int(41));
}

#[test]
fn truncate_spec_examples() {
    let cases = [
        (10, Datum::Int(1), Datum::Int(0)),
        (10, Datum::Int(-1), Datum::Int(-10)),
        (10, Datum::Long(1), Datum::Long(0)),
        (10, Datum::Long(-1), Datum::Long(-10)),
        (
            3,
            Datum::String("iceberg".into()),
            Datum::String("ice".into()),
        ),
        (
            3,
            Datum::Binary(vec![1, 2, 3, 4, 5]),
            Datum::Binary(vec![1, 2, 3]),
        ),
    ];
    for (w, input, expected) in cases {
        assert_eq!(
            Transform::Truncate(w).apply(&input).unwrap(),
            Some(expected)
        );
    }
}

#[test]
fn truncate_counts_code_points() {
    let v = Datum::String("イルカ🐬x".into());
    let got = Transform::Truncate(4).apply(&v).unwrap();
    assert_eq!(got, Some(Datum::String("イルカ🐬".into())));
}

#[test]
fn temporal_transforms_floor_before_the_epoch() {
    let apply = |t: Transform, v: Datum| t.apply(&v).unwrap().unwrap();
    for (date, year) in [
        ((1970, 1, 1), 0),
        ((2000, 1, 1), 30),
        ((2030, 1, 1), 60),
        ((1969, 1, 1), -1),
    ] {
        let (y, m, d) = date;
        assert_eq!(
            apply(Transform::Year, Datum::Date(days(y, m, d))),
            Datum::Int(year)
        );
    }
    assert_eq!(
        apply(Transform::Month, Datum::Date(days(2017, 12, 1))),
        Datum::Int(575)
    );
    assert_eq!(
        apply(Transform::Month, Datum::Date(days(1969, 12, 31))),
        Datum::Int(-1)
    );
    assert_eq!(apply(Transform::Day, Datum::Timestamp(-1)), Datum::Date(-1));
    assert_eq!(
        apply(Transform::Day, Datum::Timestamp(TS)),
        Datum::Date(days(2017, 11, 16))
    );
    assert_eq!(apply(Transform::Hour, Datum::Timestamp(-1)), Datum::Int(-1));
    assert_eq!(
        apply(Transform::Hour, Datum::Timestamp(3_600_000_000)),
        Datum::Int(1)
    );
}

#[test]
fn void_produces_null() {
    assert_eq!(Transform::Void.apply(&Datum::Int(1)).unwrap(), None);
}

#[test]
fn result_types() {
    assert_eq!(
        Transform::Day.result_type(Type::Timestamp).unwrap(),
        Type::Date
    );
    assert_eq!(Transform::Year.result_type(Type::Date).unwrap(), Type::Int);
    assert_eq!(
        Transform::Bucket(4).result_type(Type::String).unwrap(),
        Type::Int
    );
    assert_eq!(
        Transform::Truncate(4).result_type(Type::String).unwrap(),
        Type::String
    );
    assert!(Transform::Hour.result_type(Type::Date).is_err());
    assert!(Transform::Bucket(4).result_type(Type::Double).is_err());
    assert!(Transform::Truncate(4).result_type(Type::Date).is_err());
}

#[test]
fn transform_strings_round_trip() {
    for s in [
        "identity",
        "bucket[16]",
        "truncate[4]",
        "year",
        "month",
        "day",
        "hour",
        "void",
    ] {
        assert_eq!(s.parse::<Transform>().unwrap().to_string(), s);
    }
    assert!("bucket[0]".parse::<Transform>().is_err());
    assert!("bucket[x]".parse::<Transform>().is_err());
}
