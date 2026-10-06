//! Public facade tests for [`Hotlap`](super::Hotlap).

use super::*;
use crate::harness::{fixture, recompute_group_count};
use crate::row::Scalar;

fn count_by(key: usize) -> Plan {
    Plan::GroupCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![key],
    }
}

#[test]
fn api_end_to_end_group_count() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("events").unwrap();
    h.create_view("counts", count_by(0)).unwrap();
    let mut b = ChangeBatch::default();
    for f in fixture() {
        b.push(Row(vec![Scalar::I64(f.key), Scalar::I64(f.value)]), f.diff);
    }
    h.push("events", &b).unwrap();
    let mut got: Vec<(i64, i64)> = h
        .snapshot("counts")
        .unwrap()
        .into_iter()
        .map(|r| match (&r.0[0], &r.0[1]) {
            (Scalar::I64(k), Scalar::I64(c)) => (*k, *c),
            _ => panic!("shape"),
        })
        .collect();
    got.sort();
    assert_eq!(got, recompute_group_count(&fixture()));
    h.shutdown().unwrap();
}

#[test]
fn unknown_input_and_view_names_error() {
    let mut h = Hotlap::open().unwrap();
    let batch = ChangeBatch::default();
    assert!(matches!(h.push("nope", &batch), Err(HotlapError(_))));
    assert!(matches!(h.snapshot("nope"), Err(HotlapError(_))));
}

#[test]
fn schema_freezes_after_first_push() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("in").unwrap();
    h.create_view("v", count_by(0)).unwrap();
    h.push("in", &ChangeBatch::default()).unwrap();

    assert!(h.register_input("late").is_err());
    assert!(h.create_view("late", count_by(0)).is_err());
}

#[test]
fn duplicate_view_name_rejected_and_original_survives() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("in").unwrap();
    h.create_view("v", count_by(0)).unwrap();
    assert!(matches!(
        h.create_view("v", count_by(0)),
        Err(HotlapError(_))
    ));

    // The first view is still operable.
    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(7), Scalar::I64(1)]), 1);
    h.push("in", &b).unwrap();
    assert_eq!(
        h.snapshot("v").unwrap(),
        vec![Row(vec![Scalar::I64(7), Scalar::I64(1)])]
    );
}

#[test]
fn declare_watermark_facade_validates() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("in").unwrap();
    h.create_view("v", count_by(0)).unwrap();

    h.declare_watermark("in", 0, 2).unwrap();
    assert!(h.declare_watermark("nope", 0, 0).is_err());

    // Declaring after the first push is rejected: the schema is frozen.
    h.push("in", &ChangeBatch::default()).unwrap();
    assert!(h.declare_watermark("in", 0, 0).is_err());
}
