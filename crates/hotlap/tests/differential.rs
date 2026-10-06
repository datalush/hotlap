//! End-to-end differential tests: the incremental engine's snapshot must equal a
//! full recomputation from the same changelog. Exercised through the public API only.

use hotlap::{ChangeBatch, Hotlap, Plan, Row, Scalar};

fn batch(pairs: &[(i64, i64)]) -> ChangeBatch {
    let mut b = ChangeBatch::default();
    for (k, v) in pairs {
        b.push(Row(vec![Scalar::I64(*k), Scalar::I64(*v)]), 1);
    }
    b
}

fn recompute(stream: &[((i64, i64), i64)]) -> Vec<(i64, i64)> {
    use std::collections::BTreeMap;
    let mut c: BTreeMap<i64, i64> = BTreeMap::new();
    for ((k, _v), d) in stream {
        *c.entry(*k).or_default() += *d;
    }
    c.into_iter().filter(|(_, v)| *v != 0).collect()
}

/// Snapshot `view` as sorted `(key, count)` pairs, asserting the `[key, count]` shape.
fn pairs(h: &mut Hotlap, view: &str) -> Vec<(i64, i64)> {
    let mut got: Vec<(i64, i64)> = h
        .snapshot(view)
        .unwrap()
        .into_iter()
        .map(|r| match (&r.0[0], &r.0[1]) {
            (Scalar::I64(k), Scalar::I64(c)) => (*k, *c),
            _ => panic!("shape"),
        })
        .collect();
    got.sort();
    got
}

#[test]
fn incremental_equals_recompute_across_many_batches() {
    let mut h = Hotlap::open().unwrap();
    h.create_view(
        "c",
        Plan::GroupCount {
            input: Box::new(Plan::Scan),
            key: vec![0],
        },
    )
    .unwrap();

    let mut stream: Vec<((i64, i64), i64)> = Vec::new();
    for i in 0..100i64 {
        let pair = (i % 7, i % 3);
        let diff = if i % 5 == 0 { -1 } else { 1 };
        stream.push((pair, diff));
        let mut b = ChangeBatch::default();
        b.push(Row(vec![Scalar::I64(pair.0), Scalar::I64(pair.1)]), diff);
        h.push("c", &b).unwrap();
    }

    assert_eq!(pairs(&mut h, "c"), recompute(&stream));
    h.shutdown().unwrap();
}

#[test]
fn independent_views_group_by_different_columns() {
    let mut h = Hotlap::open().unwrap();
    let count_by = |key: usize| Plan::GroupCount {
        input: Box::new(Plan::Scan),
        key: vec![key],
    };
    h.create_view("by_key", count_by(0)).unwrap();
    h.create_view("by_value", count_by(1)).unwrap();

    // Same changelog fed to both views; each groups by a different column.
    let b = batch(&[(1, 10), (1, 20), (2, 10), (3, 30), (1, 10)]);
    h.push("by_key", &b).unwrap();
    h.push("by_value", &b).unwrap();

    assert_eq!(pairs(&mut h, "by_key"), vec![(1, 3), (2, 1), (3, 1)]);
    assert_eq!(pairs(&mut h, "by_value"), vec![(10, 3), (20, 1), (30, 1)]);
    h.shutdown().unwrap();
}

#[test]
fn key_retracted_to_zero_disappears() {
    let mut h = Hotlap::open().unwrap();
    h.create_view(
        "c",
        Plan::GroupCount {
            input: Box::new(Plan::Scan),
            key: vec![0],
        },
    )
    .unwrap();

    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(1)]), 1);
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(1)]), -1);
    h.push("c", &b).unwrap();

    let got = h.snapshot("c").unwrap();
    assert!(got.is_empty(), "key retracted to zero must disappear, got {got:?}");
    h.shutdown().unwrap();
}
