//! End-to-end differential tests: the incremental engine's snapshot must equal a
//! full recomputation from the same changelog. Exercised through the public API only.

use std::collections::BTreeMap;

use hotlap::{ChangeBatch, Hotlap, InputId, Plan, Row, Scalar};

fn batch(pairs: &[((i64, i64), i64)]) -> ChangeBatch {
    let mut b = ChangeBatch::default();
    for ((k, v), d) in pairs {
        b.push(Row(vec![Scalar::I64(*k), Scalar::I64(*v)]), *d);
    }
    b
}

fn recompute(stream: &[((i64, i64), i64)]) -> Vec<(i64, i64)> {
    let mut c: BTreeMap<i64, i64> = BTreeMap::new();
    for ((k, _v), d) in stream {
        *c.entry(*k).or_default() += *d;
    }
    c.into_iter().filter(|(_, v)| *v != 0).collect()
}

fn by_key(key: usize) -> Plan {
    Plan::GroupCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![key],
    }
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
    h.register_input("events").unwrap();
    h.create_view("c", by_key(0)).unwrap();

    let mut stream: Vec<((i64, i64), i64)> = Vec::new();
    for i in 0..100i64 {
        let pair = (i % 7, i % 3);
        let diff = if i % 5 == 0 { -1 } else { 1 };
        stream.push((pair, diff));
        let mut b = ChangeBatch::default();
        b.push(Row(vec![Scalar::I64(pair.0), Scalar::I64(pair.1)]), diff);
        h.push("events", &b).unwrap();
    }

    assert_eq!(pairs(&mut h, "c"), recompute(&stream));
    h.shutdown().unwrap();
}

#[test]
fn one_input_feeds_two_views() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("events").unwrap();
    h.create_view("by_key", by_key(0)).unwrap();
    h.create_view("by_value", by_key(1)).unwrap();

    // One shared input; a single push updates both views, each grouping by a
    // different column.
    let b = batch(&[
        ((1, 10), 1),
        ((1, 20), 1),
        ((2, 10), 1),
        ((3, 30), 1),
        ((1, 10), 1),
    ]);
    h.push("events", &b).unwrap();

    assert_eq!(pairs(&mut h, "by_key"), vec![(1, 3), (2, 1), (3, 1)]);
    assert_eq!(pairs(&mut h, "by_value"), vec![(10, 3), (20, 1), (30, 1)]);
    h.shutdown().unwrap();
}

#[test]
fn key_retracted_to_zero_disappears() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("events").unwrap();
    h.create_view("c", by_key(0)).unwrap();

    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(1)]), 1);
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(1)]), -1);
    h.push("events", &b).unwrap();

    let got = h.snapshot("c").unwrap();
    assert!(
        got.is_empty(),
        "key retracted to zero must disappear, got {got:?}"
    );
    h.shutdown().unwrap();
}

#[test]
fn filter_project_group_count_via_api() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("events").unwrap();
    // keep only key>1, project [key], group by key -> [(2,1),(3,1)]
    let plan = Plan::GroupCount {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::Source(InputId(0))),
                pred: hotlap::plan::Predicate::Gt(0, 1),
            }),
            cols: vec![0],
        }),
        key: vec![0],
    };
    h.create_view("v", plan).unwrap();
    let mut b = ChangeBatch::default();
    for (k, v) in [(1i64, 10i64), (1, 20), (2, 30), (3, 30)] {
        b.push(Row(vec![Scalar::I64(k), Scalar::I64(v)]), 1);
    }
    h.push("events", &b).unwrap();
    assert_eq!(pairs(&mut h, "v"), vec![(2, 1), (3, 1)]); // key1 filtered out (key>1)
    h.shutdown().unwrap();
}

type Stream = Vec<((i64, i64), i64)>;
fn join_plan() -> Plan {
    Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![0],
        right_key: vec![0],
    }
}

fn row4(v: [i64; 4]) -> Row {
    Row(v.into_iter().map(Scalar::I64).collect())
}

fn join_rows(h: &mut Hotlap, view: &str) -> Vec<Row> {
    let mut got = h.snapshot(view).unwrap();
    got.sort();
    got
}

fn consolidate(s: &Stream) -> BTreeMap<(i64, i64), i64> {
    let mut m = BTreeMap::new();
    for (row, d) in s {
        *m.entry(*row).or_default() += *d;
    }
    m
}

fn recompute_join(users: &Stream, orders: &Stream) -> Vec<Row> {
    let (nu, no) = (consolidate(users), consolidate(orders));
    let mut out: BTreeMap<Row, i64> = BTreeMap::new();
    for ((id, name), d1) in &nu {
        for ((uid, amt), d2) in &no {
            if id == uid {
                *out.entry(row4([*id, *name, *uid, *amt])).or_default() += d1 * d2;
            }
        }
    }
    out.retain(|_, d| *d != 0);
    out.into_keys().collect()
}

#[test]
fn join_equals_recompute_across_batches() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("users").unwrap();
    h.register_input("orders").unwrap();
    h.create_view("joined", join_plan()).unwrap();
    let (mut users, mut orders): (Stream, Stream) = Default::default();
    // Deterministic: (1,100,1,7) appears, then vanishes when its user retracts.
    users.push(((1, 100), 1));
    orders.push(((1, 7), 1));
    h.push("users", &batch(&[((1, 100), 1)])).unwrap();
    h.push("orders", &batch(&[((1, 7), 1)])).unwrap();
    assert_eq!(join_rows(&mut h, "joined"), vec![row4([1, 100, 1, 7])]);
    users.push(((1, 100), -1));
    h.push("users", &batch(&[((1, 100), -1)])).unwrap();
    assert!(join_rows(&mut h, "joined").is_empty());
    for i in 0..100i64 {
        let row = ((i % 5, i % 3), if i % 3 == 0 { -1 } else { 1 });
        let idx = (i % 2) as usize;
        h.push(["users", "orders"][idx], &batch(&[(row.0, row.1)]))
            .unwrap();
        [&mut users, &mut orders][idx].push(row);
    }
    assert_eq!(join_rows(&mut h, "joined"), recompute_join(&users, &orders));
}
