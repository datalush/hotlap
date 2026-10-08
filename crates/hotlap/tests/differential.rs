//! End-to-end differential tests: the incremental engine's snapshot must equal a
//! full recomputation from the same changelog. Exercised through the public API only.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap::{AggSpec, CmpOp, Hotlap, InputId, Plan, Predicate, Scalar, ZSetBatch};
use hotlap_engine::EngineCore;

/// Build a Z-set whose columns are all Int64 and whose diffs are signed.
fn zset(columns: &[Vec<i64>], diffs: &[i64]) -> ZSetBatch {
    let fields: Vec<Field> = columns
        .iter()
        .enumerate()
        .map(|(i, _)| Field::new(format!("c{i}"), DataType::Int64, true))
        .collect();
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|c| Arc::new(Int64Array::from(c.clone())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

/// Read a consolidated Z-set of Int64 columns as plain integer rows.
fn rows(z: &ZSetBatch) -> Vec<Vec<i64>> {
    let columns: Vec<&Int64Array> = z
        .batch
        .columns()
        .iter()
        .map(|c| c.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    (0..z.len())
        .map(|row| columns.iter().map(|c| c.value(row)).collect())
        .collect()
}

fn open() -> Hotlap {
    Hotlap::open_with(Box::new(EngineCore::new()))
}

fn batch(pairs: &[((i64, i64), i64)]) -> ZSetBatch {
    let keys: Vec<i64> = pairs.iter().map(|((k, _), _)| *k).collect();
    let values: Vec<i64> = pairs.iter().map(|((_, v), _)| *v).collect();
    let diffs: Vec<i64> = pairs.iter().map(|(_, d)| *d).collect();
    zset(&[keys, values], &diffs)
}

fn recompute(stream: &[((i64, i64), i64)]) -> Vec<(i64, i64)> {
    let mut c: BTreeMap<i64, i64> = BTreeMap::new();
    for ((k, _v), d) in stream {
        *c.entry(*k).or_default() += *d;
    }
    c.into_iter().filter(|(_, v)| *v != 0).collect()
}

fn by_key(key: usize) -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![key],
        aggs: vec![AggSpec::count()],
    }
}

/// Snapshot `view` as sorted `(key, count)` pairs.
fn pairs(h: &mut Hotlap, view: &str) -> Vec<(i64, i64)> {
    let mut got: Vec<(i64, i64)> = rows(&h.snapshot(view).unwrap())
        .into_iter()
        .map(|r| (r[0], r[1]))
        .collect();
    got.sort();
    got
}

#[test]
fn incremental_equals_recompute_across_many_batches() {
    let mut h = open();
    h.register_input("events").unwrap();
    h.create_view("c", by_key(0)).unwrap();

    let mut stream: Vec<((i64, i64), i64)> = Vec::new();
    for i in 0..100i64 {
        let pair = (i % 7, i % 3);
        let diff = if i % 5 == 0 { -1 } else { 1 };
        stream.push((pair, diff));
        h.push("events", &batch(&[(pair, diff)])).unwrap();
    }

    assert_eq!(pairs(&mut h, "c"), recompute(&stream));
    h.shutdown().unwrap();
}

#[test]
fn one_input_feeds_two_views() {
    let mut h = open();
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
    let mut h = open();
    h.register_input("events").unwrap();
    h.create_view("c", by_key(0)).unwrap();

    let b = zset(&[vec![1, 1], vec![1, 1]], &[1, -1]);
    h.push("events", &b).unwrap();

    let got = rows(&h.snapshot("c").unwrap());
    assert!(
        got.is_empty(),
        "key retracted to zero must disappear, got {got:?}"
    );
    h.shutdown().unwrap();
}

#[test]
fn filter_project_group_count_via_api() {
    let mut h = open();
    h.register_input("events").unwrap();
    // keep only key>1, project [key], group by key -> [(2,1),(3,1)]
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::Source(InputId(0))),
                pred: Predicate::Cmp {
                    op: CmpOp::Gt,
                    col: 0,
                    scalar: Scalar::I64(1),
                },
            }),
            cols: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    h.create_view("v", plan).unwrap();
    h.push(
        "events",
        &zset(&[vec![1, 1, 2, 3], vec![10, 20, 30, 30]], &[1, 1, 1, 1]),
    )
    .unwrap();
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

fn join_rows(h: &mut Hotlap, view: &str) -> Vec<Vec<i64>> {
    let mut got = rows(&h.snapshot(view).unwrap());
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

fn recompute_join(users: &Stream, orders: &Stream) -> Vec<Vec<i64>> {
    let (nu, no) = (consolidate(users), consolidate(orders));
    let mut out: BTreeMap<Vec<i64>, i64> = BTreeMap::new();
    for ((id, name), d1) in &nu {
        for ((uid, amt), d2) in &no {
            if id == uid {
                *out.entry(vec![*id, *name, *uid, *amt]).or_default() += d1 * d2;
            }
        }
    }
    out.retain(|_, d| *d != 0);
    out.into_keys().collect()
}

#[test]
fn join_equals_recompute_across_batches() {
    let mut h = open();
    h.register_input("users").unwrap();
    h.register_input("orders").unwrap();
    h.create_view("joined", join_plan()).unwrap();
    let (mut users, mut orders): (Stream, Stream) = Default::default();
    // Deterministic: (1,100,1,7) appears, then vanishes when its user retracts.
    users.push(((1, 100), 1));
    orders.push(((1, 7), 1));
    h.push("users", &batch(&[((1, 100), 1)])).unwrap();
    h.push("orders", &batch(&[((1, 7), 1)])).unwrap();
    assert_eq!(join_rows(&mut h, "joined"), vec![vec![1, 100, 1, 7]]);
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
