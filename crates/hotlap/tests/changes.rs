use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap::{AggSpec, Hotlap, InputId, Plan, ZSetBatch};
use hotlap_engine::EngineCore;

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

fn diffs(z: &ZSetBatch) -> Vec<i64> {
    let array = z
        .diff
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 diff");
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn open() -> Hotlap {
    Hotlap::open_with(Box::new(EngineCore::new()))
}

fn count_by_key() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    }
}

#[test]
fn changelog_reconstructs_snapshot() {
    let mut h = open();
    h.register_input("in").unwrap();
    h.create_view("c", count_by_key()).unwrap();
    h.tap_view("c").unwrap();
    let mut acc: BTreeMap<Vec<i64>, i64> = Default::default();
    for vals in [vec![1i64, 1, 2], vec![2, 3]] {
        let ones = vec![1i64; vals.len()];
        h.push("in", &zset(&[vals], &ones)).unwrap();
        let changes = h.take_changes("c").unwrap();
        for (row, diff) in rows(&changes).into_iter().zip(diffs(&changes)) {
            *acc.entry(row).or_insert(0) += diff;
        }
    }
    let snap: BTreeSet<Vec<i64>> = rows(&h.snapshot("c").unwrap()).into_iter().collect();
    let from_changes: BTreeSet<Vec<i64>> = acc
        .into_iter()
        .filter(|(_, d)| *d != 0)
        .map(|(r, _)| r)
        .collect();
    assert_eq!(snap, from_changes);
}

#[test]
fn tap_after_first_push_rejected() {
    let mut h = open();
    h.register_input("in").unwrap();
    h.create_view("c", count_by_key()).unwrap();
    h.push("in", &zset(&[vec![1]], &[1])).unwrap(); // first push -> freeze
    assert!(h.tap_view("c").is_err()); // pre-freeze only
}
