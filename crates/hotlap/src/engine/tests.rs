//! Public facade tests for [`Hotlap`](super::Hotlap).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap_core::{AggSpec, CmpOp, EngineSnapshot, Plan, Predicate, Scalar, ZSetBatch};
use hotlap_engine::EngineCore;

use super::*;
use crate::harness::{fixture, recompute_group_count};

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

fn count_by(key: usize) -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![key],
        aggs: vec![AggSpec::count()],
    }
}

#[test]
fn api_end_to_end_group_count() {
    let mut h = open();
    h.register_input("events").unwrap();
    h.create_view("counts", count_by(0)).unwrap();
    let data = fixture();
    let keys: Vec<i64> = data.iter().map(|f| f.key).collect();
    let values: Vec<i64> = data.iter().map(|f| f.value).collect();
    let diffs: Vec<i64> = data.iter().map(|f| f.diff).collect();
    h.push("events", &zset(&[keys, values], &diffs)).unwrap();

    let mut got: Vec<(i64, i64)> = rows(&h.snapshot("counts").unwrap())
        .into_iter()
        .map(|r| (r[0], r[1]))
        .collect();
    got.sort();
    assert_eq!(got, recompute_group_count(&data));
    h.shutdown().unwrap();
}

#[test]
fn unknown_input_and_view_names_error() {
    let mut h = open();
    let batch = zset(&[Vec::new(), Vec::new()], &[]);
    assert!(matches!(h.push("nope", &batch), Err(HotlapError(_))));
    assert!(matches!(h.snapshot("nope"), Err(HotlapError(_))));
}

#[test]
fn schema_freezes_after_first_push() {
    let mut h = open();
    h.register_input("in").unwrap();
    h.create_view("v", count_by(0)).unwrap();
    h.push("in", &zset(&[Vec::new(), Vec::new()], &[])).unwrap();

    assert!(h.register_input("late").is_err());
    assert!(h.create_view("late", count_by(0)).is_err());
}

#[test]
fn duplicate_view_name_rejected_and_original_survives() {
    let mut h = open();
    h.register_input("in").unwrap();
    h.create_view("v", count_by(0)).unwrap();
    assert!(matches!(
        h.create_view("v", count_by(0)),
        Err(HotlapError(_))
    ));

    // The first view is still operable.
    h.push("in", &zset(&[vec![7], vec![1]], &[1])).unwrap();
    assert_eq!(rows(&h.snapshot("v").unwrap()), vec![vec![7, 1]]);
}

#[test]
fn declare_watermark_facade_validates() {
    let mut h = open();
    h.register_input("in").unwrap();
    h.create_view("v", count_by(0)).unwrap();

    h.declare_watermark("in", 0, 2).unwrap();
    assert!(h.declare_watermark("nope", 0, 0).is_err());

    // Declaring after the first push is rejected: the schema is frozen.
    h.push("in", &zset(&[Vec::new(), Vec::new()], &[])).unwrap();
    assert!(h.declare_watermark("in", 0, 0).is_err());
}

/// A filter plan selecting the rows whose first column equals `value`.
fn filter(value: i64) -> Plan {
    Plan::Filter {
        input: Box::new(Plan::Source(InputId(0))),
        pred: Predicate::Cmp {
            op: CmpOp::Eq,
            col: 0,
            scalar: Scalar::I64(value),
        },
    }
}

/// Capture a two-view snapshot whose views hold distinguishable output.
fn two_view_snapshot() -> EngineSnapshot {
    let mut h = open();
    h.register_input("in").unwrap();
    h.create_view("a", filter(1)).unwrap();
    h.create_view("b", filter(2)).unwrap();
    h.push("in", &zset(&[vec![1, 2], vec![10, 20]], &[1, 1]))
        .unwrap();
    h.checkpoint().unwrap()
}

#[test]
fn restore_rejects_a_reordered_view_declaration() {
    let snapshot = two_view_snapshot();
    let mut h = open();
    h.register_input("in").unwrap();
    h.create_view("b", filter(2)).unwrap();
    h.create_view("a", filter(1)).unwrap();
    assert!(
        h.restore(&snapshot).is_err(),
        "two same-schema views declared in reverse must not rebind silently"
    );
}

#[test]
fn restore_rejects_a_changed_view_plan() {
    let snapshot = two_view_snapshot();
    let mut h = open();
    h.register_input("in").unwrap();
    h.create_view("a", filter(7)).unwrap();
    h.create_view("b", filter(2)).unwrap();
    assert!(
        h.restore(&snapshot).is_err(),
        "the same name with a changed plan must not bind the saved state"
    );
}

#[test]
fn restore_binds_each_name_to_its_own_plan() {
    let snapshot = two_view_snapshot();
    let mut h = open();
    h.register_input("in").unwrap();
    h.create_view("a", filter(1)).unwrap();
    h.create_view("b", filter(2)).unwrap();
    h.restore(&snapshot).unwrap();
    assert_eq!(rows(&h.snapshot("a").unwrap()), vec![vec![1, 10]]);
    assert_eq!(rows(&h.snapshot("b").unwrap()), vec![vec![2, 20]]);
}
