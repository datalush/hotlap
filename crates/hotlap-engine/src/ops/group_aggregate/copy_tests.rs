//! Deterministic evidence that a delta never copies a whole `min`/`max`
//! multiset: one update on a hot key costs independently of its cardinality.
//!
//! The counter counts multiset entries copied by `Clone`, so it is exact and
//! independent of wall-clock timing; a full `GroupEntry` clone (the shape this
//! guards against) would move every live value.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::plan::AggSpec;
use hotlap_core::snapshot::{copied_entries, reset_copied_entries};

use super::GroupAggregate;
use crate::batch::ZSetBatch;

/// One `(key, value, diff)` row over `Int64` columns.
fn zset(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let keys: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let values: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let diffs: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(Int64Array::from(values)),
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

/// A reducer over an `Int64` key computing `min`/`max` (multiset-backed).
fn reducer() -> GroupAggregate {
    GroupAggregate::new(&[0], vec![AggSpec::min(1), AggSpec::max(1)])
}

/// Seeds one hot key with `cardinality` distinct values `0..cardinality`.
fn seed_hot_key(reducer: &mut GroupAggregate, cardinality: i64) {
    let rows: Vec<(i64, i64, i64)> = (0..cardinality).map(|value| (1, value, 1)).collect();
    reducer.apply(&zset(&rows)).unwrap();
}

#[test]
fn a_non_changing_delta_does_not_copy_the_whole_multiset() {
    for cardinality in [10i64, 100, 1000, 10000] {
        let mut reducer = reducer();
        seed_hot_key(&mut reducer, cardinality);

        reset_copied_entries();
        // Retract a value that is neither the min nor the max: no output change.
        let out = reducer.apply(&zset(&[(1, cardinality / 2, -1)])).unwrap();
        assert!(
            out.is_empty(),
            "cardinality {cardinality}: no churn expected"
        );
        assert_eq!(
            copied_entries(),
            0,
            "cardinality {cardinality}: a delta must not copy the multiset"
        );
    }
}

#[test]
fn a_changing_delta_does_not_copy_the_whole_multiset() {
    for cardinality in [10i64, 100, 1000, 10000] {
        let mut reducer = reducer();
        seed_hot_key(&mut reducer, cardinality);

        reset_copied_entries();
        // Retract the current minimum: the output changes, but only the
        // rendered cells move, never the whole multiset.
        let out = reducer.apply(&zset(&[(1, 0, -1)])).unwrap();
        assert!(!out.is_empty(), "cardinality {cardinality}: output changed");
        assert_eq!(
            copied_entries(),
            0,
            "cardinality {cardinality}: changing a delta must not copy the multiset"
        );
    }
}
