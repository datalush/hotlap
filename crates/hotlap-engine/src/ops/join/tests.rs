//! Unit tests for the delta-incremental key-scoped join.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::Join;
use crate::batch::ZSetBatch;

/// Builds a Z-set of `(key, value, diff)` rows with one schema for both sides.
fn zset(rows: &[(i64, &str, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

#[test]
fn work_is_scoped_to_touched_key_not_keyspace() {
    let mut join = Join::new(&[0], &[0]);
    // Seed many keys; each push touches exactly one key with a 1x1 pair.
    for key in 0..64i64 {
        join.apply(&zset(&[(key, "l", 1)]), &zset(&[(key, "r", 1)]))
            .unwrap();
        assert_eq!(join.work(), 1);
    }

    // One more left row for key 0 re-evaluates only key 0's 2x1 pairs, even
    // though 64 keys are resident: the count stays independent of keyspace.
    join.apply(&zset(&[(0, "l2", 1)]), &zset(&[])).unwrap();
    assert_eq!(join.work(), 2);
}

#[test]
fn retraction_touches_one_key_and_work_stays_bounded() {
    let mut join = Join::new(&[0], &[0]);
    for key in 0..32i64 {
        join.apply(&zset(&[(key, "l", 1)]), &zset(&[(key, "r", 1)]))
            .unwrap();
    }
    // Give key 3 a second right row, then retract it again.
    join.apply(&zset(&[]), &zset(&[(3, "r2", 1)])).unwrap();
    assert_eq!(join.work(), 2);

    // Only key 3 is re-evaluated (1 left x 1 remaining right), not all 32.
    let out = join.apply(&zset(&[]), &zset(&[(3, "r2", -1)])).unwrap();
    assert_eq!(join.work(), 1);
    assert_eq!(out.len(), 1);
}
