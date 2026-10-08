//! Unit tests for the delta-incremental key-scoped join.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::Join;
use crate::batch::ZSetBatch;
use crate::error::EngineError;

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

    // One more left row for key 0 pairs only the new row against key 0's one
    // retained right row (1 pair), even though 64 keys are resident.
    join.apply(&zset(&[(0, "l2", 1)]), &zset(&[])).unwrap();
    assert_eq!(join.work(), 1);
}

#[test]
fn hot_key_delta_cost_is_linear_not_quadratic() {
    const N: usize = 64;
    // A single hot key with N rows per side: N*N resident joined pairs.
    let seed = |prefix: &str| -> Vec<(i64, String, i64)> {
        (0..N)
            .map(|index| (0, format!("{prefix}{index}"), 1))
            .collect()
    };
    let (seed_left, seed_right) = (seed("l"), seed("r"));

    let mut join = Join::new(&[0], &[0]);
    join.apply(&zset(&row_refs(&seed_left)), &zset(&row_refs(&seed_right)))
        .unwrap();

    // A one-row left delta must cost ~N pairs (dL x R_prev), not the full N*N
    // recompute: this assertion fails under a quadratic per-key recompute.
    join.apply(&zset(&[(0, "l-new", 1)]), &zset(&[])).unwrap();
    assert!(
        join.work() <= 2 * N as u64,
        "hot-key delta evaluated {} pairs, expected ~{N}",
        join.work()
    );
}

/// Borrows owned `(key, value, diff)` rows as the `&str` form `zset` takes.
fn row_refs(rows: &[(i64, String, i64)]) -> Vec<(i64, &str, i64)> {
    rows.iter().map(|row| (row.0, row.1.as_str(), row.2)).collect()
}

#[test]
fn retraction_touches_one_key_and_work_stays_bounded() {
    let mut join = Join::new(&[0], &[0]);
    for key in 0..32i64 {
        join.apply(&zset(&[(key, "l", 1)]), &zset(&[(key, "r", 1)]))
            .unwrap();
    }
    // Give key 3 a second right row: only key 3's retained left row pairs with
    // the new right row (1 pair), not all 32 keys.
    join.apply(&zset(&[]), &zset(&[(3, "r2", 1)])).unwrap();
    assert_eq!(join.work(), 1);

    // Only key 3 is re-evaluated (1 left x 1 remaining right), not all 32.
    let out = join.apply(&zset(&[]), &zset(&[(3, "r2", -1)])).unwrap();
    assert_eq!(join.work(), 1);
    assert_eq!(out.len(), 1);
}

#[test]
fn join_rejects_diff_product_overflow() {
    // i64::MAX * 2 cannot be represented; the join must surface an error
    // instead of wrapping or panicking.
    let mut join = Join::new(&[0], &[0]);
    let result = join.apply(&zset(&[(1, "l", i64::MAX)]), &zset(&[(1, "r", 2)]));
    assert!(matches!(result, Err(EngineError::Infrastructure(_))));
}

/// Builds a single-column `k` Z-set with the given key element type.
fn keyed(key_type: DataType, columns: ArrayRef, diffs: Vec<i64>) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("k", key_type, false)]));
    let batch = RecordBatch::try_new(schema, vec![columns]).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

#[test]
fn join_rejects_mismatched_key_types() {
    // Int64 and Int32 keys encode to different `arrow::row` bytes, so the sides
    // must not be joined positionally without a type match.
    let mut join = Join::new(&[0], &[0]);
    let left = keyed(
        DataType::Int64,
        Arc::new(Int64Array::from(vec![1i64])),
        vec![1],
    );
    let right = keyed(
        DataType::Int32,
        Arc::new(Int32Array::from(vec![1i32])),
        vec![1],
    );
    let result = join.apply(&left, &right);
    assert!(matches!(result, Err(EngineError::Unsupported(_))));
}

#[test]
fn join_rejects_schema_change_after_first_apply() {
    let mut join = Join::new(&[0], &[0]);
    join.apply(&zset(&[(1, "a", 1)]), &zset(&[(1, "x", 1)]))
        .unwrap();
    // The cached converters are only valid for the schema learned first; a
    // different left schema must be rejected rather than silently reused.
    let changed = keyed(
        DataType::Int64,
        Arc::new(Int64Array::from(vec![1i64])),
        vec![1],
    );
    let result = join.apply(&changed, &zset(&[(1, "x", 1)]));
    assert!(matches!(result, Err(EngineError::Unsupported(_))));
}
