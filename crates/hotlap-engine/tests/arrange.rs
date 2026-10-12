use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_engine::{KeyConverter, KeyedArrangement, ZSetBatch};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

/// Builds a Z-set of `(key, value, diff)` triples over the shared schema.
fn zset(rows: &[(i64, &str, i64)]) -> ZSetBatch {
    let keys: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let values: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let diffs: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(StringArray::from(values)),
    ];
    let batch = RecordBatch::try_new(schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn keys() -> KeyConverter {
    KeyConverter::new(schema().as_ref(), &[0]).unwrap()
}

fn arrangement() -> KeyedArrangement {
    KeyedArrangement::new(schema(), &[0]).unwrap()
}

/// Reads the materialized Z-set as sorted `(key, value, diff)` triples.
fn entries(arrangement: &KeyedArrangement) -> Vec<(i64, String, i64)> {
    let zset = arrangement.to_zset().unwrap();
    let keys = zset
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = zset
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let diffs = zset.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out: Vec<(i64, String, i64)> = (0..zset.len())
        .map(|i| (keys.value(i), values.value(i).to_string(), diffs.value(i)))
        .collect();
    out.sort();
    out
}

#[test]
fn incremental_matches_full_recompute() {
    let mut incremental = arrangement();
    incremental
        .apply(&zset(&[(1, "a", 1), (2, "b", 1)]), &keys())
        .unwrap();
    incremental
        .apply(&zset(&[(1, "a", 1), (2, "b", -1), (3, "c", 1)]), &keys())
        .unwrap();

    let mut full = arrangement();
    full.apply(
        &zset(&[
            (1, "a", 1),
            (2, "b", 1),
            (1, "a", 1),
            (2, "b", -1),
            (3, "c", 1),
        ]),
        &keys(),
    )
    .unwrap();

    assert_eq!(entries(&incremental), entries(&full));
    assert_eq!(
        entries(&incremental),
        vec![(1, "a".to_string(), 2), (3, "c".to_string(), 1)]
    );
}

#[test]
fn retraction_removes_row_and_zero_crossing_empties_state() {
    let mut arrangement = arrangement();
    arrangement
        .apply(&zset(&[(1, "a", 1), (1, "b", 1)]), &keys())
        .unwrap();
    assert_eq!(arrangement.len(), 2);

    arrangement.retract(&zset(&[(1, "a", 1)]), &keys()).unwrap();
    assert_eq!(entries(&arrangement), vec![(1, "b".to_string(), 1)]);

    arrangement.retract(&zset(&[(1, "b", 1)]), &keys()).unwrap();
    assert_eq!(arrangement.len(), 0);
    assert!(arrangement.to_zset().unwrap().is_empty());
}

#[test]
fn same_key_with_different_payload_stays_distinct() {
    let mut arrangement = arrangement();
    arrangement
        .apply(&zset(&[(1, "a", 1), (1, "b", 1)]), &keys())
        .unwrap();

    assert_eq!(arrangement.len(), 2);
    assert_eq!(
        entries(&arrangement),
        vec![(1, "a".to_string(), 1), (1, "b".to_string(), 1)]
    );
}

#[test]
fn state_is_deterministic_across_insertion_orders() {
    let mut first = arrangement();
    first
        .apply(&zset(&[(2, "b", 1), (1, "a", 1), (1, "b", -1)]), &keys())
        .unwrap();

    let mut second = arrangement();
    second
        .apply(&zset(&[(1, "b", -1), (2, "b", 1), (1, "a", 1)]), &keys())
        .unwrap();

    assert_eq!(entries(&first), entries(&second));
    let first_iter: Vec<_> = first.iter().collect();
    assert_eq!(first_iter, second.iter().collect::<Vec<_>>());
}

#[test]
fn applied_changelog_matches_consolidated_full_rows() {
    let mut arrangement = arrangement();
    arrangement
        .apply(&zset(&[(1, "a", 1), (1, "a", -1), (2, "b", 1)]), &keys())
        .unwrap();

    // The zero-sum `(1, "a")` pair must not survive in the arrangement.
    assert_eq!(entries(&arrangement), vec![(2, "b".to_string(), 1)]);
    assert_eq!(arrangement.len(), 1);
}

#[test]
fn borrowed_update_input_can_be_dropped_and_key_reinserted_after_zero() {
    let mut state = arrangement();
    {
        let input = zset(&[(7, "first", 3), (7, "second", 2)]);
        state.apply(&input, &keys()).unwrap();
    }
    state.retract(&zset(&[(7, "first", 3)]), &keys()).unwrap();
    assert_eq!(entries(&state), vec![(7, "second".into(), 2)]);
    state.retract(&zset(&[(7, "second", 2)]), &keys()).unwrap();
    assert!(state.is_empty());
    state
        .apply(&zset(&[(7, "reinserted", 1)]), &keys())
        .unwrap();
    assert_eq!(entries(&state), vec![(7, "reinserted".into(), 1)]);
}

#[test]
fn zero_diff_new_key_does_not_create_an_empty_or_zero_entry() {
    let mut state = arrangement();
    state.apply(&zset(&[(9, "payload", 0)]), &keys()).unwrap();
    assert!(state.is_empty());
    assert!(entries(&state).is_empty());
}

#[test]
fn zero_diff_new_payload_does_not_create_an_entry_under_existing_key() {
    let mut state = arrangement();
    state.apply(&zset(&[(9, "retained", 2)]), &keys()).unwrap();
    state.apply(&zset(&[(9, "zero", 0)]), &keys()).unwrap();
    assert_eq!(entries(&state), vec![(9, "retained".into(), 2)]);
}
