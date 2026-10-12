use super::*;
use crate::batch::ZSetBatch;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};

fn arrangement() -> (KeyedArrangement, KeyConverter) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    let converter = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    (arrangement, converter)
}

fn batch(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let keys = Int64Array::from(rows.iter().map(|row| row.0).collect::<Vec<_>>());
    let values = Int64Array::from(rows.iter().map(|row| row.1).collect::<Vec<_>>());
    let diffs = Int64Array::from(rows.iter().map(|row| row.2).collect::<Vec<_>>());
    ZSetBatch::new(
        RecordBatch::try_new(schema, vec![Arc::new(keys), Arc::new(values)]).unwrap(),
        Arc::new(diffs),
    )
    .unwrap()
}

#[test]
fn row_state_overflow_is_typed_and_preserves_existing_and_new_entries() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let converter = converter_for(schema.as_ref(), &[0]).unwrap();
    let key = converter
        .convert_columns(&[Arc::new(Int64Array::from(vec![1]))])
        .unwrap()
        .row(0)
        .owned();
    let payload = converter
        .convert_columns(&[Arc::new(Int64Array::from(vec![2]))])
        .unwrap()
        .row(0)
        .owned();
    let mut state = RowState::default();
    state.add(key.clone(), payload.clone(), i64::MAX).unwrap();
    assert!(matches!(
        state.add(key.clone(), payload.clone(), 1),
        Err(EngineError::NumericOverflow(_))
    ));
    state.add(key.clone(), payload.clone(), -i64::MAX).unwrap();
    assert!(state.is_empty());
    state.add(key.clone(), payload.clone(), i64::MIN).unwrap();
    assert!(matches!(
        state.add(key, payload, -1),
        Err(EngineError::NumericOverflow(_))
    ));
    assert_eq!(state.sorted()[0].2, i64::MIN);
}

#[test]
fn arrangement_overflow_rolls_back_valid_batch_prefix() {
    let (mut arrangement, converter) = arrangement();
    arrangement
        .apply(&batch(&[(1, 10, i64::MAX), (2, 20, i64::MAX)]), &converter)
        .unwrap();
    let before = arrangement.iter().collect::<Vec<_>>();
    let error = arrangement
        .apply(&batch(&[(1, 11, 4), (2, 20, 1)]), &converter)
        .unwrap_err();
    assert!(matches!(error, EngineError::NumericOverflow(_)));
    assert_eq!(arrangement.iter().collect::<Vec<_>>(), before);
}

#[test]
fn signed_diff_overflow_returns_numeric_error_without_mutation() {
    let (mut arrangement, converter) = arrangement();
    arrangement
        .apply(&batch(&[(1, 10, 1)]), &converter)
        .unwrap();
    let before = arrangement.iter().collect::<Vec<_>>();
    let error = arrangement
        .retract(&batch(&[(1, 10, i64::MIN)]), &converter)
        .unwrap_err();
    assert!(matches!(error, EngineError::NumericOverflow(_)));
    assert_eq!(arrangement.iter().collect::<Vec<_>>(), before);
}

#[test]
fn ordered_boundary_deltas_allow_valid_cancellation_without_net_preaggregation() {
    let (mut arrangement, converter) = arrangement();
    arrangement
        .apply(&batch(&[(1, 10, i64::MIN)]), &converter)
        .unwrap();
    arrangement
        .apply(&batch(&[(1, 10, i64::MAX), (1, 10, 1)]), &converter)
        .unwrap();
    assert!(arrangement.is_empty());
}

#[test]
fn retract_overflow_is_checked_per_row_even_when_net_delta_is_zero() {
    let (mut arrangement, converter) = arrangement();
    arrangement
        .apply(&batch(&[(1, 10, 7)]), &converter)
        .unwrap();
    let before = arrangement.iter().collect::<Vec<_>>();
    let error = arrangement
        .retract(
            &batch(&[(1, 10, i64::MIN), (1, 10, i64::MAX), (1, 10, 1)]),
            &converter,
        )
        .unwrap_err();
    assert!(matches!(error, EngineError::NumericOverflow(_)));
    assert_eq!(arrangement.iter().collect::<Vec<_>>(), before);
}

#[test]
fn batch_commit_prunes_zero_identity_and_keeps_other_payloads() {
    let (mut arrangement, converter) = arrangement();
    arrangement
        .apply(&batch(&[(1, 10, 1), (1, 20, 9)]), &converter)
        .unwrap();
    arrangement
        .apply(&batch(&[(1, 10, -1), (1, 10, 2)]), &converter)
        .unwrap();
    let rows = arrangement.iter().collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(_, _, weight)| *weight != 0));
    assert_eq!(rows.iter().map(|(_, _, weight)| *weight).sum::<i64>(), 11);
    assert!(rows.iter().any(|(_, _, weight)| *weight == 2));
}
