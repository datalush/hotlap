//! `min`/`max` output-type preservation and snapshot round-trip tests.

use arrow::datatypes::DataType;
use hotlap_core::plan::AggSpec;

use super::{
    GroupAggregate, float_rows, float_values, int_rows, int32_values, int64_values, reducer, zset,
};

#[test]
fn float_min_max_preserve_the_input_type() {
    let mut reducer = reducer();
    let out = reducer
        .apply(&zset(
            &[1, 1],
            float_values(&[Some(1.5), Some(-2.0)]),
            &[1, 1],
        ))
        .unwrap();
    assert_eq!(out.schema().field(1).data_type(), &DataType::Float64);
    assert_eq!(out.schema().field(2).data_type(), &DataType::Float64);
    assert_eq!(float_rows(&out), vec![(1, Some(-2.0), Some(1.5), 1)]);
}

#[test]
fn int32_min_preserves_the_input_type() {
    let mut reducer = GroupAggregate::new(&[0], vec![AggSpec::min(1)]);
    let out = reducer
        .apply(&zset(&[1], int32_values(&[Some(7)]), &[1]))
        .unwrap();
    assert_eq!(out.schema().field(1).data_type(), &DataType::Int32);
}

#[test]
fn snapshot_round_trips_the_multiset() {
    let mut source = reducer();
    source
        .apply(&zset(
            &[1, 1, 1],
            int64_values(&[Some(10), Some(20), Some(30)]),
            &[1, 1, 1],
        ))
        .unwrap();
    let state = source.export_state().unwrap();
    let bytes = crate::encode_framed(&state).unwrap();
    let decoded: hotlap_core::snapshot::GroupState = crate::decode_framed(&bytes).unwrap();

    let mut restored = reducer();
    restored.import_state(&decoded).unwrap();
    let out = restored
        .apply(&zset(&[1], int64_values(&[Some(10)]), &[-1]))
        .unwrap();
    assert_eq!(
        int_rows(&out),
        vec![(1, Some(10), Some(30), -1), (1, Some(20), Some(30), 1)]
    );
}
