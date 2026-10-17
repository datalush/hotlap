//! Unit tests for the incremental grouped-aggregate reducer.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Int8Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Int8Type, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::plan::AggSpec;

use super::GroupAggregate;
use crate::batch::ZSetBatch;

fn zset(column: &str, values: &[(i64, i64)], diffs: &[i64]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new(column, DataType::Int64, true),
    ]));
    let keys: Vec<i64> = values.iter().map(|row| row.0).collect();
    let vals: Vec<i64> = values.iter().map(|row| row.1).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(Int64Array::from(vals)),
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

/// `(key, count, sum, avg, diff)` rows sorted for comparison.
fn rows(z: &ZSetBatch) -> Vec<(i64, i64, i64, f64, i64)> {
    let key = int_column(z, 0);
    let count = int_column(z, 1);
    let sum = int_column(z, 2);
    let avg = z
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out: Vec<(i64, i64, i64, f64, i64)> = (0..z.len())
        .map(|i| {
            (
                key.value(i),
                count.value(i),
                sum.value(i),
                avg.value(i),
                diffs.value(i),
            )
        })
        .collect();
    out.sort_by(|a, b| a.partial_cmp(b).unwrap());
    out
}

fn int_column(z: &ZSetBatch, index: usize) -> Int64Array {
    z.batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone()
}

fn reducer() -> GroupAggregate {
    GroupAggregate::new(
        &[0],
        vec![AggSpec::count(), AggSpec::sum(1), AggSpec::avg(1)],
    )
}

fn dictionary_zset(start: i64, count: usize, value: i64) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "k",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("v", DataType::Int64, true),
    ]));
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from((0..count).map(|i| Some(i as i8)).collect::<Vec<_>>()),
        Arc::new(StringArray::from(
            (start..start + count as i64)
                .map(|v| Some(format!("k{v}")))
                .collect::<Vec<_>>(),
        )),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(dictionary) as ArrayRef,
            Arc::new(Int64Array::from(vec![value; count])),
        ],
    )
    .unwrap();
    ZSetBatch::new(
        batch,
        Arc::new(Int64Array::from(vec![1; count])) as ArrayRef,
    )
    .unwrap()
}

fn dictionary_retract_pair() -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "k",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("v", DataType::Int64, true),
    ]));
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0_i8), Some(0_i8)]),
        Arc::new(StringArray::from(vec![Some("k0")])),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(dictionary) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![-1, -1])) as ArrayRef).unwrap()
}

fn dictionary_composite_zset(rows: usize, diffs: Vec<i64>) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let dict_values = Arc::new(StringArray::from(
        (0..128).map(|i| Some(format!("d{i}"))).collect::<Vec<_>>(),
    ));
    let keys = Int8Array::from((0..rows).map(|i| Some((i % 128) as i8)).collect::<Vec<_>>());
    let dictionary = DictionaryArray::<Int8Type>::try_new(keys, dict_values).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(dictionary) as ArrayRef,
            Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(vec![1; rows])),
        ],
    )
    .unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs)) as ArrayRef).unwrap()
}

fn nullable_dictionary_zset(keys: Vec<Option<i8>>, values: Vec<i64>) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("v", DataType::Int64, true),
    ]));
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(keys),
        Arc::new(StringArray::from(vec![Some("")])),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(dictionary) as ArrayRef,
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap();
    let len = batch.num_rows();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1; len])) as ArrayRef).unwrap()
}

#[test]
fn typed_dictionary_group_state_roundtrips_over_int8_capacity_and_continues() {
    let mut source = GroupAggregate::new(&[0], vec![AggSpec::count(), AggSpec::sum(1)]);
    for part in 0..3 {
        source.apply(&dictionary_zset(part * 50, 50, 1)).unwrap();
    }
    assert_eq!(source.groups.len(), 150);
    assert_eq!(source.native_keys.as_ref().unwrap().len(), 150);
    let state = source.export_state().unwrap();
    let hotlap_core::snapshot::TypedKeyPayload::Ipc(key_bytes) = &state.typed_keys else {
        panic!("initialized group state must have typed keys");
    };
    let key_schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )]));
    let (batches, rows) = crate::core::ipc::typed_keys::decode(key_bytes, &key_schema).unwrap();
    assert_eq!(rows, 150);
    assert_eq!(batches.len(), 150);
    let mut restored = GroupAggregate::new(&[0], vec![AggSpec::count(), AggSpec::sum(1)]);
    restored.import_state(&state).unwrap();
    assert_eq!(restored.groups.len(), 150);
    restored.apply(&dictionary_zset(0, 1, 2)).unwrap();
    let entry = restored
        .groups
        .values()
        .find(|entry| entry.rows == 2)
        .unwrap();
    assert_eq!(entry.values[0], hotlap_core::snapshot::AggValue::Count(2));
    assert_eq!(
        entry.values[1],
        hotlap_core::snapshot::AggValue::SumInteger { sum: 3, count: 2 }
    );
    restored.apply(&dictionary_retract_pair()).unwrap();
    assert_eq!(restored.groups.len(), 149);
    assert_eq!(restored.native_keys.as_ref().unwrap().len(), 149);
}

#[test]
fn dictionary_composite_output_accepts_multiple_keys_than_dictionary_capacity() {
    let input = dictionary_composite_zset(256, vec![1; 256]);
    let baseline_converter = crate::keys::converter_for(&input.schema(), &[0, 1]).unwrap();
    let baseline_key_columns = vec![input.batch.column(0).clone(), input.batch.column(1).clone()];
    let baseline_rows = baseline_converter
        .convert_columns(&baseline_key_columns)
        .unwrap();
    let parser = baseline_converter.parser();
    let encoded = (0..baseline_rows.num_rows())
        .map(|row| baseline_rows.row(row).as_ref().to_vec())
        .collect::<Vec<_>>();
    let parsed = encoded
        .iter()
        .map(|bytes| parser.parse(bytes))
        .collect::<Vec<_>>();
    let baseline = baseline_converter.convert_rows(parsed).unwrap();
    assert_eq!(baseline[0].data_type(), &DataType::Utf8);
    assert_ne!(baseline[0].data_type(), input.schema().field(0).data_type());
    let mut group = GroupAggregate::new(&[0, 1], vec![AggSpec::count()]);
    let result = group.apply(&input);
    assert!(result.is_err());
    assert!(
        group.groups.is_empty(),
        "failed output must not publish groups"
    );
    assert!(group.native_keys.as_ref().unwrap().is_empty());
    let continuation = dictionary_composite_zset(128, vec![1; 128]);
    assert_eq!(group.apply(&continuation).unwrap().len(), 128);
    assert_eq!(group.groups.len(), 128);
}

#[test]
fn admitted_dictionary_old_new_output_deduplicates_native_dictionary_values() {
    let mut group = GroupAggregate::new(&[0], vec![AggSpec::count()]);
    group.apply(&dictionary_zset(0, 65, 1)).unwrap();
    let changed = group.apply(&dictionary_zset(0, 65, 1)).unwrap();
    assert_eq!(changed.len(), 130);
    assert_eq!(group.groups.len(), 65);
    assert_eq!(group.native_keys.as_ref().unwrap().len(), 65);
}

#[test]
fn dictionary_list_key_is_rejected_by_source_row_converter_before_group_apply() {
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let key_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::List(item)));
    let schema = Arc::new(Schema::new(vec![Field::new("k", key_type, true)]));
    let error = crate::keys::converter_for(&schema, &[0]).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Row format support not yet implemented")
    );
}

#[test]
fn nullable_dictionary_output_keeps_null_and_empty_distinct_across_updates() {
    let mut group = GroupAggregate::new(&[0], vec![AggSpec::count()]);
    let first = nullable_dictionary_zset(vec![None, Some(0)], vec![3, 4]);
    let output = group.apply(&first).unwrap();
    assert_eq!(output.len(), 2);
    assert!(output.batch.column(0).is_null(0));
    assert!(!output.batch.column(0).is_null(1));
    let next = nullable_dictionary_zset(vec![Some(0)], vec![9]);
    let output = group.apply(&next).unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(group.groups.len(), 2);
}

#[test]
fn native_dictionary_old_new_output_above_int8_capacity_continues() {
    let mut group = GroupAggregate::new(&[0, 1], vec![AggSpec::count()]);
    let seed = dictionary_composite_zset(128, vec![1; 128]);
    group.apply(&seed).unwrap();
    let before = group.export_state().unwrap();
    let cached_before = group.native_keys.as_ref().unwrap().len();
    let result = group.apply(&dictionary_composite_zset(128, vec![1; 128]));
    assert!(result.is_ok());
    assert_ne!(group.export_state().unwrap(), before);
    assert_eq!(group.native_keys.as_ref().unwrap().len(), cached_before);
}

#[test]
fn global_group_state_preserves_explicit_zero_column_row_count_and_null_input() {
    let empty_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Null, true)]));
    let empty_delta = ZSetBatch::new(
        RecordBatch::try_new(
            empty_schema,
            vec![Arc::new(arrow::array::NullArray::new(0))],
        )
        .unwrap(),
        Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
    )
    .unwrap();
    let mut empty_global = GroupAggregate::new(&[], vec![AggSpec::count()]);
    empty_global.apply(&empty_delta).unwrap();
    let empty_state = empty_global.export_state().unwrap();
    let hotlap_core::snapshot::TypedKeyPayload::Ipc(empty_bytes) = &empty_state.typed_keys else {
        panic!("initialized global state needs IPC")
    };
    let expected = Arc::new(Schema::empty());
    let (empty_batches, empty_rows) =
        crate::core::ipc::typed_keys::decode(empty_bytes, &expected).unwrap();
    assert_eq!(empty_rows, 0);
    assert_eq!(empty_batches[0].num_rows(), 0);

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Null, true)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(arrow::array::NullArray::new(2))]).unwrap();
    let delta = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef).unwrap();
    let mut source = GroupAggregate::new(&[], vec![AggSpec::count()]);
    source.apply(&delta).unwrap();
    let state = source.export_state().unwrap();
    let hotlap_core::snapshot::TypedKeyPayload::Ipc(bytes) = &state.typed_keys else {
        panic!("initialized global group needs IPC")
    };
    let expected = Arc::new(Schema::empty());
    let (batches, rows) = crate::core::ipc::typed_keys::decode(bytes, &expected).unwrap();
    assert_eq!(rows, 1);
    assert_eq!(batches[0].num_columns(), 0);
    assert_eq!(batches[0].num_rows(), 1);
    let mut restored = GroupAggregate::new(&[], vec![AggSpec::count()]);
    restored.import_state(&state).unwrap();
    assert_eq!(restored.groups.len(), 1);
    restored.apply(&delta).unwrap();
    assert_eq!(restored.groups.values().next().unwrap().rows, 4);
}

#[test]
fn null_key_is_restored_as_a_real_null_column() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Null, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::NullArray::new(2)),
            Arc::new(Int64Array::from(vec![4, 5])),
        ],
    )
    .unwrap();
    let delta = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef).unwrap();
    let mut source = GroupAggregate::new(&[0], vec![AggSpec::count()]);
    source.apply(&delta).unwrap();
    let state = source.export_state().unwrap();
    let mut restored = GroupAggregate::new(&[0], vec![AggSpec::count()]);
    restored.import_state(&state).unwrap();
    let output = restored.apply(&delta).unwrap();
    assert_eq!(output.schema().field(0).data_type(), &DataType::Null);
    assert_eq!(output.batch.column(0).data_type(), &DataType::Null);
    assert_eq!(output.batch.column(0).len(), output.len());
}

#[test]
fn group_import_rejects_malformed_shape_duplicates_and_old_layout_atomically() {
    use serde::Serialize;

    use hotlap_core::snapshot::{GroupEntry, GroupState, TypedKeyPayload};

    let mut source = GroupAggregate::new(&[0], vec![AggSpec::count()]);
    source.apply(&zset("v", &[(1, 10)], &[1])).unwrap();
    let valid = source.export_state().unwrap();
    let mut bad_stream = valid.clone();
    if let TypedKeyPayload::Ipc(bytes) = &mut bad_stream.typed_keys {
        bytes.pop();
    }
    let mut bad_pairing = valid.clone();
    bad_pairing.groups.clear();
    let wrong_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
    let wrong_batch = RecordBatch::try_new(
        wrong_schema.clone(),
        vec![Arc::new(StringArray::from(vec![Some("1")])) as ArrayRef],
    )
    .unwrap();
    let mut bad_schema = valid.clone();
    bad_schema.typed_keys = TypedKeyPayload::Ipc(
        crate::core::ipc::typed_keys::encode(&wrong_schema, [&wrong_batch]).unwrap(),
    );
    let mut bad_entry = valid.clone();
    bad_entry.groups[0].values[0] =
        hotlap_core::snapshot::AggValue::SumInteger { sum: 1, count: 1 };
    let mut duplicate = valid.clone();
    duplicate.groups.push(duplicate.groups[0].clone());
    let key_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let key_batch = RecordBatch::try_new(
        key_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef],
    )
    .unwrap();
    duplicate.typed_keys = TypedKeyPayload::Ipc(
        crate::core::ipc::typed_keys::encode(&key_schema, [&key_batch]).unwrap(),
    );
    let mut target = GroupAggregate::new(&[0], vec![AggSpec::count()]);
    target.apply(&zset("v", &[(9, 99)], &[1])).unwrap();
    let before = target.export_state().unwrap();
    for invalid in [
        &bad_stream,
        &bad_pairing,
        &bad_schema,
        &bad_entry,
        &duplicate,
    ] {
        assert!(target.import_state(invalid).is_err());
        assert_eq!(target.export_state().unwrap(), before);
    }

    #[derive(Serialize)]
    struct LegacyRawGroupState {
        schema: Option<Vec<u8>>,
        groups: Vec<(Vec<u8>, GroupEntry)>,
    }
    let legacy = LegacyRawGroupState {
        schema: valid.schema.clone(),
        groups: vec![(vec![1], valid.groups[0].clone())],
    };
    let encoded = crate::encode_framed(&legacy).unwrap();
    assert!(crate::decode_framed::<GroupState>(&encoded).is_err());
}

#[test]
fn insert_then_retract_updates_all_aggregates() {
    let mut reducer = reducer();
    let first = reducer.apply(&zset("v", &[(1, 10)], &[1])).unwrap();
    assert_eq!(rows(&first), vec![(1, 1, 10, 10.0, 1)]);

    let second = reducer.apply(&zset("v", &[(1, 20)], &[1])).unwrap();
    assert_eq!(
        rows(&second),
        vec![(1, 1, 10, 10.0, -1), (1, 2, 30, 15.0, 1)]
    );
}

#[test]
fn retraction_lowers_sum_avg_and_count() {
    let mut reducer = reducer();
    reducer
        .apply(&zset("v", &[(1, 10), (1, 20)], &[1, 1]))
        .unwrap();
    let out = reducer.apply(&zset("v", &[(1, 20)], &[-1])).unwrap();
    assert_eq!(rows(&out), vec![(1, 1, 10, 10.0, 1), (1, 2, 30, 15.0, -1)]);
}

#[test]
fn retraction_to_zero_retracts_the_group() {
    let mut reducer = reducer();
    reducer.apply(&zset("v", &[(1, 10)], &[1])).unwrap();
    let out = reducer.apply(&zset("v", &[(1, 10)], &[-1])).unwrap();
    assert_eq!(rows(&out), vec![(1, 1, 10, 10.0, -1)]);
    assert_eq!(reducer.work(), 1);
}

#[test]
fn swaps_that_keep_the_row_count_still_update_sum() {
    let mut reducer = reducer();
    reducer.apply(&zset("v", &[(1, 10)], &[1])).unwrap();
    // Retract 10 and insert 30 in one delta: the row count is unchanged.
    let out = reducer
        .apply(&zset("v", &[(1, 10), (1, 30)], &[-1, 1]))
        .unwrap();
    assert_eq!(rows(&out), vec![(1, 1, 10, 10.0, -1), (1, 1, 30, 30.0, 1)]);
}

#[test]
fn float_sum_and_avg_are_tracked() {
    let mut reducer = GroupAggregate::new(&[0], vec![AggSpec::sum(1), AggSpec::avg(1)]);
    let out = reducer.apply(&zset("v", &[(1, 4)], &[1])).unwrap();
    assert_eq!(out.schema().field(1).data_type(), &DataType::Int64);
    assert_eq!(out.schema().field(2).data_type(), &DataType::Float64);
}

#[test]
fn snapshot_round_trips_aggregate_state() {
    let mut source = reducer();
    source
        .apply(&zset("v", &[(1, 10), (1, 20), (2, 5)], &[1, 1, 1]))
        .unwrap();
    assert!(source.native_keys.is_none());
    let state = source.export_state().unwrap();

    let mut restored = reducer();
    restored.import_state(&state).unwrap();
    assert!(restored.native_keys.is_none());
    let out = restored.apply(&zset("v", &[(1, 30)], &[1])).unwrap();
    assert_eq!(rows(&out), vec![(1, 2, 30, 15.0, -1), (1, 3, 60, 20.0, 1)]);
}

#[test]
fn work_is_proportional_to_touched_keys() {
    let mut reducer = reducer();
    for key in 0..20i64 {
        let out = reducer.apply(&zset("v", &[(key, 1)], &[1])).unwrap();
        assert_eq!(reducer.work(), 1);
        assert_eq!(rows(&out), vec![(key, 1, 1, 1.0, 1)]);
    }
}

#[test]
fn out_of_range_integer_sum_is_rejected_atomically() {
    let mut reducer = reducer();
    reducer.apply(&zset("v", &[(1, i64::MAX)], &[1])).unwrap();

    // `i64::MAX + 1` fits the `i128` accumulator but not the `Int64` output.
    assert!(
        reducer.apply(&zset("v", &[(1, 1)], &[1])).is_err(),
        "an out-of-range output must be rejected"
    );

    // The prior state must be intact: retracting `i64::MAX` removes the group.
    let out = reducer.apply(&zset("v", &[(1, i64::MAX)], &[-1])).unwrap();
    assert_eq!(rows(&out), vec![(1, 1, i64::MAX, i64::MAX as f64, -1)]);
}
