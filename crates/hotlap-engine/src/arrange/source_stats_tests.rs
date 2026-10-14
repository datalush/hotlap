use super::source_stats::{SourceColumnStats, SourceColumnValue, SourceStats};
use super::{KeyedArrangement, converter_for};
use crate::batch::ZSetBatch;
use crate::keys::KeyConverter;
use arrow::array::Int64Array;
use arrow::datatypes::i256;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[test]
fn one_arrangement_tracks_independent_count_and_sum_columns() {
    use arrow::array::{Int32Array, StringArray};

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("wide", DataType::Int64, true),
        Field::new("narrow", DataType::Int32, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    arrangement
        .register_source_stats(
            &[1, 2, 3],
            &[DataType::Utf8, DataType::Int64, DataType::Int32],
            &[false, true, true],
        )
        .unwrap();
    let batch = ZSetBatch::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![4, 4])),
                Arc::new(StringArray::from(vec![Some("x"), None])),
                Arc::new(Int64Array::from(vec![None, Some(i64::MIN)])),
                Arc::new(Int32Array::from(vec![Some(0), None])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![2, -1])),
    )
    .unwrap();
    arrangement.apply(&batch, &keys).unwrap();
    let key = converter_for(arrangement.schema().as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![4]))])
        .unwrap()
        .row(0)
        .owned();
    let stats = arrangement.source_stats(key.as_ref()).unwrap();
    assert_eq!(stats.weight, 1);
    assert_eq!(stats.abs_weight, 3);
    assert_eq!(stats.support, 2);
    assert_eq!(stats.negative_support, 1);
    assert_eq!(stats.columns[0].nonnull, 2);
    assert_eq!(stats.columns[0].sum, None);
    assert_eq!(stats.columns[1].nonnull, -1);
    assert_eq!(stats.columns[1].sum, Some(i256::from_i128(1_i128 << 63)));
    assert_eq!(
        stats.columns[1].abs_sum,
        Some(i256::from_i128(1_i128 << 63))
    );
    assert_eq!(stats.columns[2].nonnull, 2);
    assert_eq!(stats.columns[2].sum, Some(i256::ZERO));
}

#[test]
fn memory_usage_accounts_retained_stats_capacity_and_repeated_zero_support_cycles() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    let batch = |diff| {
        ZSetBatch::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![9])),
                    Arc::new(Int64Array::from(vec![Some(1)])),
                ],
            )
            .unwrap(),
            Arc::new(Int64Array::from(vec![diff])),
        )
        .unwrap()
    };
    arrangement.apply(&batch(1), &keys).unwrap();
    let before_registration = accounted(arrangement.memory_usage().unwrap());
    arrangement
        .register_source_stats(&[1], &[DataType::Int64], &[true])
        .unwrap();
    let after_registration = accounted(arrangement.memory_usage().unwrap());
    assert!(after_registration > before_registration);
    let key = converter_for(schema.as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![9]))])
        .unwrap()
        .row(0)
        .owned();
    for _ in 0..8 {
        arrangement.apply(&batch(-1), &keys).unwrap();
        assert!(arrangement.is_empty());
        assert_eq!(arrangement.source_stats_entries_for_test(), 0);
        assert!(arrangement.source_stats(key.as_ref()).is_none());
        let after_removal = arrangement.memory_usage().unwrap();
        assert!(accounted(after_removal) > before_registration);
        assert!(arrangement.source_stats_capacity_for_test() > 0);
        assert!(
            after_removal.2
                >= arrangement.source_stats_capacity_for_test()
                    * std::mem::size_of::<(Vec<u8>, SourceStats)>()
        );
        arrangement.apply(&batch(1), &keys).unwrap();
        assert_eq!(arrangement.source_stats_entries_for_test(), 1);
        assert!(arrangement.source_stats(key.as_ref()).is_some());
    }
}

#[test]
fn empty_column_demand_tracks_scalar_count_star_stats_and_one_pass_rebuild() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    let batch = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..10).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(vec![Some(0); 10])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![1; 10])),
    )
    .unwrap();
    arrangement.apply(&batch, &keys).unwrap();
    KeyedArrangement::reset_source_stats_rebuild_rows_visited_for_test();
    arrangement.register_source_stats(&[], &[], &[]).unwrap();
    assert_eq!(
        KeyedArrangement::source_stats_rebuild_rows_visited_for_test(),
        10
    );
    assert_eq!(
        KeyedArrangement::source_stats_key_decode_calls_for_test(),
        0
    );
    assert_eq!(
        KeyedArrangement::source_stats_payload_rows_decoded_for_test(),
        0
    );
    arrangement.register_source_stats(&[], &[], &[]).unwrap();
    assert_eq!(
        KeyedArrangement::source_stats_rebuild_rows_visited_for_test(),
        10
    );
    assert_eq!(
        KeyedArrangement::source_stats_key_decode_calls_for_test(),
        0
    );
    assert_eq!(
        KeyedArrangement::source_stats_payload_rows_decoded_for_test(),
        0
    );
    assert!(arrangement.iter().all(|(key, _, _)| {
        arrangement
            .source_stats(&key)
            .is_some_and(|stats| stats.weight == 1 && stats.columns.is_empty())
    }));
}

#[test]
fn late_multirow_bucket_rebuild_decodes_the_canonical_key_once() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    let batch = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7; 10])),
                Arc::new(Int64Array::from((1..=10).collect::<Vec<_>>())),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![1; 10])),
    )
    .unwrap();
    arrangement.apply(&batch, &keys).unwrap();
    KeyedArrangement::reset_source_stats_rebuild_rows_visited_for_test();
    arrangement
        .register_source_stats(&[0, 1], &[DataType::Int64, DataType::Int64], &[false, true])
        .unwrap();
    assert_eq!(
        KeyedArrangement::source_stats_rebuild_rows_visited_for_test(),
        10
    );
    assert_eq!(
        KeyedArrangement::source_stats_key_decode_calls_for_test(),
        1
    );
    assert_eq!(
        KeyedArrangement::source_stats_payload_rows_decoded_for_test(),
        10
    );
    arrangement
        .register_source_stats(&[0, 1], &[DataType::Int64, DataType::Int64], &[false, true])
        .unwrap();
    assert_eq!(
        KeyedArrangement::source_stats_key_decode_calls_for_test(),
        1
    );
    assert_eq!(
        KeyedArrangement::source_stats_payload_rows_decoded_for_test(),
        10
    );
    let key = arrangement.iter_key_bytes().next().unwrap();
    let stats = arrangement.source_stats(key).unwrap();
    assert_eq!(stats.weight, 10);
    assert_eq!(stats.columns[0].nonnull, 10);
    assert_eq!(stats.columns[1].sum, Some(i256::from_i128(55)));
}

#[test]
fn nullable_key_sum_reuses_key_row_zero_for_each_payload() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    let batch = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![None, None])),
                Arc::new(Int64Array::from(vec![Some(10), Some(11)])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![1, 1])),
    )
    .unwrap();
    arrangement.apply(&batch, &keys).unwrap();
    arrangement
        .register_source_stats(&[0], &[DataType::Int64], &[true])
        .unwrap();
    let key = keys
        .convert(&[Arc::new(Int64Array::from(vec![None]))])
        .unwrap()
        .row(0)
        .owned();
    let stats = arrangement.source_stats(key.as_ref()).unwrap();
    assert_eq!(stats.support, 2);
    assert_eq!(stats.weight, 2);
    assert_eq!(stats.columns[0].nonnull, 0);
    assert_eq!(stats.columns[0].sum, Some(i256::ZERO));
}

#[test]
fn canonical_one_many_one_empty_reinsert_reclaims_stats_without_net_liveness() {
    use arrow::array::StringArray;

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    let batch = |labels: Vec<Option<&str>>, values: Vec<Option<i64>>, diffs: Vec<i64>| {
        ZSetBatch::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1; diffs.len()])),
                    Arc::new(StringArray::from(labels)),
                    Arc::new(Int64Array::from(values)),
                ],
            )
            .unwrap(),
            Arc::new(Int64Array::from(diffs)),
        )
        .unwrap()
    };
    arrangement
        .apply(&batch(vec![Some("a")], vec![Some(2)], vec![3]), &keys)
        .unwrap();
    arrangement
        .register_source_stats(&[1, 2], &[DataType::Utf8, DataType::Int64], &[false, true])
        .unwrap();
    let canonical_key = arrangement.iter().next().unwrap().0;
    assert_eq!(
        arrangement.bucket_kind_for_test(&canonical_key),
        Some("one")
    );

    arrangement
        .apply(&batch(vec![None], vec![None], vec![-1]), &keys)
        .unwrap();
    assert_eq!(
        arrangement.bucket_kind_for_test(&canonical_key),
        Some("many")
    );
    let stats = arrangement.source_stats(&canonical_key).unwrap();
    assert_eq!((stats.weight, stats.abs_weight, stats.support), (2, 4, 2));
    assert_eq!(stats.negative_support, 1);

    arrangement
        .apply(&batch(vec![None], vec![None], vec![1]), &keys)
        .unwrap();
    assert_eq!(
        arrangement.bucket_kind_for_test(&canonical_key),
        Some("one")
    );
    assert_eq!(arrangement.source_stats(&canonical_key).unwrap().support, 1);
    arrangement
        .apply(&batch(vec![Some("a")], vec![Some(2)], vec![-3]), &keys)
        .unwrap();
    assert!(arrangement.is_empty());
    assert!(arrangement.source_stats(&canonical_key).is_none());

    arrangement
        .apply(&batch(vec![Some("a")], vec![Some(0)], vec![-2]), &keys)
        .unwrap();
    let stats = arrangement.source_stats(&canonical_key).unwrap();
    assert_eq!((stats.weight, stats.abs_weight, stats.support), (-2, 2, 1));
    assert_eq!(stats.negative_support, 1);
    assert_eq!(stats.columns[0].nonnull, -2);
    assert_eq!(stats.columns[0].sum, None);
    assert_eq!(stats.columns[1].nonnull, -2);
    assert_eq!(stats.columns[1].sum, Some(i256::ZERO));
    assert_eq!(
        arrangement.bucket_kind_for_test(&canonical_key),
        Some("one")
    );
}

fn accounted((payload, capacity, structural): (usize, usize, usize)) -> usize {
    payload + capacity + structural
}

#[test]
fn keyed_arrangement_maintains_registered_stats_from_changed_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let key_converter = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    arrangement
        .register_source_stats(&[1], &[DataType::Int64], &[true])
        .unwrap();
    let batch = ZSetBatch::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7, 7])),
                Arc::new(Int64Array::from(vec![i64::MIN, i64::MIN])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![2, -1])),
    )
    .unwrap();
    arrangement.apply(&batch, &key_converter).unwrap();
    let key = converter_for(arrangement.schema().as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![7]))])
        .unwrap()
        .row(0)
        .owned();
    let stats = arrangement.source_stats(key.as_ref()).unwrap();
    assert_eq!(stats.weight, 1);
    assert_eq!(stats.abs_weight, 1);
    assert_eq!(stats.support, 1);
    assert_eq!(stats.columns[0].nonnull, 1);
    assert_eq!(
        stats.columns[0].sum,
        Some(i256::from_i128(i128::from(i64::MIN)))
    );
    assert_eq!(
        stats.columns[0].abs_sum,
        Some(i256::from_i128(1_i128 << 63))
    );
}

#[test]
fn failed_later_weight_prefix_leaves_rows_and_source_stats_unchanged() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let key_converter = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    arrangement
        .register_source_stats(&[1], &[DataType::Int64], &[true])
        .unwrap();
    let make_batch = |rows: &[(i64, Option<i64>, i64)]| {
        ZSetBatch::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap(),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        )
        .unwrap()
    };
    arrangement
        .apply(&make_batch(&[(7, Some(0), i64::MAX)]), &key_converter)
        .unwrap();
    let key = converter_for(arrangement.schema().as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![7]))])
        .unwrap()
        .row(0)
        .owned();
    let before = arrangement.source_stats(key.as_ref()).unwrap().clone();
    let rows_before = arrangement.iter().collect::<Vec<_>>();
    let stats_entries_before = arrangement.source_stats_entries_for_test();
    assert!(
        arrangement
            .apply(
                &make_batch(&[(8, Some(5), 1), (7, Some(0), 1)]),
                &key_converter
            )
            .is_err()
    );
    assert_eq!(arrangement.source_stats(key.as_ref()), Some(&before));
    assert_eq!(
        arrangement.source_stats_entries_for_test(),
        stats_entries_before
    );
    assert_eq!(arrangement.iter().collect::<Vec<_>>(), rows_before);
    let new_key = converter_for(arrangement.schema().as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![8]))])
        .unwrap()
        .row(0)
        .owned();
    assert!(arrangement.source_stats(new_key.as_ref()).is_none());
    assert_eq!(before.columns[0].nonnull, i128::from(i64::MAX));
    assert_eq!(before.columns[0].sum, Some(i256::ZERO));
    assert_eq!(before.columns[0].abs_sum, Some(i256::ZERO));
}

#[test]
fn late_registration_rebuilds_once_then_tracks_next_delta() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let key_converter = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    let batch = |value, diff| {
        ZSetBatch::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![7])),
                    Arc::new(Int64Array::from(vec![value])),
                ],
            )
            .unwrap(),
            Arc::new(Int64Array::from(vec![diff])),
        )
        .unwrap()
    };
    arrangement
        .apply(&batch(Some(4), 3), &key_converter)
        .unwrap();
    arrangement
        .register_source_stats(&[1], &[DataType::Int64], &[true])
        .unwrap();
    arrangement
        .register_source_stats(&[1], &[DataType::Int64], &[true])
        .unwrap();
    let key = converter_for(arrangement.schema().as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![7]))])
        .unwrap()
        .row(0)
        .owned();
    assert_eq!(arrangement.source_stats(key.as_ref()).unwrap().weight, 3);
    assert_eq!(
        arrangement.source_stats(key.as_ref()).unwrap().columns[0].sum,
        Some(i256::from_i128(12))
    );
    arrangement
        .apply(&batch(Some(4), 2), &key_converter)
        .unwrap();
    let stats = arrangement.source_stats(key.as_ref()).unwrap();
    assert_eq!(stats.weight, 5);
    assert_eq!(stats.columns[0].sum, Some(i256::from_i128(20)));
}

#[test]
fn staged_final_weight_commit_publishes_rows_and_source_stats_together() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let key_converter = KeyConverter::new(schema.clone().as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    let batch = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![Some(5)])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![2])),
    )
    .unwrap();
    arrangement.apply(&batch, &key_converter).unwrap();
    arrangement
        .register_source_stats(&[1], &[DataType::Int64], &[true])
        .unwrap();
    let (key, payload, old) = arrangement.iter().next().unwrap();
    let staged = arrangement
        .stage_final_weights(&schema, &[(key, payload, old, -3)])
        .unwrap();
    let key_owned = converter_for(schema.as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![7]))])
        .unwrap()
        .row(0)
        .owned();
    assert_eq!(
        arrangement.source_stats(key_owned.as_ref()).unwrap().weight,
        2
    );
    arrangement.commit_final_weights(staged);
    let stats = arrangement.source_stats(key_owned.as_ref()).unwrap();
    assert_eq!(stats.weight, -3);
    assert_eq!(stats.abs_weight, 3);
    assert_eq!(stats.negative_support, 1);
    assert_eq!(stats.columns[0].sum, Some(i256::from_i128(-15)));
}

#[test]
fn prepared_source_update_borrows_proposed_stats_and_publishes_only_on_commit() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    arrangement
        .register_source_stats(&[1], &[DataType::Int64], &[true])
        .unwrap();
    let initial = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![Some(5)])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![2])),
    )
    .unwrap();
    arrangement.apply(&initial, &keys).unwrap();
    let key = converter_for(schema.as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![7]))])
        .unwrap()
        .row(0)
        .owned();
    let before = arrangement.source_stats(key.as_ref()).unwrap().clone();
    let update = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![5])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![3])),
    )
    .unwrap();
    let prepared = arrangement.prepare_source_update(&update, &keys).unwrap();
    assert_eq!(arrangement.source_stats(key.as_ref()), Some(&before));
    let proposed = prepared
        .proposed_stats(&arrangement, key.as_ref())
        .unwrap()
        .clone();
    assert_eq!(proposed.weight, 5);
    assert_eq!(proposed.columns[0].nonnull, 5);
    assert_eq!(proposed.columns[0].sum, Some(i256::from_i128(25)));
    prepared.commit(&mut arrangement);
    assert_eq!(arrangement.source_stats(key.as_ref()), Some(&proposed));
}

#[test]
fn prepared_source_update_deletion_is_none_not_old_stats() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    arrangement
        .register_source_stats(&[1], &[DataType::Int64], &[false])
        .unwrap();
    let initial = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![Some(5)])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![1])),
    )
    .unwrap();
    arrangement.apply(&initial, &keys).unwrap();
    let key = converter_for(schema.as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![7]))])
        .unwrap()
        .row(0)
        .owned();
    let delete = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![5])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![-1])),
    )
    .unwrap();
    let prepared = arrangement.prepare_source_update(&delete, &keys).unwrap();
    assert!(
        prepared
            .proposed_stats(&arrangement, key.as_ref())
            .is_none()
    );
    assert!(arrangement.source_stats(key.as_ref()).is_some());
}

#[test]
fn failed_prepared_source_update_keeps_all_canonical_rows_and_stats_unchanged() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    arrangement
        .register_source_stats(&[1], &[DataType::Int64], &[true])
        .unwrap();
    let initial = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![i64::MAX, 1])),
    )
    .unwrap();
    arrangement.apply(&initial, &keys).unwrap();
    let before_rows = arrangement.iter().collect::<Vec<_>>();
    let before_stats = arrangement.source_stats_entries_for_test();
    let overflow = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 1])),
                Arc::new(Int64Array::from(vec![Some(20), Some(10)])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![1, 1])),
    )
    .unwrap();
    assert!(arrangement.prepare_source_update(&overflow, &keys).is_err());
    assert_eq!(arrangement.iter().collect::<Vec<_>>(), before_rows);
    assert_eq!(arrangement.source_stats_entries_for_test(), before_stats);
}

#[test]
fn key_only_prepared_update_uses_the_arrangements_canonical_unit_payload() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    arrangement
        .register_source_stats(&[0], &[DataType::Int64], &[true])
        .unwrap();
    let append = ZSetBatch::new(
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![7]))]).unwrap(),
        Arc::new(Int64Array::from(vec![2])),
    )
    .unwrap();
    let prepared = arrangement.prepare_source_update(&append, &keys).unwrap();
    let key = keys
        .convert(append.batch.columns())
        .unwrap()
        .row(0)
        .as_ref()
        .to_vec();
    assert_eq!(
        prepared.proposed_stats(&arrangement, &key).unwrap().weight,
        2
    );
    let proposed = prepared.proposed_stats(&arrangement, &key).unwrap();
    assert_eq!(proposed.columns[0].nonnull, 2);
    assert_eq!(proposed.columns[0].sum, Some(i256::from_i128(14)));
    assert!(arrangement.is_empty());
    prepared.commit(&mut arrangement);
    assert_eq!(
        arrangement.iter().map(|row| row.2).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(arrangement.source_stats(&key).unwrap().weight, 2);
}

#[test]
fn repeated_identity_prepared_update_keeps_original_old_weight() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    arrangement
        .register_source_stats(&[1], &[DataType::Int64], &[true])
        .unwrap();
    let append = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7, 7])),
                Arc::new(Int64Array::from(vec![Some(5), Some(5)])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![1, 1])),
    )
    .unwrap();
    let prepared = arrangement.prepare_source_update(&append, &keys).unwrap();
    let key = keys
        .convert(append.batch.columns())
        .unwrap()
        .row(0)
        .as_ref()
        .to_vec();
    assert_eq!(
        prepared.proposed_stats(&arrangement, &key).unwrap().weight,
        2
    );
    prepared.commit(&mut arrangement);
    assert_eq!(
        arrangement.iter().map(|row| row.2).collect::<Vec<_>>(),
        vec![2]
    );
    let stats = arrangement.source_stats(&key).unwrap();
    assert_eq!(stats.weight, 2);
    assert_eq!(stats.columns[0].nonnull, 2);
    assert_eq!(stats.columns[0].sum, Some(i256::from_i128(10)));
    let retract = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7, 7])),
                Arc::new(Int64Array::from(vec![Some(5), Some(5)])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![-1, -1])),
    )
    .unwrap();
    let prepared = arrangement.prepare_source_update(&retract, &keys).unwrap();
    assert!(prepared.proposed_stats(&arrangement, &key).is_none());
    prepared.commit(&mut arrangement);
    assert!(arrangement.is_empty());
    assert!(arrangement.source_stats(&key).is_none());
}

#[test]
fn registered_int32_column_is_decoded_exactly() {
    use arrow::array::Int32Array;

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int32, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    let batch = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![Some(i32::MIN)])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![3])),
    )
    .unwrap();
    arrangement.apply(&batch, &keys).unwrap();
    arrangement
        .register_source_stats(&[1], &[DataType::Int32], &[true])
        .unwrap();
    let key = converter_for(schema.as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![2]))])
        .unwrap()
        .row(0)
        .owned();
    let column = &arrangement.source_stats(key.as_ref()).unwrap().columns[0];
    assert_eq!(column.nonnull, 3);
    assert_eq!(column.sum, Some(i256::from_i128(i128::from(i32::MIN) * 3)));
    assert_eq!(column.abs_sum, Some(i256::from_i128((1_i128 << 31) * 3)));
}

#[test]
fn registered_int8_and_int16_sum_columns_widen_exactly() {
    use arrow::array::{Int8Array, Int16Array};

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("small", DataType::Int8, true),
        Field::new("medium", DataType::Int16, true),
    ]));
    let keys = KeyConverter::new(schema.as_ref(), &[0]).unwrap();
    let mut arrangement = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
    arrangement
        .register_source_stats(&[1, 2], &[DataType::Int8, DataType::Int16], &[true, true])
        .unwrap();
    let batch = ZSetBatch::new(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3])),
                Arc::new(Int8Array::from(vec![Some(i8::MIN)])),
                Arc::new(Int16Array::from(vec![Some(i16::MIN)])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![2])),
    )
    .unwrap();
    arrangement.apply(&batch, &keys).unwrap();
    let key = converter_for(schema.as_ref(), &[0])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![3]))])
        .unwrap()
        .row(0)
        .owned();
    let columns = &arrangement.source_stats(key.as_ref()).unwrap().columns;
    assert_eq!(
        columns[0].sum,
        Some(i256::from_i128(i128::from(i8::MIN) * 2))
    );
    assert_eq!(columns[0].abs_sum, Some(i256::from_i128(256)));
    assert_eq!(
        columns[1].sum,
        Some(i256::from_i128(i128::from(i16::MIN) * 2))
    );
    assert_eq!(columns[1].abs_sum, Some(i256::from_i128(65_536)));
}

#[test]
fn weight_change_tracks_nullable_i64_and_absolute_minimum_exactly() {
    let old = SourceStats::default();
    let next = old
        .stage_weight_change(0, 2, &[integer(i128::from(i64::MIN), true)])
        .unwrap();
    assert_eq!(next.weight, 2);
    assert_eq!(next.abs_weight, 2);
    assert_eq!(next.support, 1);
    assert_eq!(next.negative_support, 0);
    assert_eq!(
        next.columns,
        vec![SourceColumnStats {
            nonnull: 2,
            sum: Some(i256::from_i128(i128::from(i64::MIN) * 2)),
            abs_sum: Some(i256::from_i128(1_i128 << 64)),
        }]
    );
    let removed = next
        .stage_weight_change(2, 0, &[integer(i128::from(i64::MIN), true)])
        .unwrap();
    assert_eq!(removed.weight, 0);
    assert_eq!(removed.abs_weight, 0);
    assert_eq!(removed.support, 0);
    assert_eq!(removed.negative_support, 0);
    assert_eq!(
        removed.columns,
        vec![SourceColumnStats {
            nonnull: 0,
            sum: Some(i256::ZERO),
            abs_sum: Some(i256::ZERO),
        }]
    );
}

#[test]
fn weight_change_preserves_null_counts_and_negative_row_support() {
    let state = SourceStats::default()
        .stage_weight_change(0, -3, &[null(false)])
        .unwrap();
    assert_eq!(state.weight, -3);
    assert_eq!(state.abs_weight, 3);
    assert_eq!(state.support, 1);
    assert_eq!(state.negative_support, 1);
    assert_eq!(
        state.columns,
        vec![SourceColumnStats {
            nonnull: 0,
            sum: None,
            abs_sum: None,
        }]
    );
    let state = state.stage_weight_change(-3, 4, &[null(false)]).unwrap();
    assert_eq!(state.weight, 4);
    assert_eq!(state.abs_weight, 4);
    assert_eq!(state.negative_support, 0);
}

fn integer(value: i128, sum: bool) -> SourceColumnValue {
    SourceColumnValue {
        is_null: false,
        integer: Some(value),
        sum,
    }
}

fn null(sum: bool) -> SourceColumnValue {
    SourceColumnValue {
        is_null: true,
        integer: None,
        sum,
    }
}
