use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::i256;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use hotlap_core::ids::InputId;
use hotlap_core::plan::{AggFunc, AggSpec, Plan};
use hotlap_core::plan_normalize::{ColumnMapping, Recognition, recognize};
use hotlap_core::snapshot::AggValue;

use super::group_codec::GroupKeyCodec;
use super::group_memo::{Contribution, GroupMemo, MetricContribution, MetricLimit};
use super::group_metrics::{
    JoinMetricDemand, SourceMeasure, compile_metric_demands, contribution_metrics,
};
use crate::arrange::KeyedArrangement;
use crate::arrange::source_stats::{SourceColumnStats, SourceStats};
use crate::batch::ZSetBatch;
use crate::keys::KeyConverter;
use crate::ops::{GroupAggregate, Join};

fn exact(value: i128, abs: i128) -> MetricContribution {
    MetricContribution::exact(i256::from_i128(value), i256::from_i128(abs))
}

#[test]
fn replacing_a_full_key_updates_shared_group_and_reclaims_zero_support() {
    let mut memo = GroupMemo::default();
    let key_a = vec![1, 2];
    let key_b = vec![3, 4];
    let group = vec![9];
    memo.replace(
        key_a.clone(),
        Contribution::new(group.clone(), vec![exact(7, 7)]),
    )
    .unwrap();
    memo.replace(
        key_b.clone(),
        Contribution::new(group.clone(), vec![exact(-2, 2)]),
    )
    .unwrap();
    assert_eq!(
        memo.group(&group).unwrap().metrics[0].subtotal,
        i256::from_i128(5)
    );
    memo.replace(
        key_a.clone(),
        Contribution::new(group.clone(), vec![exact(0, 0)]),
    )
    .unwrap();
    assert_eq!(
        memo.group(&group).unwrap().metrics[0].subtotal,
        i256::from_i128(-2)
    );
    memo.remove(&key_a).unwrap();
    memo.remove(&key_b).unwrap();
    assert!(memo.group(&group).is_none());
    assert!(memo.is_empty());
}

#[test]
fn negative_source_key_support_is_counted_sparse_and_recovers_on_removal() {
    let positive = SourceStats::default()
        .stage_weight_change(0, 2, &[])
        .unwrap();
    let negative = SourceStats::default()
        .stage_weight_change(0, -1, &[])
        .unwrap();
    let right = SourceStats::default()
        .stage_weight_change(0, 1, &[])
        .unwrap();
    let demand = [JoinMetricDemand {
        left: SourceMeasure::Weight,
        right: SourceMeasure::Weight,
        limit: i256::from_i128(i64::MAX as i128),
    }];
    let mut memo = GroupMemo::default();
    let stage = memo
        .stage_updates([
            (
                vec![1],
                Some(Contribution::from_source_stats(vec![7], &positive, &right, &demand).unwrap()),
            ),
            (
                vec![2],
                Some(Contribution::from_source_stats(vec![7], &negative, &right, &demand).unwrap()),
            ),
        ])
        .unwrap();
    memo.commit(stage);
    assert_eq!(memo.group(&[7]).unwrap().negative_source_keys, 1);

    let revised_positive = SourceStats::default()
        .stage_weight_change(0, 3, &[])
        .unwrap();
    let stage = memo
        .stage_updates([(
            vec![1],
            Some(
                Contribution::from_source_stats(vec![7], &revised_positive, &right, &demand)
                    .unwrap(),
            ),
        )])
        .unwrap();
    memo.commit(stage);
    assert_eq!(memo.group(&[7]).unwrap().negative_source_keys, 1);

    let stage = memo.stage_updates([(vec![2], None)]).unwrap();
    memo.commit(stage);
    assert_eq!(memo.group(&[7]).unwrap().negative_source_keys, 0);
    assert_eq!(
        memo.group(&[7]).unwrap().metrics[0].subtotal,
        i256::from_i128(3)
    );
}

#[test]
fn over_limit_key_keeps_group_unknown_until_that_key_is_removed() {
    let mut memo = GroupMemo::default();
    let key_large = vec![1];
    let key_large_negative = vec![3];
    let key_small = vec![2];
    let group = vec![7];
    memo.replace(
        key_large.clone(),
        Contribution::new(group.clone(), vec![MetricContribution::over_limit(1)]),
    )
    .unwrap();
    memo.replace(
        key_large_negative.clone(),
        Contribution::new(group.clone(), vec![MetricContribution::over_limit(-1)]),
    )
    .unwrap();
    memo.replace(
        key_small.clone(),
        Contribution::new(group.clone(), vec![exact(3, 3)]),
    )
    .unwrap();
    assert_eq!(
        memo.group(&group).unwrap().metrics[0].eligibility,
        MetricLimit::OverLimit
    );
    memo.remove(&key_large).unwrap();
    assert_eq!(
        memo.group(&group).unwrap().metrics[0].eligibility,
        MetricLimit::OverLimit
    );
    memo.remove(&key_large_negative).unwrap();
    let metric = &memo.group(&group).unwrap().metrics[0];
    assert_eq!(metric.eligibility, MetricLimit::Exact);
    assert_eq!(metric.subtotal, i256::from_i128(3));
}

#[test]
fn stage_failure_does_not_publish_any_touched_group_changes() {
    let mut memo = GroupMemo::default();
    let key = vec![1];
    let group = vec![8];
    memo.replace(
        key.clone(),
        Contribution::new(group.clone(), vec![exact(2, 2)]),
    )
    .unwrap();
    let before = memo.clone();
    let invalid = Contribution::new(vec![9], vec![]);
    assert!(memo.replace(key, invalid).is_err());
    assert_eq!(
        memo.group(&group).unwrap().metrics[0].subtotal,
        i256::from_i128(2)
    );
    assert_eq!(memo.len(), before.len());
}

#[test]
fn multi_key_stage_is_sparse_and_failed_batch_leaves_original_untouched() {
    let mut memo = GroupMemo::default();
    memo.replace(vec![1], Contribution::new(vec![8], vec![exact(2, 2)]))
        .unwrap();
    let stage = memo.stage_updates([
        (vec![2], Some(Contribution::new(vec![8], vec![exact(4, 4)]))),
        (vec![1], Some(Contribution::new(vec![9], vec![]))),
    ]);
    assert!(stage.is_err());
    assert_eq!(memo.len(), 1);
    assert_eq!(
        memo.group(&[8]).unwrap().metrics[0].subtotal,
        i256::from_i128(2)
    );
    assert!(memo.group(&[9]).is_none());

    let stage = memo
        .stage_updates([
            (vec![1], Some(Contribution::new(vec![8], vec![exact(5, 5)]))),
            (
                vec![2],
                Some(Contribution::new(vec![8], vec![exact(-1, 1)])),
            ),
        ])
        .unwrap();
    assert_eq!(memo.len(), 1, "staging must not publish full-key entries");
    memo.commit(stage);
    assert_eq!(
        memo.group(&[8]).unwrap().metrics[0].subtotal,
        i256::from_i128(4)
    );
    assert_eq!(memo.len(), 2);
}

#[test]
fn group_codec_preserves_reordered_duplicate_and_nullable_key_components() {
    let types = vec![DataType::Int64, DataType::Utf8];
    let codec = GroupKeyCodec::new(&types, &[1, 0, 1]).unwrap();
    let full = RowConverter::new(types.iter().cloned().map(SortField::new).collect()).unwrap();
    let columns: Vec<ArrayRef> = vec![
        std::sync::Arc::new(Int64Array::from(vec![None])),
        std::sync::Arc::new(StringArray::from(vec![Some("x")])),
    ];
    let key = full
        .convert_columns(&columns)
        .unwrap()
        .row(0)
        .as_ref()
        .to_vec();
    let group = codec.encode(&key).unwrap();
    let projected = RowConverter::new(
        [DataType::Utf8, DataType::Int64, DataType::Utf8]
            .into_iter()
            .map(SortField::new)
            .collect(),
    )
    .unwrap();
    let expected: Vec<ArrayRef> = vec![
        std::sync::Arc::new(StringArray::from(vec![Some("x")])),
        std::sync::Arc::new(Int64Array::from(vec![None])),
        std::sync::Arc::new(StringArray::from(vec![Some("x")])),
    ];
    let expected = projected
        .convert_columns(&expected)
        .unwrap()
        .row(0)
        .as_ref()
        .to_vec();
    assert_eq!(group, expected);
}

#[test]
fn joined_metrics_use_borrowed_source_stats_and_bound_before_multiply() {
    let left = SourceStats {
        weight: 3,
        abs_weight: 3,
        support: 1,
        negative_support: 0,
        columns: vec![SourceColumnStats {
            nonnull: 2,
            sum: Some(i256::from_i128(11)),
            abs_sum: Some(i256::from_i128(11)),
        }],
    };
    let right = SourceStats {
        weight: 4,
        abs_weight: 4,
        support: 1,
        negative_support: 0,
        columns: vec![SourceColumnStats {
            nonnull: 3,
            sum: Some(i256::from_i128(13)),
            abs_sum: Some(i256::from_i128(13)),
        }],
    };
    let demands = [
        JoinMetricDemand {
            left: SourceMeasure::Weight,
            right: SourceMeasure::Weight,
            limit: i256::from_i128(i64::MAX as i128),
        },
        JoinMetricDemand {
            left: SourceMeasure::NonNull(0),
            right: SourceMeasure::Weight,
            limit: i256::from_i128(i64::MAX as i128),
        },
        JoinMetricDemand {
            left: SourceMeasure::Sum(0),
            right: SourceMeasure::Weight,
            limit: i256::from_i128(i128::MAX),
        },
    ];
    let result = contribution_metrics(Some(&left), Some(&right), &demands).unwrap();
    assert_eq!(result[0].value, i256::from_i128(12));
    assert_eq!(result[1].value, i256::from_i128(8));
    assert_eq!(result[2].value, i256::from_i128(44));
    let oversized = MetricContribution::bounded_product(
        i256::from_i128(i128::MAX),
        i256::from_i128(2),
        i256::from_i128(i128::MAX),
    )
    .unwrap();
    assert!(oversized.over_limit);
}

#[test]
fn memo_memory_report_tracks_live_key_and_group_metadata() {
    let mut memo = GroupMemo::default();
    memo.replace(
        vec![1, 2, 3],
        Contribution::new(vec![8, 9], vec![exact(1, 1)]),
    )
    .unwrap();
    let (payload, capacity) = memo.memory_usage().unwrap();
    assert!(payload >= 5);
    assert!(capacity > 0);
    memo.remove(&[1, 2, 3]).unwrap();
    assert!(memo.is_empty());
    assert_eq!(memo.group(&[8, 9]), None);
}

#[test]
fn metric_demands_keep_logical_aggregate_order_and_add_sum_nonnull() {
    let mapping = ColumnMapping {
        left_source: InputId(1),
        right_source: InputId(2),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![None, Some(2), Some(2)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![None, Some(DataType::Int64), Some(DataType::Int64)],
    };
    let aggs = [
        AggSpec::count(),
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(0),
        },
        AggSpec::sum(0),
    ];
    let demands = compile_metric_demands(&mapping, &aggs, 2, &[0], &[0]).unwrap();
    assert_eq!(demands.len(), 4);
    assert_eq!(demands[0].left, SourceMeasure::Weight);
    assert_eq!(demands[0].right, SourceMeasure::Weight);
    assert_eq!(demands[1].left, SourceMeasure::Weight);
    assert_eq!(demands[1].right, SourceMeasure::NonNull(0));
    assert_eq!(demands[2].left, SourceMeasure::Weight);
    assert_eq!(demands[2].right, SourceMeasure::Sum(0));
    assert_eq!(demands[3].right, SourceMeasure::NonNull(0));
}

#[test]
fn normalized_sum_right_nullable_sum_left_count_star_order_keeps_hidden_nn_slots() {
    let left_schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("lk", DataType::Int64, false),
        Field::new("lv", DataType::Int64, true),
    ]));
    let right_schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("rk", DataType::Int64, false),
        Field::new("rv", DataType::Int64, true),
    ]));
    let aggs = vec![
        AggSpec::sum(3),
        AggSpec {
            func: AggFunc::Count,
            input: Some(1),
        },
        AggSpec::sum(1),
        AggSpec::count(),
    ];
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(1))),
            right: Box::new(Plan::Source(InputId(2))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: aggs.clone(),
    };
    let schemas = [
        (InputId(1), left_schema.clone()),
        (InputId(2), right_schema.clone()),
    ]
    .into_iter()
    .collect();
    let Recognition::Eligible(mapping) = recognize(&plan, &schemas) else {
        panic!("normalized grouped join expected");
    };
    let demand =
        super::demand::compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap();
    let metrics = compile_metric_demands(
        &mapping,
        &aggs,
        2,
        &demand.left_columns,
        &demand.right_columns,
    )
    .unwrap();
    let bindings = super::certificate::compile_metric_bindings(&mapping, &aggs).unwrap();
    assert_eq!(metrics.len(), 6);
    assert_eq!(
        bindings
            .iter()
            .map(|binding| (
                binding.memo_index,
                binding.aggregate_index,
                binding.nn_memo_index
            ))
            .collect::<Vec<_>>(),
        vec![(0, 0, Some(1)), (2, 1, None), (3, 2, Some(4)), (5, 3, None)]
    );
    assert_eq!(
        (metrics[0].left, metrics[0].right),
        (SourceMeasure::Weight, SourceMeasure::Sum(0))
    );
    assert_eq!(
        (metrics[1].left, metrics[1].right),
        (SourceMeasure::Weight, SourceMeasure::NonNull(0))
    );
    assert_eq!(
        (metrics[2].left, metrics[2].right),
        (SourceMeasure::NonNull(0), SourceMeasure::Weight)
    );
    assert_eq!(
        (metrics[3].left, metrics[3].right),
        (SourceMeasure::Sum(0), SourceMeasure::Weight)
    );
    assert_eq!(
        (metrics[4].left, metrics[4].right),
        (SourceMeasure::NonNull(0), SourceMeasure::Weight)
    );
    assert_eq!(
        (metrics[5].left, metrics[5].right),
        (SourceMeasure::Weight, SourceMeasure::Weight)
    );
}

#[test]
fn actual_arrangements_memo_matches_dense_join_group_after_incremental_updates() {
    let left_schema = test_source_schema("left");
    let right_schema = test_source_schema("right");
    let aggs = vec![
        AggSpec::count(),
        AggSpec {
            func: AggFunc::Count,
            input: Some(2),
        },
        AggSpec::sum(2),
        AggSpec::sum(5),
    ];
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(1))),
            right: Box::new(Plan::Source(InputId(2))),
            left_key: vec![0, 1],
            right_key: vec![0, 1],
        }),
        key: vec![0],
        aggs: aggs.clone(),
    };
    let schemas = [
        (InputId(1), left_schema.clone()),
        (InputId(2), right_schema.clone()),
    ]
    .into_iter()
    .collect();
    let Recognition::Eligible(mapping) = recognize(&plan, &schemas) else {
        panic!("expected normalized grouped join plan");
    };
    let demand =
        super::demand::compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap();
    assert_eq!(demand.left_columns, vec![2]);
    assert_eq!(demand.right_columns, vec![2]);
    let metric_demands = compile_metric_demands(
        &mapping,
        &aggs,
        left_schema.fields().len(),
        &demand.left_columns,
        &demand.right_columns,
    )
    .unwrap();
    let group_codec = GroupKeyCodec::new(&mapping.join_key_types, &mapping.group_key).unwrap();

    let left_keys = KeyConverter::new(left_schema.as_ref(), &[0, 1]).unwrap();
    let right_keys = KeyConverter::new(right_schema.as_ref(), &[0, 1]).unwrap();
    let mut left_arrangement = KeyedArrangement::new(left_schema.clone(), &[0, 1]).unwrap();
    let mut right_arrangement = KeyedArrangement::new(right_schema.clone(), &[0, 1]).unwrap();
    left_arrangement
        .register_source_stats(
            &demand.left_columns,
            &demand.left_column_types,
            &demand.left_column_sums,
        )
        .unwrap();
    right_arrangement
        .register_source_stats(
            &demand.right_columns,
            &demand.right_column_types,
            &demand.right_column_sums,
        )
        .unwrap();

    let mut dense_join = Join::new(&[0, 1], &[0, 1]);
    let mut dense_group = GroupAggregate::new(&[0], aggs);
    let mut memo = GroupMemo::default();
    let left_delta = source_delta(
        left_schema.clone(),
        &[(1, 10, Some(5), 1), (1, 20, None, 1), (2, 10, None, 1)],
    );
    let right_delta = source_delta(
        right_schema.clone(),
        &[
            (1, 10, Some(2), 1),
            (1, 20, Some(8), 1),
            (2, 10, Some(3), 1),
        ],
    );
    apply_dense_and_memo(
        &left_delta,
        &right_delta,
        &mut dense_join,
        &mut dense_group,
        &mut left_arrangement,
        &mut right_arrangement,
        &left_keys,
        &right_keys,
        &group_codec,
        &metric_demands,
        &mut memo,
    );
    assert_group_matches_dense(&memo, &dense_group, 2, 3);
    assert_dense_sum_count(&dense_group, 2, 0);

    // Both sides change the same existing canonical key in one push. The new
    // contribution is computed from final L and R stats, including the cross term.
    let left_delta = source_delta(left_schema.clone(), &[(1, 10, Some(0), 1)]);
    let right_delta = source_delta(right_schema.clone(), &[(1, 10, Some(4), 1)]);
    apply_dense_and_memo(
        &left_delta,
        &right_delta,
        &mut dense_join,
        &mut dense_group,
        &mut left_arrangement,
        &mut right_arrangement,
        &left_keys,
        &right_keys,
        &group_codec,
        &metric_demands,
        &mut memo,
    );
    assert_group_matches_dense(&memo, &dense_group, 2, 3);

    // Two more full join keys collapse into the same subset group; group 2 is
    // retained with SUM(left)=NULL (nonnull count zero) until a zero arrives.
    let left_delta = source_delta(left_schema.clone(), &[(1, 30, Some(0), 1)]);
    let right_delta = source_delta(right_schema.clone(), &[(1, 30, None, 1)]);
    apply_dense_and_memo(
        &left_delta,
        &right_delta,
        &mut dense_join,
        &mut dense_group,
        &mut left_arrangement,
        &mut right_arrangement,
        &left_keys,
        &right_keys,
        &group_codec,
        &metric_demands,
        &mut memo,
    );
    assert_group_matches_dense(&memo, &dense_group, 2, 4);
    assert_dense_sum_count(&dense_group, 2, 0);

    let left_delta = source_delta(left_schema.clone(), &[(2, 10, Some(0), 1)]);
    let right_delta = ZSetBatch::empty(right_schema.clone());
    apply_dense_and_memo(
        &left_delta,
        &right_delta,
        &mut dense_join,
        &mut dense_group,
        &mut left_arrangement,
        &mut right_arrangement,
        &left_keys,
        &right_keys,
        &group_codec,
        &metric_demands,
        &mut memo,
    );
    assert_group_matches_dense(&memo, &dense_group, 2, 4);
    assert_dense_sum_count(&dense_group, 2, 1);

    let left_delta = source_delta(left_schema.clone(), &[(2, 10, None, -1)]);
    let right_delta = ZSetBatch::empty(right_schema.clone());
    apply_dense_and_memo(
        &left_delta,
        &right_delta,
        &mut dense_join,
        &mut dense_group,
        &mut left_arrangement,
        &mut right_arrangement,
        &left_keys,
        &right_keys,
        &group_codec,
        &metric_demands,
        &mut memo,
    );
    assert_group_matches_dense(&memo, &dense_group, 2, 4);
    assert_dense_sum_count(&dense_group, 2, 1);

    let left_delta = source_delta(left_schema.clone(), &[(2, 10, Some(0), -1)]);
    apply_dense_and_memo(
        &left_delta,
        &right_delta,
        &mut dense_join,
        &mut dense_group,
        &mut left_arrangement,
        &mut right_arrangement,
        &left_keys,
        &right_keys,
        &group_codec,
        &metric_demands,
        &mut memo,
    );
    assert_group_matches_dense(&memo, &dense_group, 1, 3);
}

fn test_source_schema(prefix: &str) -> std::sync::Arc<Schema> {
    std::sync::Arc::new(Schema::new(vec![
        Field::new(format!("{prefix}_group"), DataType::Int64, false),
        Field::new(format!("{prefix}_key"), DataType::Int64, false),
        Field::new(format!("{prefix}_value"), DataType::Int64, true),
    ]))
}

fn source_delta(
    schema: std::sync::Arc<Schema>,
    entries: &[(i64, i64, Option<i64>, i64)],
) -> ZSetBatch {
    let batch = RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(Int64Array::from(
                entries.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(Int64Array::from(
                entries.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(Int64Array::from(
                entries.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ZSetBatch::new(
        batch,
        std::sync::Arc::new(Int64Array::from(
            entries.iter().map(|row| row.3).collect::<Vec<_>>(),
        )),
    )
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn apply_dense_and_memo(
    left_delta: &ZSetBatch,
    right_delta: &ZSetBatch,
    dense_join: &mut Join,
    dense_group: &mut GroupAggregate,
    left_arrangement: &mut KeyedArrangement,
    right_arrangement: &mut KeyedArrangement,
    left_keys: &KeyConverter,
    right_keys: &KeyConverter,
    group_codec: &GroupKeyCodec,
    metric_demands: &[JoinMetricDemand],
    memo: &mut GroupMemo,
) {
    let joined_delta = dense_join.apply(left_delta, right_delta).unwrap();
    dense_group.apply(&joined_delta).unwrap();
    left_arrangement.apply(left_delta, left_keys).unwrap();
    right_arrangement.apply(right_delta, right_keys).unwrap();

    let mut affected = std::collections::BTreeSet::new();
    for (delta, converter) in [(left_delta, left_keys), (right_delta, right_keys)] {
        let encoded = converter.convert(delta.batch.columns()).unwrap();
        for index in 0..encoded.num_rows() {
            affected.insert(encoded.row(index).as_ref().to_vec());
        }
    }
    let updates = affected
        .into_iter()
        .map(|join_key| {
            let left = left_arrangement.source_stats(&join_key);
            let right = right_arrangement.source_stats(&join_key);
            let contribution = match (left, right) {
                (Some(left), Some(right)) => Some(
                    Contribution::from_source_stats(
                        group_codec.encode(&join_key).unwrap(),
                        left,
                        right,
                        metric_demands,
                    )
                    .unwrap(),
                ),
                _ => None,
            };
            (join_key, contribution)
        })
        .collect::<Vec<_>>();
    let stage = memo.stage_updates(updates).unwrap();
    memo.commit(stage);
}

fn assert_group_matches_dense(
    memo: &GroupMemo,
    dense: &GroupAggregate,
    expected_group_count: usize,
    expected_full_key_count: usize,
) {
    let group_converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
    let groups = dense.retained_groups();
    assert_eq!(groups.len(), expected_group_count);
    assert_eq!(memo.len(), expected_full_key_count);
    for group_value in [1_i64, 2_i64] {
        let group_key = group_converter
            .convert_columns(&[std::sync::Arc::new(Int64Array::from(vec![group_value]))])
            .unwrap()
            .row(0)
            .as_ref()
            .to_vec();
        match (memo.group(&group_key), groups.get(&group_key)) {
            (None, None) => {}
            (Some(actual), Some(expected)) => {
                assert_eq!(actual.metrics.len(), 6);
                assert_eq!(
                    actual.metrics[0].subtotal,
                    i256::from_i128(count(&expected.values[0]))
                );
                assert_eq!(
                    actual.metrics[1].subtotal,
                    i256::from_i128(count(&expected.values[1]))
                );
                assert_sum_metric(&actual.metrics[2], &expected.values[2]);
                assert_eq!(
                    actual.metrics[3].subtotal,
                    i256::from_i128(sum_count(&expected.values[2]))
                );
                assert_sum_metric(&actual.metrics[4], &expected.values[3]);
                assert_eq!(
                    actual.metrics[5].subtotal,
                    i256::from_i128(sum_count(&expected.values[3]))
                );
            }
            state => panic!(
                "memo and independent dense GroupAggregate disagree for group {group_value}: {state:?}"
            ),
        }
    }
}

fn count(value: &AggValue) -> i128 {
    match value {
        AggValue::Count(count) => i128::from(*count),
        other => panic!("expected COUNT accumulator, got {other:?}"),
    }
}

fn sum_count(value: &AggValue) -> i128 {
    match value {
        AggValue::SumInteger { count, .. } => i128::from(*count),
        other => panic!("expected integer SUM accumulator, got {other:?}"),
    }
}

fn assert_sum_metric(metric: &super::group_memo::GroupMetric, value: &AggValue) {
    match value {
        AggValue::SumInteger { sum, .. } => {
            assert_eq!(metric.subtotal, i256::from_i128(*sum));
        }
        other => panic!("expected integer SUM accumulator, got {other:?}"),
    }
}

fn assert_dense_sum_count(dense: &GroupAggregate, group: i64, expected_count: i128) {
    let key = RowConverter::new(vec![SortField::new(DataType::Int64)])
        .unwrap()
        .convert_columns(&[std::sync::Arc::new(Int64Array::from(vec![group]))])
        .unwrap()
        .row(0)
        .as_ref()
        .to_vec();
    let entry = dense.retained_groups().get(&key).unwrap();
    assert!(entry.rows > 0, "the group remains live while SUM is NULL");
    assert_eq!(sum_count(&entry.values[2]), expected_count);
    if expected_count == 0 {
        assert!(matches!(
            entry.values[2],
            AggValue::SumInteger { sum: 0, count: 0 }
        ));
    }
}
