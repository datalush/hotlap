use super::super::certificate::{
    BoundKind, CertificateDecision, GroupPrefixBounds, MetricBinding, certify_group,
    compile_metric_bindings,
};
use super::super::group_memo::MetricLimit;
use super::super::group_memo::{Contribution, GroupMemo};
use super::super::raw_bounds::SignedBound;
use super::*;
use arrow::datatypes::i256;
use hotlap_core::snapshot::{AggValue, GroupEntry};

#[test]
fn maintained_integer_avg_compiles_sum_and_nonnull_certificate_binding() {
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    let mapping = ColumnMapping {
        left_source: hotlap_core::InputId(0),
        right_source: hotlap_core::InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Int64)],
    };
    let bindings = compile_metric_bindings(&mapping, &[AggSpec::avg(1)])
        .expect("integer AVG needs a maintained SUM and NN certificate binding");
    assert_eq!(
        bindings
            .iter()
            .map(|binding| (
                binding.memo_index,
                binding.aggregate_index,
                binding.nn_memo_index,
            ))
            .collect::<Vec<_>>(),
        vec![(0, 0, Some(1))]
    );
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let right_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let demand = super::super::demand::compile_demand(
        &mapping,
        &[AggSpec::avg(1)],
        &left_schema,
        &right_schema,
    )
    .expect("integer AVG should demand source value/NN facts");
    assert_eq!(demand.left_columns, vec![1]);
    assert_eq!(demand.left_column_sums, vec![true]);
    let metrics = super::super::group_metrics::compile_metric_demands(
        &mapping,
        &[AggSpec::avg(1)],
        2,
        &demand.left_columns,
        &demand.right_columns,
    )
    .unwrap();
    assert_eq!(metrics.len(), 2);
    assert_eq!(
        metrics[0].left,
        super::super::group_metrics::SourceMeasure::Sum(0)
    );
    assert_eq!(
        metrics[1].left,
        super::super::group_metrics::SourceMeasure::NonNull(0)
    );
    let mixed_mapping = ColumnMapping {
        aggregate_inputs: vec![None, Some(1), Some(2)],
        aggregate_input_types: vec![None, Some(DataType::Int64), Some(DataType::Int64)],
        ..mapping
    };
    let mixed = compile_metric_bindings(
        &mixed_mapping,
        &[AggSpec::count(), AggSpec::avg(1), AggSpec::sum(2)],
    )
    .unwrap();
    assert_eq!(
        mixed
            .iter()
            .map(|binding| (
                binding.kind,
                binding.memo_index,
                binding.aggregate_index,
                binding.nn_memo_index,
            ))
            .collect::<Vec<_>>(),
        vec![
            (BoundKind::Count, 0, 0, None),
            (BoundKind::IntegerAvg, 1, 1, Some(2)),
            (BoundKind::IntegerSum, 3, 2, Some(4)),
        ]
    );
}

#[test]
fn maintained_integer_avg_certificate_accepts_exact_source_stats_append() {
    use crate::arrange::source_stats::{SourceColumnValue, SourceStats};
    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let demands = [
        super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::Sum(0),
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(1_i128 << 53),
        },
        super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::NonNull(0),
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(i64::MAX as i128),
        },
    ];
    let left = SourceStats::default()
        .stage_weight_change(
            0,
            1,
            &[SourceColumnValue {
                is_null: false,
                integer: Some(1_i128 << 53),
                sum: true,
            }],
        )
        .unwrap();
    let right = SourceStats::default()
        .stage_weight_change(0, 1, &[])
        .unwrap();
    let contribution = Contribution::from_source_stats(vec![7], &left, &right, &demands).unwrap();
    let mut memo = GroupMemo::default();
    memo.replace(vec![1], contribution).unwrap();
    let mut rows = SignedBound::default();
    rows.add_raw(i256::ONE).unwrap();
    let mut sum = SignedBound::default();
    sum.add_raw(i256::from_i128(1_i128 << 53)).unwrap();
    let mut nn = SignedBound::default();
    nn.add_raw(i256::ONE).unwrap();
    let bounds = GroupPrefixBounds {
        rows,
        metrics: vec![sum, nn],
        sum_abs_delta: vec![(0, i256::from_i128(1_i128 << 53))],
    };
    let binding = MetricBinding {
        kind: BoundKind::IntegerAvg,
        memo_index: 0,
        aggregate_index: 0,
        nn_memo_index: Some(1),
    };
    assert_eq!(
        certify_group(None, None, memo.group(&[7]), &bounds, &[binding]),
        CertificateDecision::Certified
    );

    let input = crate::batch::ZSetBatch::new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![7_i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1_i64 << 53])) as ArrayRef,
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![1_i64])),
    )
    .unwrap();
    let mut dense = crate::ops::GroupAggregate::new(&[0], vec![AggSpec::avg(1)]);
    let dense_output = dense.apply(&input).unwrap();
    let dense_avg = dense_output
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        dense_avg.value(0).to_bits(),
        ((1_i64 << 53) as f64).to_bits()
    );
}

#[test]
fn maintained_integer_avg_rejects_source_product_above_exact_f64_boundary() {
    use crate::arrange::source_stats::{SourceColumnValue, SourceStats};
    let demands = [
        super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::Sum(0),
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(1_i128 << 53),
        },
        super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::NonNull(0),
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(i64::MAX as i128),
        },
    ];
    let left = SourceStats::default()
        .stage_weight_change(
            0,
            1,
            &[SourceColumnValue {
                is_null: false,
                integer: Some((1_i128 << 53) + 1),
                sum: true,
            }],
        )
        .unwrap();
    let right = SourceStats::default()
        .stage_weight_change(0, 1, &[])
        .unwrap();
    let mut memo = GroupMemo::default();
    memo.replace(
        vec![1],
        Contribution::from_source_stats(vec![7], &left, &right, &demands).unwrap(),
    )
    .unwrap();
    let mut rows = SignedBound::default();
    rows.add_raw(i256::ONE).unwrap();
    let mut sum = SignedBound::default();
    sum.add_raw(i256::from_i128((1_i128 << 53) + 1)).unwrap();
    let mut nn = SignedBound::default();
    nn.add_raw(i256::ONE).unwrap();
    let bounds = GroupPrefixBounds {
        rows,
        metrics: vec![sum, nn],
        sum_abs_delta: vec![(0, i256::from_i128((1_i128 << 53) + 1))],
    };
    let binding = MetricBinding {
        kind: BoundKind::IntegerAvg,
        memo_index: 0,
        aggregate_index: 0,
        nn_memo_index: Some(1),
    };
    assert_eq!(
        certify_group(None, None, memo.group(&[7]), &bounds, &[binding]),
        CertificateDecision::NeedsRowEvaluation
    );
}

#[test]
fn maintained_integer_avg_source_stats_bound_within_key_cancellation_magnitude() {
    use crate::arrange::source_stats::{SourceColumnValue, SourceStats};
    let demands = [super::super::group_metrics::JoinMetricDemand {
        left: super::super::group_metrics::SourceMeasure::Sum(0),
        right: super::super::group_metrics::SourceMeasure::Weight,
        limit: i256::from_i128(1_i128 << 53),
    }];
    let row = |integer| SourceColumnValue {
        is_null: false,
        integer: Some(integer),
        sum: true,
    };
    let left = SourceStats::default()
        .stage_weight_change(0, 1, &[row(1_i128 << 53)])
        .unwrap()
        .stage_weight_change(0, 1, &[row(-(1_i128 << 53))])
        .unwrap();
    let right = SourceStats::default()
        .stage_weight_change(0, 1, &[])
        .unwrap();
    assert_eq!(left.columns[0].sum, Some(i256::ZERO));
    assert_eq!(left.columns[0].abs_sum, Some(i256::from_i128(1_i128 << 54)));
    let contribution = Contribution::from_source_stats(vec![7], &left, &right, &demands).unwrap();
    let mut memo = GroupMemo::default();
    memo.replace(vec![1], contribution).unwrap();
    assert_eq!(
        memo.group(&[7]).unwrap().metrics[0].eligibility,
        MetricLimit::OverLimit,
        "AVG L1 history above 2^53 must not look exact just because its signed sum cancels"
    );
}

#[test]
fn maintained_integer_avg_rejects_large_join_weight_even_for_zero_products() {
    use crate::arrange::source_stats::{SourceColumnValue, SourceStats};
    let demands = [
        super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::Sum(0),
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(1_i128 << 53),
        },
        super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::NonNull(0),
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(i64::MAX as i128),
        },
    ];
    let weight = (1_i64 << 53) + 1;
    let left = SourceStats::default()
        .stage_weight_change(
            0,
            weight,
            &[SourceColumnValue {
                is_null: false,
                integer: Some(0),
                sum: true,
            }],
        )
        .unwrap();
    let right = SourceStats::default()
        .stage_weight_change(0, 1, &[])
        .unwrap();
    let mut memo = GroupMemo::default();
    memo.replace(
        vec![1],
        Contribution::from_source_stats(vec![7], &left, &right, &demands).unwrap(),
    )
    .unwrap();
    let mut rows = SignedBound::default();
    rows.add_raw(i256::from_i128(i128::from(weight))).unwrap();
    let sum = SignedBound::default();
    let mut nn = SignedBound::default();
    nn.add_raw(i256::from_i128(i128::from(weight))).unwrap();
    let bounds = GroupPrefixBounds {
        rows,
        metrics: vec![sum, nn],
        sum_abs_delta: vec![(0, i256::ZERO)],
    };
    let binding = MetricBinding {
        kind: BoundKind::IntegerAvg,
        memo_index: 0,
        aggregate_index: 0,
        nn_memo_index: Some(1),
    };
    assert_eq!(
        certify_group(None, None, memo.group(&[7]), &bounds, &[binding]),
        CertificateDecision::NeedsRowEvaluation
    );
}

#[test]
fn maintained_integer_avg_rejects_inexact_authoritative_float_history() {
    use crate::arrange::source_stats::{SourceColumnValue, SourceStats};
    let demands = [
        super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::Sum(0),
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(1_i128 << 53),
        },
        super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::NonNull(0),
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(i64::MAX as i128),
        },
    ];
    let value = |integer| SourceColumnValue {
        is_null: false,
        integer: Some(integer),
        sum: true,
    };
    let old_left = SourceStats::default()
        .stage_weight_change(0, 1, &[value(4)])
        .unwrap();
    let left = old_left.stage_weight_change(0, 1, &[value(2)]).unwrap();
    let right = SourceStats::default()
        .stage_weight_change(0, 1, &[])
        .unwrap();
    let mut old_memo = GroupMemo::default();
    old_memo
        .replace(
            vec![1],
            Contribution::from_source_stats(vec![7], &old_left, &right, &demands).unwrap(),
        )
        .unwrap();
    let next_contribution =
        Contribution::from_source_stats(vec![7], &left, &right, &demands).unwrap();
    let stage = old_memo
        .stage_updates([(vec![1], Some(next_contribution))])
        .unwrap();
    let proposed = old_memo.group_after_stage(&stage, &[7]);
    let mut rows = SignedBound::default();
    rows.add_raw(i256::ONE).unwrap();
    let mut sum = SignedBound::default();
    sum.add_raw(i256::from_i128(2)).unwrap();
    let mut nn = SignedBound::default();
    nn.add_raw(i256::ONE).unwrap();
    let bounds = GroupPrefixBounds {
        rows,
        metrics: vec![sum, nn],
        sum_abs_delta: vec![(0, i256::from_i128(2))],
    };
    let binding = MetricBinding {
        kind: BoundKind::IntegerAvg,
        memo_index: 0,
        aggregate_index: 0,
        nn_memo_index: Some(1),
    };
    let inexact_history = GroupEntry {
        rows: 1,
        values: vec![AggValue::Avg {
            sum: 4.5,
            count: 1,
            nan: 0,
            pos_inf: 0,
            neg_inf: 0,
        }],
    };
    assert_eq!(
        certify_group(
            Some(&inexact_history),
            old_memo.group(&[7]),
            proposed,
            &bounds,
            &[binding]
        ),
        CertificateDecision::NeedsRowEvaluation
    );
}

#[test]
fn maintained_integer_avg_rejects_signed_zero_authoritative_history() {
    use crate::arrange::source_stats::{SourceColumnValue, SourceStats};
    let demands = [
        super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::Sum(0),
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(1_i128 << 53),
        },
        super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::NonNull(0),
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(i64::MAX as i128),
        },
    ];
    let value = |integer| SourceColumnValue {
        is_null: false,
        integer: Some(integer),
        sum: true,
    };
    let old_left = SourceStats::default()
        .stage_weight_change(0, 1, &[value(0)])
        .unwrap();
    let left = old_left.stage_weight_change(0, 1, &[value(1)]).unwrap();
    let right = SourceStats::default()
        .stage_weight_change(0, 1, &[])
        .unwrap();
    let mut old_memo = GroupMemo::default();
    old_memo
        .replace(
            vec![1],
            Contribution::from_source_stats(vec![7], &old_left, &right, &demands).unwrap(),
        )
        .unwrap();
    let stage = old_memo
        .stage_updates([(
            vec![1],
            Some(Contribution::from_source_stats(vec![7], &left, &right, &demands).unwrap()),
        )])
        .unwrap();
    let proposed = old_memo.group_after_stage(&stage, &[7]);
    let mut rows = SignedBound::default();
    rows.add_raw(i256::ONE).unwrap();
    let mut sum = SignedBound::default();
    sum.add_raw(i256::ONE).unwrap();
    let mut nn = SignedBound::default();
    nn.add_raw(i256::ONE).unwrap();
    let bounds = GroupPrefixBounds {
        rows,
        metrics: vec![sum, nn],
        sum_abs_delta: vec![(0, i256::ONE)],
    };
    let binding = MetricBinding {
        kind: BoundKind::IntegerAvg,
        memo_index: 0,
        aggregate_index: 0,
        nn_memo_index: Some(1),
    };
    let old = GroupEntry {
        rows: 1,
        values: vec![AggValue::Avg {
            sum: -0.0,
            count: 1,
            nan: 0,
            pos_inf: 0,
            neg_inf: 0,
        }],
    };
    assert_eq!(
        certify_group(
            Some(&old),
            old_memo.group(&[7]),
            proposed,
            &bounds,
            &[binding]
        ),
        CertificateDecision::NeedsRowEvaluation
    );
}
