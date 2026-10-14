//! Numeric fast-path eligibility. `NeedsRowEvaluation` is never a query error.

use std::collections::HashMap;

use arrow::datatypes::{DataType, i256};
use hotlap_core::plan::{AggFunc, AggSpec};
use hotlap_core::plan_normalize::ColumnMapping;
use hotlap_core::snapshot::{AggValue, GroupEntry};

use super::{
    group_memo::{GroupContribution, MetricLimit},
    raw_bounds::{KeyNumericBounds, SignedBound},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CertificateDecision {
    Certified,
    NeedsRowEvaluation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BoundKind {
    Count,
    NullableCount,
    IntegerSum,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MetricBinding {
    pub(crate) kind: BoundKind,
    /// Exact slot in old and proposed source-derived group memo.
    pub(crate) memo_index: usize,
    /// Logical aggregate index in authoritative `GroupEntry`.
    pub(crate) aggregate_index: usize,
    /// Hidden NN slot for SUM, otherwise `None`.
    pub(crate) nn_memo_index: Option<usize>,
}

/// Compile memo-slot bindings directly from the normalized aggregate order.
/// SUM contributes its sum slot then its hidden non-null-count slot.
pub(crate) fn compile_metric_bindings(
    mapping: &ColumnMapping,
    aggregates: &[AggSpec],
) -> Option<Vec<MetricBinding>> {
    if mapping.aggregate_inputs.len() != aggregates.len() {
        return None;
    }
    if mapping.aggregate_input_types.len() != aggregates.len() {
        return None;
    }
    let mut bindings = Vec::with_capacity(aggregates.len());
    let mut slot = 0;
    for (aggregate_index, aggregate) in aggregates.iter().enumerate() {
        if mapping
            .aggregate_inputs
            .get(aggregate_index)
            .is_none_or(|input| input.is_some() != aggregate.input.is_some())
        {
            return None;
        }
        match (aggregate.func, aggregate.input) {
            (AggFunc::Count, None) => {
                if mapping.aggregate_input_types[aggregate_index].is_some() {
                    return None;
                }
                bindings.push(MetricBinding {
                    kind: BoundKind::Count,
                    memo_index: slot,
                    aggregate_index,
                    nn_memo_index: None,
                });
                slot += 1;
            }
            (AggFunc::Count, Some(_)) => {
                mapping.aggregate_input_types[aggregate_index].as_ref()?;
                bindings.push(MetricBinding {
                    kind: BoundKind::NullableCount,
                    memo_index: slot,
                    aggregate_index,
                    nn_memo_index: None,
                });
                slot += 1;
            }
            (AggFunc::Sum, Some(_)) => {
                if !matches!(
                    mapping
                        .aggregate_input_types
                        .get(aggregate_index)
                        .and_then(Option::as_ref),
                    Some(DataType::Int32 | DataType::Int64)
                ) {
                    return None;
                }
                bindings.push(MetricBinding {
                    kind: BoundKind::IntegerSum,
                    memo_index: slot,
                    aggregate_index,
                    nn_memo_index: Some(slot + 1),
                });
                slot += 2;
            }
            _ => return None,
        }
    }
    Some(bindings)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GroupPrefixBounds {
    pub(crate) rows: SignedBound,
    /// Per memo slot, aggregated from every affected canonical full key.
    pub(crate) metrics: Vec<SignedBound>,
    /// Per integer SUM aggregate index, absolute joined-sum delta bounds.
    pub(crate) sum_abs_delta: Vec<(usize, i256)>,
}

/// Merge only key summaries already mapped through the canonical requested
/// group codec. Callers pass touched keys; unaffected groups are not scanned.
pub(crate) fn aggregate_group_bounds(
    mapped: impl IntoIterator<Item = (Vec<u8>, KeyNumericBounds)>,
) -> Option<HashMap<Vec<u8>, GroupPrefixBounds>> {
    let mut groups = HashMap::<Vec<u8>, GroupPrefixBounds>::new();
    for (group_key, key) in mapped {
        let group = groups.entry(group_key).or_default();
        if let Some(rows) = key.rows {
            group.rows.positive = group.rows.positive.checked_add(rows.positive)?;
            group.rows.negative = group.rows.negative.checked_add(rows.negative)?;
        }
        if group.metrics.len() < key.metrics.len() {
            group
                .metrics
                .resize(key.metrics.len(), SignedBound::default());
            group
                .sum_abs_delta
                .resize(key.metrics.len(), (0, i256::ZERO));
        }
        for (index, bound) in key.metrics.into_iter().enumerate() {
            group.metrics[index].positive =
                group.metrics[index].positive.checked_add(bound.positive)?;
            group.metrics[index].negative =
                group.metrics[index].negative.checked_add(bound.negative)?;
        }
        for (index, bound) in key.sum_abs_delta.into_iter().enumerate() {
            if let Some(bound) = bound {
                group.sum_abs_delta[index].0 = index;
                group.sum_abs_delta[index].1 = group.sum_abs_delta[index].1.checked_add(bound)?;
            }
        }
    }
    Some(groups)
}

/// Compare proposed source-derived totals with actual authoritative history and
/// prove ordered COUNT/NN and conservative integer-SUM/render bounds. It only
/// chooses eligibility; callers retain Dense semantics for all other cases.
pub(crate) fn certify_group(
    old: Option<&GroupEntry>,
    old_memo: Option<&GroupContribution>,
    proposed_memo: Option<&GroupContribution>,
    bounds: &GroupPrefixBounds,
    bindings: &[MetricBinding],
) -> CertificateDecision {
    if bindings.is_empty()
        || old_memo.is_some_and(|group| group.negative_source_keys != 0)
        || proposed_memo.is_some_and(|group| group.negative_source_keys != 0)
    {
        return CertificateDecision::NeedsRowEvaluation;
    }
    let old_rows = i256::from_i128(i128::from(old.map_or(0, |entry| entry.rows)));
    if old_rows < i256::ZERO
        || !bounds
            .rows
            .within_prefixes(old_rows, i256::from_i128(i64::MAX as i128))
    {
        return CertificateDecision::NeedsRowEvaluation;
    }
    let Some(proposed_rows) = bounds
        .rows
        .positive
        .checked_add(old_rows)
        .and_then(|value| value.checked_sub(bounds.rows.negative))
    else {
        return CertificateDecision::NeedsRowEvaluation;
    };
    let proposed_rows = match i64::try_from(
        proposed_rows
            .to_string()
            .parse::<i128>()
            .unwrap_or(i128::MAX),
    ) {
        Ok(value) if value >= 0 => value,
        _ => return CertificateDecision::NeedsRowEvaluation,
    };
    if proposed_rows == 0 {
        return if proposed_memo.is_none() && validate_old_history(old, old_memo, bounds, bindings) {
            CertificateDecision::Certified
        } else {
            CertificateDecision::NeedsRowEvaluation
        };
    }
    let Some(proposed_memo) = proposed_memo else {
        return CertificateDecision::NeedsRowEvaluation;
    };
    if old.is_some_and(|entry| {
        entry.rows < 0
            || entry.rows
                != i64::try_from(old_rows.to_string().parse::<i128>().unwrap_or(i128::MAX))
                    .unwrap_or(-1)
    }) {
        return CertificateDecision::NeedsRowEvaluation;
    }
    let zero_metric = super::group_memo::GroupMetric::default();
    for binding in bindings {
        let (Some(previous), Some(next), Some(delta_bound)) = (
            old_memo
                .and_then(|memo| memo.metrics.get(binding.memo_index))
                .or(Some(&zero_metric)),
            proposed_memo.metrics.get(binding.memo_index),
            bounds.metrics.get(binding.memo_index),
        ) else {
            return CertificateDecision::NeedsRowEvaluation;
        };
        let metric_delta_valid = if binding.kind == BoundKind::IntegerSum {
            let delta_abs = bounds
                .sum_abs_delta
                .iter()
                .find(|(index, _)| *index == binding.memo_index)
                .map(|(_, value)| *value);
            match (
                next.subtotal
                    .checked_sub(previous.subtotal)
                    .and_then(abs_i256),
                delta_abs,
            ) {
                (Some(actual), Some(bound)) => actual <= bound,
                _ => false,
            }
        } else {
            delta_bound
                .positive
                .checked_add(previous.subtotal)
                .and_then(|value| value.checked_sub(delta_bound.negative))
                == Some(next.subtotal)
        };
        if previous.eligibility != MetricLimit::Exact
            || next.eligibility != MetricLimit::Exact
            || !delta_bound.within_prefixes(previous.subtotal, previous.limit)
            || !metric_delta_valid
        {
            return CertificateDecision::NeedsRowEvaluation;
        }
        let history = old.and_then(|entry| entry.values.get(binding.aggregate_index));
        match (binding.kind, history) {
            (BoundKind::Count, Some(AggValue::Count(value)))
                if i256::from_i128(i128::from(*value)) == previous.subtotal
                    && old.is_some_and(|entry| *value == entry.rows)
                    && bounds.metrics[binding.memo_index] == bounds.rows => {}
            (BoundKind::Count, None) if previous.subtotal == i256::ZERO => {}
            (BoundKind::NullableCount, Some(AggValue::Count(value)))
                if i256::from_i128(i128::from(*value)) == previous.subtotal
                    && *value >= 0
                    && old.is_some_and(|entry| *value <= entry.rows) => {}
            (BoundKind::NullableCount, None) if previous.subtotal == i256::ZERO => {}
            (BoundKind::IntegerSum, Some(AggValue::SumInteger { sum, count })) => {
                if i256::from_i128(*sum) != previous.subtotal {
                    return CertificateDecision::NeedsRowEvaluation;
                }
                let Some(nn_index) = binding.nn_memo_index else {
                    return CertificateDecision::NeedsRowEvaluation;
                };
                let (Some(old_nn), Some(new_nn), Some(nn_bound)) = (
                    old_memo
                        .and_then(|memo| memo.metrics.get(nn_index))
                        .or(Some(&zero_metric)),
                    proposed_memo.metrics.get(nn_index),
                    bounds.metrics.get(nn_index),
                ) else {
                    return CertificateDecision::NeedsRowEvaluation;
                };
                if old_nn.subtotal != i256::from_i128(i128::from(*count))
                    || old_nn.eligibility != MetricLimit::Exact
                    || new_nn.eligibility != MetricLimit::Exact
                    || !nn_bound.within_prefixes(old_nn.subtotal, i256::from_i128(i64::MAX as i128))
                    || nn_bound
                        .positive
                        .checked_add(old_nn.subtotal)
                        .and_then(|value| value.checked_sub(nn_bound.negative))
                        != Some(new_nn.subtotal)
                    || *count < 0
                    || old.is_some_and(|entry| *count > entry.rows)
                    || i64::try_from(*sum).is_err()
                {
                    return CertificateDecision::NeedsRowEvaluation;
                }
                let abs_delta = bounds
                    .sum_abs_delta
                    .iter()
                    .find(|(index, _)| *index == binding.memo_index)
                    .map(|(_, value)| *value);
                let Some(abs_delta) = abs_delta else {
                    return CertificateDecision::NeedsRowEvaluation;
                };
                let Some(old_abs) = abs_i256(i256::from_i128(*sum)) else {
                    return CertificateDecision::NeedsRowEvaluation;
                };
                if old_abs
                    .checked_add(abs_delta)
                    .is_none_or(|bound| bound > i256::from_i128(i128::MAX))
                    || (new_nn.subtotal > i256::ZERO
                        && i64::try_from(
                            next.subtotal
                                .to_string()
                                .parse::<i128>()
                                .unwrap_or(i128::MAX),
                        )
                        .is_err())
                {
                    return CertificateDecision::NeedsRowEvaluation;
                }
            }
            (BoundKind::IntegerSum, None) if previous.subtotal == i256::ZERO => {
                if !validate_sum_final(*binding, old_memo, proposed_memo, bounds) {
                    return CertificateDecision::NeedsRowEvaluation;
                }
            }
            _ => return CertificateDecision::NeedsRowEvaluation,
        }
    }
    CertificateDecision::Certified
}

fn validate_old_history(
    old: Option<&GroupEntry>,
    old_memo: Option<&GroupContribution>,
    bounds: &GroupPrefixBounds,
    bindings: &[MetricBinding],
) -> bool {
    let Some(old) = old else {
        return old_memo.is_none();
    };
    let Some(memo) = old_memo else {
        return false;
    };
    if old.rows <= 0 {
        return false;
    }
    bindings.iter().all(|binding| {
        let (Some(metric), Some(value)) = (
            memo.metrics.get(binding.memo_index),
            old.values.get(binding.aggregate_index),
        ) else {
            return false;
        };
        let Some(delta) = bounds.metrics.get(binding.memo_index) else {
            return false;
        };
        if metric.eligibility != MetricLimit::Exact
            || !delta.within_prefixes(metric.subtotal, metric.limit)
            || delta
                .positive
                .checked_add(metric.subtotal)
                .and_then(|v| v.checked_sub(delta.negative))
                != Some(i256::ZERO)
        {
            return false;
        }
        match (binding.kind, value) {
            (BoundKind::Count, AggValue::Count(count)) => {
                *count == old.rows && metric.subtotal == i256::from_i128(i128::from(*count))
            }
            (BoundKind::NullableCount, AggValue::Count(count)) => {
                *count >= 0
                    && *count <= old.rows
                    && metric.subtotal == i256::from_i128(i128::from(*count))
            }
            (BoundKind::IntegerSum, AggValue::SumInteger { sum, count }) => {
                let Some(nn_index) = binding.nn_memo_index else {
                    return false;
                };
                let Some(nn_delta) = bounds.metrics.get(nn_index) else {
                    return false;
                };
                memo.metrics.get(nn_index).is_some_and(|nn| {
                    nn.eligibility == MetricLimit::Exact
                        && nn.subtotal == i256::from_i128(i128::from(*count))
                        && nn_delta.within_prefixes(nn.subtotal, i256::from_i128(i64::MAX as i128))
                        && nn_delta
                            .positive
                            .checked_add(nn.subtotal)
                            .and_then(|v| v.checked_sub(nn_delta.negative))
                            == Some(i256::ZERO)
                }) && metric.subtotal == i256::from_i128(*sum)
                    && *count >= 0
                    && *count <= old.rows
                    && bounds
                        .sum_abs_delta
                        .iter()
                        .find(|(index, _)| *index == binding.memo_index)
                        .and_then(|(_, delta)| abs_i256(i256::from_i128(*sum))?.checked_add(*delta))
                        .is_some_and(|bound| bound <= i256::from_i128(i128::MAX))
            }
            _ => false,
        }
    })
}

fn validate_sum_final(
    binding: MetricBinding,
    old_memo: Option<&GroupContribution>,
    proposed_memo: &GroupContribution,
    bounds: &GroupPrefixBounds,
) -> bool {
    let Some(nn_index) = binding.nn_memo_index else {
        return false;
    };
    let zero = super::group_memo::GroupMetric::default();
    let Some(old_nn) = old_memo
        .and_then(|memo| memo.metrics.get(nn_index))
        .or(Some(&zero))
    else {
        return false;
    };
    let (Some(new_nn), Some(nn_bound), Some(sum), Some(sum_delta)) = (
        proposed_memo.metrics.get(nn_index),
        bounds.metrics.get(nn_index),
        proposed_memo.metrics.get(binding.memo_index),
        bounds
            .sum_abs_delta
            .iter()
            .find(|(index, _)| *index == binding.memo_index)
            .map(|(_, value)| value),
    ) else {
        return false;
    };
    let old_sum = old_memo
        .and_then(|memo| memo.metrics.get(binding.memo_index))
        .map_or(i256::ZERO, |metric| metric.subtotal);
    let Some(actual_sum_delta) = sum.subtotal.checked_sub(old_sum).and_then(abs_i256) else {
        return false;
    };
    let Some(old_abs) = abs_i256(old_sum) else {
        return false;
    };
    if old_nn.eligibility != MetricLimit::Exact
        || new_nn.eligibility != MetricLimit::Exact
        || !nn_bound.within_prefixes(old_nn.subtotal, i256::from_i128(i64::MAX as i128))
        || nn_bound
            .positive
            .checked_add(old_nn.subtotal)
            .and_then(|v| v.checked_sub(nn_bound.negative))
            != Some(new_nn.subtotal)
        || actual_sum_delta > *sum_delta
        || old_abs
            .checked_add(*sum_delta)
            .is_none_or(|bound| bound > i256::from_i128(i128::MAX))
    {
        return false;
    }
    new_nn.subtotal == i256::ZERO
        || i64::try_from(
            sum.subtotal
                .to_string()
                .parse::<i128>()
                .unwrap_or(i128::MAX),
        )
        .is_ok()
}

fn abs_i256(value: i256) -> Option<i256> {
    if value < i256::ZERO {
        i256::ZERO.checked_sub(value)
    } else {
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::join::factorized::group_memo::{Contribution, GroupMemo, MetricContribution};

    fn memo(count: i128) -> GroupMemo {
        let mut memo = GroupMemo::default();
        memo.replace(
            vec![1],
            Contribution::new(
                vec![7],
                vec![MetricContribution::exact_with_limit(
                    i256::from_i128(count),
                    abs_i256(i256::from_i128(count)).unwrap(),
                    i256::from_i128(i64::MAX as i128),
                )],
            ),
        )
        .unwrap();
        memo
    }

    #[test]
    fn sorted_raw_prefix_rejects_old_count_max_minus_one_plus_two_then_minus_one() {
        let max = i64::MAX as i128;
        let prior = memo(max - 1);
        let proposed = memo(max);
        let mut rows = SignedBound::default();
        rows.add_raw(i256::from_i128(2)).unwrap();
        rows.add_raw(i256::from_i128(-1)).unwrap();
        let mut delta = SignedBound::default();
        delta.add_raw(i256::from_i128(2)).unwrap();
        delta.add_raw(i256::from_i128(-1)).unwrap();
        let old = GroupEntry {
            rows: (max - 1) as i64,
            values: vec![AggValue::Count((max - 1) as i64)],
        };
        let bounds = GroupPrefixBounds {
            rows,
            metrics: vec![delta],
            sum_abs_delta: vec![],
        };
        assert_eq!(
            certify_group(
                Some(&old),
                prior.group(&[7]),
                proposed.group(&[7]),
                &bounds,
                &[MetricBinding {
                    kind: BoundKind::Count,
                    memo_index: 0,
                    aggregate_index: 0,
                    nn_memo_index: None,
                }]
            ),
            CertificateDecision::NeedsRowEvaluation
        );
    }

    #[test]
    fn safe_bilateral_opposite_sign_delta_certifies_against_real_group_history() {
        let prior = memo(4);
        let proposed = memo(4);
        let mut rows = SignedBound::default();
        rows.add_raw(i256::ONE).unwrap();
        rows.add_raw(-i256::ONE).unwrap();
        let mut delta = SignedBound::default();
        delta.add_raw(i256::ONE).unwrap();
        delta.add_raw(-i256::ONE).unwrap();
        let old = GroupEntry {
            rows: 4,
            values: vec![AggValue::Count(4)],
        };
        let bounds = GroupPrefixBounds {
            rows,
            metrics: vec![delta],
            sum_abs_delta: vec![],
        };
        assert_eq!(
            certify_group(
                Some(&old),
                prior.group(&[7]),
                proposed.group(&[7]),
                &bounds,
                &[MetricBinding {
                    kind: BoundKind::Count,
                    memo_index: 0,
                    aggregate_index: 0,
                    nn_memo_index: None,
                }]
            ),
            CertificateDecision::Certified
        );
    }

    #[test]
    fn absent_authoritative_group_certifies_positive_append_from_zero_lower_bound() {
        let proposed = memo(2);
        let mut rows = SignedBound::default();
        rows.add_raw(i256::from_i128(2)).unwrap();
        let mut delta = SignedBound::default();
        delta.add_raw(i256::from_i128(2)).unwrap();
        let bounds = GroupPrefixBounds {
            rows,
            metrics: vec![delta],
            sum_abs_delta: vec![],
        };
        assert_eq!(
            certify_group(
                None,
                None,
                proposed.group(&[7]),
                &bounds,
                &[MetricBinding {
                    kind: BoundKind::Count,
                    memo_index: 0,
                    aggregate_index: 0,
                    nn_memo_index: None,
                }]
            ),
            CertificateDecision::Certified
        );
    }

    #[test]
    fn mixed_metric_bindings_certify_sum_right_then_reject_sum_left_render_overflow() {
        let limits = [
            i128::MAX,
            i64::MAX as i128,
            i64::MAX as i128,
            i128::MAX,
            i64::MAX as i128,
            i64::MAX as i128,
        ];
        let mut old_memo = GroupMemo::default();
        let contribution = |values: [i128; 6]| {
            Contribution::new(
                vec![7],
                values
                    .into_iter()
                    .zip(limits)
                    .map(|(value, limit)| {
                        MetricContribution::exact_with_limit(
                            i256::from_i128(value),
                            abs_i256(i256::from_i128(value)).unwrap(),
                            i256::from_i128(limit),
                        )
                    })
                    .collect(),
            )
        };
        old_memo
            .replace(vec![1], contribution([13, 2, 1, i64::MAX as i128, 2, 2]))
            .unwrap();
        let mut safe_proposed = GroupMemo::default();
        safe_proposed
            .replace(vec![1], contribution([14, 2, 1, i64::MAX as i128, 2, 3]))
            .unwrap();
        let mut overflowing_proposed = GroupMemo::default();
        overflowing_proposed
            .replace(
                vec![1],
                contribution([14, 2, 1, i64::MAX as i128 + 1, 2, 3]),
            )
            .unwrap();
        let old = GroupEntry {
            rows: 2,
            values: vec![
                AggValue::SumInteger { sum: 13, count: 2 },
                AggValue::Count(1),
                AggValue::SumInteger {
                    sum: i64::MAX as i128,
                    count: 2,
                },
                AggValue::Count(2),
            ],
        };
        let mut row = SignedBound::default();
        row.add_raw(i256::ONE).unwrap();
        let mut count = SignedBound::default();
        count.add_raw(i256::ONE).unwrap();
        let bounds = GroupPrefixBounds {
            rows: row,
            metrics: vec![
                SignedBound::default(),
                SignedBound::default(),
                SignedBound::default(),
                SignedBound::default(),
                SignedBound::default(),
                count,
            ],
            sum_abs_delta: vec![(0, i256::ONE), (3, i256::ONE)],
        };
        let bindings = [
            MetricBinding {
                kind: BoundKind::IntegerSum,
                memo_index: 0,
                aggregate_index: 0,
                nn_memo_index: Some(1),
            },
            MetricBinding {
                kind: BoundKind::NullableCount,
                memo_index: 2,
                aggregate_index: 1,
                nn_memo_index: None,
            },
            MetricBinding {
                kind: BoundKind::IntegerSum,
                memo_index: 3,
                aggregate_index: 2,
                nn_memo_index: Some(4),
            },
            MetricBinding {
                kind: BoundKind::Count,
                memo_index: 5,
                aggregate_index: 3,
                nn_memo_index: None,
            },
        ];
        assert_eq!(
            certify_group(
                Some(&old),
                old_memo.group(&[7]),
                safe_proposed.group(&[7]),
                &bounds,
                &bindings
            ),
            CertificateDecision::Certified
        );
        assert_eq!(
            certify_group(
                Some(&old),
                old_memo.group(&[7]),
                overflowing_proposed.group(&[7]),
                &bounds,
                &bindings
            ),
            CertificateDecision::NeedsRowEvaluation
        );
    }

    #[test]
    fn sparse_full_key_bounds_collapse_into_one_requested_group() {
        let delta = |positive| KeyNumericBounds {
            rows: Some(super::super::raw_bounds::JoinDeltaBounds {
                positive: i256::from_i128(positive),
                negative: i256::ZERO,
            }),
            metrics: vec![super::super::raw_bounds::JoinDeltaBounds {
                positive: i256::from_i128(positive),
                negative: i256::ZERO,
            }],
            sum_abs_delta: vec![None],
        };
        let groups =
            aggregate_group_bounds(vec![(vec![7], delta(2)), (vec![7], delta(3))]).unwrap();
        let group = groups.get(&vec![7]).unwrap();
        assert_eq!(group.rows.positive, i256::from_i128(5));
        assert_eq!(group.metrics[0].positive, i256::from_i128(5));
        assert_eq!(groups.len(), 1);
    }

    #[test]
    fn proposed_source_negative_row_support_forces_dense_despite_positive_net_count() {
        use crate::arrange::source_stats::SourceStats;
        let left_one = SourceStats::default()
            .stage_weight_change(0, 1, &[])
            .unwrap();
        let left_two = left_one.stage_weight_change(0, 2, &[]).unwrap();
        let proposed_left = left_two.stage_weight_change(1, -1, &[]).unwrap();
        let right = SourceStats::default()
            .stage_weight_change(0, 1, &[])
            .unwrap();
        assert_eq!(proposed_left.weight, 1);
        assert_eq!(proposed_left.negative_support, 1);

        let demands = [super::super::group_metrics::JoinMetricDemand {
            left: super::super::group_metrics::SourceMeasure::Weight,
            right: super::super::group_metrics::SourceMeasure::Weight,
            limit: i256::from_i128(i64::MAX as i128),
        }];
        let old_contribution =
            Contribution::from_source_stats(vec![7], &left_two, &right, &demands).unwrap();
        let mut prior = GroupMemo::default();
        prior.replace(vec![1], old_contribution).unwrap();
        let proposed_contribution =
            Contribution::from_source_stats(vec![7], &proposed_left, &right, &demands).unwrap();
        let stage = prior
            .stage_updates([(vec![1], Some(proposed_contribution))])
            .unwrap();
        let mut proposed = prior.clone();
        proposed.commit(stage);
        let old = GroupEntry {
            rows: 3,
            values: vec![AggValue::Count(3)],
        };
        let mut rows = SignedBound::default();
        rows.add_raw(-i256::from_i128(2)).unwrap();
        let mut count = SignedBound::default();
        count.add_raw(-i256::from_i128(2)).unwrap();
        let bounds = GroupPrefixBounds {
            rows,
            metrics: vec![count],
            sum_abs_delta: vec![],
        };
        assert_eq!(
            certify_group(
                Some(&old),
                prior.group(&[7]),
                proposed.group(&[7]),
                &bounds,
                &[MetricBinding {
                    kind: BoundKind::Count,
                    memo_index: 0,
                    aggregate_index: 0,
                    nn_memo_index: None,
                }]
            ),
            CertificateDecision::NeedsRowEvaluation
        );
    }
}
