//! Conservative, bitwise-preserving eligibility checks specific to integer AVG.

use arrow::datatypes::i256;
use hotlap_core::snapshot::{AggValue, GroupEntry};

use super::certificate::{GroupPrefixBounds, MetricBinding};
use super::group_memo::{GroupContribution, GroupMetric, MetricLimit};

pub(super) fn validate_old_avg_history(
    old: &GroupEntry,
    binding: MetricBinding,
    metric: &GroupMetric,
    memo: &GroupContribution,
    bounds: &GroupPrefixBounds,
    sum: f64,
    count: i64,
) -> bool {
    let Some(nn_index) = binding.nn_memo_index else {
        return false;
    };
    let Some(nn_delta) = bounds.metrics.get(nn_index) else {
        return false;
    };
    let Some(nn) = memo.metrics.get(nn_index) else {
        return false;
    };
    let abs_bound = bounds
        .sum_abs_delta
        .iter()
        .find(|(index, _)| *index == binding.memo_index)
        .and_then(|(_, delta)| metric.abs_subtotal.checked_add(*delta));
    let abs_rows = bounds
        .rows
        .positive
        .checked_add(bounds.rows.negative)
        .and_then(|delta| delta.checked_add(i256::from_i128(i128::from(old.rows))));
    sum.to_bits()
        == (metric
            .subtotal
            .to_string()
            .parse::<i128>()
            .unwrap_or(i128::MIN) as f64)
            .to_bits()
        && abs_i256(metric.subtotal).is_some_and(|value| {
            value <= i256::from_i128(super::group_metrics::AVG_EXACT_F64_INTEGER_LIMIT)
        })
        && abs_rows.is_some_and(|value| {
            value <= i256::from_i128(super::group_metrics::AVG_EXACT_F64_INTEGER_LIMIT)
        })
        && count >= 0
        && count <= old.rows
        && nn.eligibility == MetricLimit::Exact
        && nn.subtotal == i256::from_i128(i128::from(count))
        && nn_delta.within_prefixes(nn.subtotal, i256::from_i128(i64::MAX as i128))
        && nn_delta
            .positive
            .checked_add(nn.subtotal)
            .and_then(|value| value.checked_sub(nn_delta.negative))
            == Some(i256::ZERO)
        && abs_bound.is_some_and(|bound| {
            bound <= i256::from_i128(super::group_metrics::AVG_EXACT_F64_INTEGER_LIMIT)
        })
}

pub(super) fn validate_avg_history(
    old: Option<&GroupEntry>,
    binding: MetricBinding,
    previous: &GroupMetric,
    old_memo: Option<&GroupContribution>,
    proposed_memo: &GroupContribution,
    bounds: &GroupPrefixBounds,
) -> bool {
    let Some(nn_index) = binding.nn_memo_index else {
        return false;
    };
    let zero = GroupMetric::default();
    let old_nn = old_memo
        .and_then(|memo| memo.metrics.get(nn_index))
        .unwrap_or(&zero);
    let (Some(next), Some(next_nn), Some(nn_bound), Some(sum_delta)) = (
        proposed_memo.metrics.get(binding.memo_index),
        proposed_memo.metrics.get(nn_index),
        bounds.metrics.get(nn_index),
        bounds
            .sum_abs_delta
            .iter()
            .find(|(index, _)| *index == binding.memo_index)
            .map(|(_, value)| value),
    ) else {
        return false;
    };
    let old_value = old.and_then(|entry| entry.values.get(binding.aggregate_index));
    let history_matches = match (old, old_value) {
        (None, None) => previous.subtotal == i256::ZERO && old_nn.subtotal == i256::ZERO,
        (
            Some(entry),
            Some(AggValue::Avg {
                sum,
                count,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            }),
        ) => previous
            .subtotal
            .to_string()
            .parse::<i128>()
            .is_ok_and(|integer| {
                sum.to_bits() == (integer as f64).to_bits()
                    && previous.abs_subtotal
                        <= i256::from_i128(super::group_metrics::AVG_EXACT_F64_INTEGER_LIMIT)
                    && *count >= 0
                    && *count <= entry.rows
                    && old_nn.subtotal == i256::from_i128(i128::from(*count))
            }),
        _ => false,
    };
    if !history_matches
        || old_nn.eligibility != MetricLimit::Exact
        || next_nn.eligibility != MetricLimit::Exact
        || !nn_bound.within_prefixes(old_nn.subtotal, i256::from_i128(i64::MAX as i128))
        || nn_bound
            .positive
            .checked_add(old_nn.subtotal)
            .and_then(|value| value.checked_sub(nn_bound.negative))
            != Some(next_nn.subtotal)
        || next_nn.subtotal < i256::ZERO
    {
        return false;
    }
    let old_rows = i256::from_i128(i128::from(old.map_or(0, |entry| entry.rows)));
    let Some(proposed_rows) = bounds
        .rows
        .positive
        .checked_add(old_rows)
        .and_then(|value| value.checked_sub(bounds.rows.negative))
    else {
        return false;
    };
    let Some(abs_rows_bound) = old_rows
        .checked_add(bounds.rows.positive)
        .and_then(|value| value.checked_add(bounds.rows.negative))
    else {
        return false;
    };
    if next_nn.subtotal > proposed_rows
        || abs_rows_bound > i256::from_i128(super::group_metrics::AVG_EXACT_F64_INTEGER_LIMIT)
    {
        return false;
    }
    let Some(actual_delta) = next
        .subtotal
        .checked_sub(previous.subtotal)
        .and_then(abs_i256)
    else {
        return false;
    };
    actual_delta <= *sum_delta
        && previous
            .abs_subtotal
            .checked_add(*sum_delta)
            .is_some_and(|bound| {
                bound <= i256::from_i128(super::group_metrics::AVG_EXACT_F64_INTEGER_LIMIT)
            })
        && next.abs_subtotal <= i256::from_i128(super::group_metrics::AVG_EXACT_F64_INTEGER_LIMIT)
}

fn abs_i256(value: i256) -> Option<i256> {
    if value < i256::ZERO {
        i256::ZERO.checked_sub(value)
    } else {
        Some(value)
    }
}
