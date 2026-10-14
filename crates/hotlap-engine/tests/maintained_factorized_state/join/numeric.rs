use std::collections::{HashMap, HashSet};

use arrow::datatypes::i256;

use super::{Change, Contribution, JoinGate, JoinView, Overlay, Relation, Summary};

#[derive(Clone, Copy, Debug, Default)]
struct DeltaAbs {
    rows: i256,
    nonnull: i256,
    sum: i256,
}

fn unsigned(value: u128) -> Option<i256> {
    value.to_string().parse().ok()
}

fn magnitude(value: i256) -> i256 {
    if value < i256::ZERO {
        i256::ZERO - value
    } else {
        value
    }
}

fn raw_abs(changes: &[Change]) -> Option<HashMap<i64, DeltaAbs>> {
    let mut out = HashMap::<i64, DeltaAbs>::new();
    for change in changes {
        let weight = i256::from_i128(i128::from(change.delta.unsigned_abs()));
        let slot = out.entry(change.id.0).or_default();
        slot.rows = slot.rows.checked_add(weight)?;
        if let Some(value) = change.value {
            slot.nonnull = slot.nonnull.checked_add(weight)?;
            let term =
                i256::from_i128(i128::from(value).unsigned_abs() as i128).checked_mul(weight)?;
            slot.sum = slot.sum.checked_add(term)?;
        }
    }
    Some(out)
}

fn source<'a>(relation: &'a Relation, overlay: &'a Overlay, key: i64) -> Option<&'a Summary> {
    if let Some(summary) = overlay.summaries.get(&key) {
        return (overlay.support.get(&key).copied().unwrap_or_default() != 0).then_some(summary);
    }
    relation.by_key.get(&key)
}

fn cross_bound(delta_a: i256, old_b: i256, old_a: i256, delta_b: i256) -> Option<i256> {
    delta_a
        .checked_mul(old_b)?
        .checked_add(old_a.checked_mul(delta_b)?)?
        .checked_add(delta_a.checked_mul(delta_b)?)
}

fn add_bound(target: &mut Contribution, value: Contribution) -> Option<()> {
    target.count = target.count.checked_add(value.count)?;
    target.left_nonnull = target.left_nonnull.checked_add(value.left_nonnull)?;
    target.left_sum = target.left_sum.checked_add(value.left_sum)?;
    target.right_nonnull = target.right_nonnull.checked_add(value.right_nonnull)?;
    target.right_sum = target.right_sum.checked_add(value.right_sum)?;
    Some(())
}

fn count_fits(
    old: i256,
    delta_abs: i256,
    max: i256,
    increase_only: bool,
    decrease_only: bool,
) -> bool {
    old >= i256::ZERO
        && old <= max
        && (decrease_only || old.checked_add(delta_abs).is_some_and(|value| value <= max))
        && (increase_only
            || old
                .checked_sub(delta_abs)
                .is_some_and(|value| value >= i256::ZERO))
}

fn sum_fits(old: i256, delta_abs: i256, max: i256) -> bool {
    magnitude(old)
        .checked_add(delta_abs)
        .is_some_and(|bound| bound <= max)
}

pub(super) fn classify(
    view: &JoinView,
    left_changes: &[Change],
    right_changes: &[Change],
    left_overlay: &Overlay,
    right_overlay: &Overlay,
    touched: &HashSet<i64>,
) -> JoinGate {
    if left_overlay.negative_prefix || right_overlay.negative_prefix {
        return JoinGate::NeedsRowEvaluation;
    }
    let Some(left_delta) = raw_abs(left_changes) else {
        return JoinGate::NeedsRowEvaluation;
    };
    let Some(right_delta) = raw_abs(right_changes) else {
        return JoinGate::NeedsRowEvaluation;
    };
    let increase_only = left_changes
        .iter()
        .chain(right_changes)
        .all(|change| change.delta >= 0);
    let decrease_only = left_changes
        .iter()
        .chain(right_changes)
        .all(|change| change.delta <= 0);
    let mut group_bounds = HashMap::<i64, Contribution>::new();
    for &key in touched {
        let old_left = view.left.by_key.get(&key);
        let old_right = view.right.by_key.get(&key);
        let new_left = source(&view.left, left_overlay, key);
        let new_right = source(&view.right, right_overlay, key);
        if old_left.is_some_and(|state| state.negative_rows != 0)
            || old_right.is_some_and(|state| state.negative_rows != 0)
            || new_left.is_some_and(|state| state.negative_rows != 0)
            || new_right.is_some_and(|state| state.negative_rows != 0)
        {
            return JoinGate::NeedsRowEvaluation;
        }

        let dl = left_delta.get(&key).copied().unwrap_or_default();
        let dr = right_delta.get(&key).copied().unwrap_or_default();
        let lw = old_left.map_or(Some(i256::ZERO), |state| unsigned(state.abs_weight));
        let rw = old_right.map_or(Some(i256::ZERO), |state| unsigned(state.abs_weight));
        let ln = old_left.map_or(Some(i256::ZERO), |state| unsigned(state.abs_nonnull));
        let rn = old_right.map_or(Some(i256::ZERO), |state| unsigned(state.abs_nonnull));
        let ls = old_left.map_or(Some(i256::ZERO), |state| Some(state.abs_sum));
        let rs = old_right.map_or(Some(i256::ZERO), |state| Some(state.abs_sum));
        let (Some(lw), Some(rw), Some(ln), Some(rn), Some(ls), Some(rs)) = (lw, rw, ln, rn, ls, rs)
        else {
            return JoinGate::WideBoundsUnavailable;
        };
        let bounds = Contribution {
            count: match cross_bound(dl.rows, rw, lw, dr.rows) {
                Some(value) => value,
                None => return JoinGate::WideBoundsUnavailable,
            },
            left_nonnull: match cross_bound(dl.nonnull, rw, ln, dr.rows) {
                Some(value) => value,
                None => return JoinGate::WideBoundsUnavailable,
            },
            left_sum: match cross_bound(dl.sum, rw, ls, dr.rows) {
                Some(value) => value,
                None => return JoinGate::WideBoundsUnavailable,
            },
            right_nonnull: match cross_bound(dr.nonnull, lw, rn, dl.rows) {
                Some(value) => value,
                None => return JoinGate::WideBoundsUnavailable,
            },
            right_sum: match cross_bound(dr.sum, lw, rs, dl.rows) {
                Some(value) => value,
                None => return JoinGate::WideBoundsUnavailable,
            },
        };
        let group = *view.group_for_join_key.get(&key).unwrap_or(&key);
        let target = group_bounds.entry(group).or_default();
        if add_bound(target, bounds).is_none() {
            return JoinGate::WideBoundsUnavailable;
        }
    }

    let count_max = i256::from_i128(i128::from(i64::MAX));
    let sum_max = i256::from_i128(i128::from(i64::MAX));
    for (group, bound) in group_bounds {
        let old = view.by_group.get(&group).cloned().unwrap_or_default();
        if !count_fits(
            old.count,
            bound.count,
            count_max,
            increase_only,
            decrease_only,
        ) || !count_fits(
            old.left_nonnull,
            bound.left_nonnull,
            count_max,
            increase_only,
            decrease_only,
        ) || !count_fits(
            old.right_nonnull,
            bound.right_nonnull,
            count_max,
            increase_only,
            decrease_only,
        ) || !sum_fits(old.left_sum, bound.left_sum, sum_max)
            || !sum_fits(old.right_sum, bound.right_sum, sum_max)
        {
            return JoinGate::NeedsRowEvaluation;
        }
    }
    JoinGate::Certified
}
