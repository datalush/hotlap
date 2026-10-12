use std::collections::HashMap;

use hotlap_core::plan::{AggFunc, AggSpec};
use hotlap_core::plan_normalize::ColumnMapping;
use hotlap_core::snapshot::{AggValue, GroupEntry};

use super::prepare::GroupChange;
use super::stats::Stats;

pub(super) fn aggregate_groups(
    mapping: &ColumnMapping,
    aggs: &[AggSpec],
    old: &[(Vec<u8>, GroupEntry)],
    deltas: &HashMap<Vec<u8>, Vec<Stats>>,
    groups: &HashMap<Vec<u8>, Vec<u8>>,
    row_deltas: &HashMap<Vec<u8>, (i128, u128)>,
    state_abs_bounds: &HashMap<Vec<u8>, Vec<(u128, u128)>>,
) -> Option<Vec<GroupChange>> {
    let mut grouped = HashMap::<Vec<u8>, Vec<Stats>>::new();
    let mut group_rows = HashMap::<Vec<u8>, (i128, u128)>::new();
    for (join, values) in deltas {
        let group_key = groups.get(join)?.clone();
        grouped
            .entry(group_key.clone())
            .or_insert_with(|| vec![Stats::default(); aggs.len()])
            .iter_mut()
            .zip(values)
            .try_for_each(|(a, b)| a.checked_add(b))?;
        let (delta, bound) = row_deltas.get(join)?;
        let prior = group_rows.entry(group_key).or_default();
        prior.0 = prior.0.checked_add(*delta)?;
        prior.1 = prior.1.checked_add(*bound)?;
    }
    let mut result = Vec::new();
    for (key, ds) in grouped {
        let prior = old.iter().find(|(k, _)| *k == key).map(|(_, v)| v);
        let (rows_delta, rows_bound) = group_rows.get(&key).copied()?;
        let prior_rows = prior.map_or(0, |v| v.rows);
        if u128::from(prior_rows.unsigned_abs()).checked_add(rows_bound)? > i64::MAX as u128 {
            return None;
        }
        let rows = prior
            .map_or(0, |v| v.rows)
            .checked_add(i64::try_from(rows_delta).ok()?)?;
        if rows < 0 {
            return None;
        }
        let mut values = Vec::with_capacity(aggs.len());
        for (i, agg) in aggs.iter().enumerate() {
            let oldval = prior.and_then(|p| p.values.get(i));
            let d = ds[i].rows;
            let count_delta = i64::try_from(ds[i].nonnull).ok()?;
            match (agg.func, agg.input, oldval) {
                (AggFunc::Count, None, Some(AggValue::Count(n))) => {
                    if u128::from(n.unsigned_abs()).checked_add(ds[i].abs_rows)? > i64::MAX as u128
                    {
                        return None;
                    }
                    values.push(AggValue::Count(n.checked_add(i64::try_from(d).ok()?)?))
                }
                (AggFunc::Count, Some(_), Some(AggValue::Count(n))) => {
                    if u128::from(n.unsigned_abs()).checked_add(ds[i].abs_nonnull)?
                        > i64::MAX as u128
                    {
                        return None;
                    }
                    values.push(AggValue::Count(n.checked_add(i64::try_from(d).ok()?)?))
                }
                (AggFunc::Sum, Some(_), Some(AggValue::SumInteger { sum, count })) => {
                    if sum.unsigned_abs().checked_add(ds[i].abs_sum)? > i128::MAX as u128
                        || u128::from(count.unsigned_abs()).checked_add(ds[i].abs_nonnull)?
                            > i64::MAX as u128
                    {
                        return None;
                    }
                    let next_sum = sum.checked_add(ds[i].sum)?;
                    if i64::try_from(next_sum).is_err() {
                        return None;
                    }
                    values.push(AggValue::SumInteger {
                        sum: next_sum,
                        count: count.checked_add(count_delta)?,
                    })
                }
                (
                    AggFunc::Avg,
                    Some(_),
                    Some(AggValue::Avg {
                        sum,
                        count,
                        nan,
                        pos_inf,
                        neg_inf,
                    }),
                ) => {
                    let (state_abs_sum, state_abs_count) = *state_abs_bounds.get(&key)?.get(i)?;
                    if state_abs_sum > (1_u128 << 53)
                        || state_abs_count > (1_u128 << 53)
                        || *nan != 0
                        || *pos_inf != 0
                        || *neg_inf != 0
                        || !exact_f64_integer(*sum)
                    {
                        return None;
                    }
                    let old_sum = *sum as i128;
                    if old_sum.unsigned_abs().checked_add(ds[i].abs_sum)? > (1_u128 << 53)
                        || u128::from(count.unsigned_abs()).checked_add(ds[i].abs_nonnull)?
                            > (1_u128 << 53)
                    {
                        return None;
                    }
                    let next_sum = old_sum.checked_add(ds[i].sum)?;
                    let next_count = count.checked_add(count_delta)?;
                    if next_sum.unsigned_abs() > (1_u128 << 53)
                        || next_count.unsigned_abs() > (1_i64 << 53) as u64
                        || !exact_i128_f64(next_sum)
                    {
                        return None;
                    }
                    values.push(AggValue::Avg {
                        sum: next_sum as f64,
                        count: next_count,
                        nan: 0,
                        pos_inf: 0,
                        neg_inf: 0,
                    });
                }
                (AggFunc::Count, None, None) => {
                    values.push(AggValue::Count(i64::try_from(d).ok()?))
                }
                (AggFunc::Count, Some(_), None) => values.push(AggValue::Count(count_delta)),
                (AggFunc::Sum, Some(_), None) => {
                    let sum = ds[i].sum;
                    if i64::try_from(sum).is_err() || ds[i].abs_sum > i128::MAX as u128 {
                        return None;
                    }
                    values.push(AggValue::SumInteger {
                        sum,
                        count: count_delta,
                    });
                }
                (AggFunc::Avg, Some(_), None) => {
                    let sum = ds[i].sum;
                    let count = count_delta;
                    let (state_abs_sum, state_abs_count) = *state_abs_bounds.get(&key)?.get(i)?;
                    if state_abs_sum > (1_u128 << 53)
                        || state_abs_count > (1_u128 << 53)
                        || sum.unsigned_abs() > (1_u128 << 53)
                        || ds[i].abs_sum > (1_u128 << 53)
                        || ds[i].abs_nonnull > (1_u128 << 53)
                        || count.unsigned_abs() > (1_i64 << 53) as u64
                        || !exact_i128_f64(sum)
                    {
                        return None;
                    }
                    values.push(AggValue::Avg {
                        sum: sum as f64,
                        count,
                        nan: 0,
                        pos_inf: 0,
                        neg_inf: 0,
                    });
                }
                _ => return None,
            }
        }
        if rows == 0 {
            result.push(GroupChange::Remove { key });
            continue;
        }
        if values.len() != mapping.aggregate_inputs.len() {
            return None;
        }
        result.push(GroupChange::Upsert {
            key,
            entry: GroupEntry { rows, values },
        });
    }
    Some(result)
}

pub(super) fn exact_f64_integer(value: f64) -> bool {
    value.is_finite()
        && value.abs() <= (1_u64 << 53) as f64
        && (value as i128) as f64 == value
        && !(value == 0.0 && value.is_sign_negative())
}

pub(super) fn exact_i128_f64(value: i128) -> bool {
    value.unsigned_abs() <= (1_u128 << 53) && (value as f64) as i128 == value
}
