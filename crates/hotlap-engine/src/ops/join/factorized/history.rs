use std::collections::{HashMap, HashSet};

use arrow::row::{RowConverter, SortField};
use hotlap_core::plan::{AggFunc, AggSpec};
use hotlap_core::plan_normalize::ColumnMapping;
use hotlap_core::snapshot::{AggValue, GroupEntry};

use crate::arrange::KeyedArrangement;
use crate::keys::KeyConverter;

use super::groups::{exact_f64_integer, exact_i128_f64};
use super::prepare::{DeltaPair, DenseReason, bucket_for, combine_bucket, group_key_for};
use super::stats::{GroupAbsBounds, Stats, summarize};

pub(super) fn validate_old_group_state(
    mapping: &ColumnMapping,
    aggs: &[AggSpec],
    touched: &HashSet<Vec<u8>>,
    groups: &[(Vec<u8>, GroupEntry)],
    left: &KeyedArrangement,
    right: &KeyedArrangement,
    inputs: &DeltaPair,
) -> Result<GroupAbsBounds, DenseReason> {
    let join = RowConverter::new(
        mapping
            .join_key_types
            .iter()
            .cloned()
            .map(SortField::new)
            .collect(),
    )
    .map_err(|_| DenseReason::InputMapping)?;
    let group_types = mapping
        .group_key
        .iter()
        .map(|&i| {
            mapping
                .join_key_types
                .get(i)
                .cloned()
                .ok_or(DenseReason::InputMapping)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let group = RowConverter::new(group_types.into_iter().map(SortField::new).collect())
        .map_err(|_| DenseReason::InputMapping)?;
    let affected = touched
        .iter()
        .map(|key| {
            group_key_for(&join, &group, key, &mapping.group_key)
                .map_err(|_| DenseReason::InputMapping)
        })
        .collect::<Result<HashSet<_>, _>>()?;
    let left_keys = KeyConverter::new(inputs.left.schema().as_ref(), &mapping.left_join_key)
        .map_err(|_| DenseReason::InputMapping)?;
    let right_keys = KeyConverter::new(inputs.right.schema().as_ref(), &mapping.right_join_key)
        .map_err(|_| DenseReason::InputMapping)?;
    let mut keys: HashSet<Vec<u8>> = left.iter().map(|(key, _, _)| key).collect();
    keys.extend(right.iter().map(|(key, _, _)| key));
    keys.extend(touched.iter().cloned());
    let width = left.schema().fields().len();
    let mut derived = HashMap::<Vec<u8>, (i128, Vec<Stats>)>::new();
    let mut proposed_abs = HashMap::<Vec<u8>, Vec<(u128, u128)>>::new();
    for key in keys {
        let l = left
            .to_zset_for_key(&key)
            .map_err(|_| DenseReason::InputMapping)?;
        let r = right
            .to_zset_for_key(&key)
            .map_err(|_| DenseReason::InputMapping)?;
        let ls =
            summarize(&l, 0, aggs, mapping, width).map_err(|_| DenseReason::UnsupportedNumeric)?;
        let rs =
            summarize(&r, 1, aggs, mapping, width).map_err(|_| DenseReason::UnsupportedNumeric)?;
        let (new_l, new_r) = if touched.contains(&key) {
            let dl = bucket_for(&inputs.left, &left_keys, &key)
                .map_err(|_| DenseReason::InputMapping)?;
            let dr = bucket_for(&inputs.right, &right_keys, &key)
                .map_err(|_| DenseReason::InputMapping)?;
            (
                combine_bucket(&l, &dl).map_err(|_| DenseReason::StateShape)?,
                combine_bucket(&r, &dr).map_err(|_| DenseReason::StateShape)?,
            )
        } else {
            (l.clone(), r.clone())
        };
        let new_ls = summarize(&new_l, 0, aggs, mapping, width)
            .map_err(|_| DenseReason::UnsupportedNumeric)?;
        let new_rs = summarize(&new_r, 1, aggs, mapping, width)
            .map_err(|_| DenseReason::UnsupportedNumeric)?;
        let row_count = ls[0]
            .rows
            .checked_mul(rs[0].rows)
            .ok_or(DenseReason::NegativeOrOverflow)?;
        let group_key = group_key_for(&join, &group, &key, &mapping.group_key)
            .map_err(|_| DenseReason::InputMapping)?;
        let entry = derived
            .entry(group_key.clone())
            .or_insert_with(|| (0, vec![Stats::default(); aggs.len()]));
        let abs_entry = proposed_abs
            .entry(group_key.clone())
            .or_insert_with(|| vec![(0, 0); aggs.len()]);
        entry.0 = entry
            .0
            .checked_add(row_count)
            .ok_or(DenseReason::NegativeOrOverflow)?;
        for (i, agg) in aggs.iter().enumerate() {
            if agg.input.is_none() {
                entry.1[i].rows = entry.1[i]
                    .rows
                    .checked_add(row_count)
                    .ok_or(DenseReason::NegativeOrOverflow)?;
                continue;
            }
            let (new_value, new_other) = if agg.input.unwrap() < width {
                (&new_ls[i], &new_rs[i])
            } else {
                (&new_rs[i], &new_ls[i])
            };
            let abs_count = new_value
                .abs_nonnull
                .checked_mul(new_other.abs_rows)
                .ok_or(DenseReason::NegativeOrOverflow)?;
            abs_entry[i].1 = abs_entry[i]
                .1
                .checked_add(abs_count)
                .ok_or(DenseReason::NegativeOrOverflow)?;
            if matches!(agg.func, AggFunc::Sum | AggFunc::Avg) {
                let abs_sum = new_value
                    .abs_sum
                    .checked_mul(new_other.abs_rows)
                    .ok_or(DenseReason::NegativeOrOverflow)?;
                abs_entry[i].0 = abs_entry[i]
                    .0
                    .checked_add(abs_sum)
                    .ok_or(DenseReason::NegativeOrOverflow)?;
            }
            let (value, other) = if agg.input.unwrap() < width {
                (&ls[i], &rs[i])
            } else {
                (&rs[i], &ls[i])
            };
            let count = value
                .nonnull
                .checked_mul(other.rows)
                .ok_or(DenseReason::NegativeOrOverflow)?;
            match agg.func {
                AggFunc::Count => {
                    entry.1[i].nonnull = entry.1[i]
                        .nonnull
                        .checked_add(count)
                        .ok_or(DenseReason::NegativeOrOverflow)?
                }
                AggFunc::Sum | AggFunc::Avg => {
                    let contribution = value
                        .sum
                        .checked_mul(other.rows)
                        .ok_or(DenseReason::NegativeOrOverflow)?;
                    entry.1[i].sum = entry.1[i]
                        .sum
                        .checked_add(contribution)
                        .ok_or(DenseReason::NegativeOrOverflow)?;
                    let absolute_contribution = value
                        .abs_sum
                        .checked_mul(other.abs_rows)
                        .ok_or(DenseReason::NegativeOrOverflow)?;
                    entry.1[i].abs_sum = entry.1[i]
                        .abs_sum
                        .checked_add(absolute_contribution)
                        .ok_or(DenseReason::NegativeOrOverflow)?;
                    entry.1[i].nonnull = entry.1[i]
                        .nonnull
                        .checked_add(count)
                        .ok_or(DenseReason::NegativeOrOverflow)?;
                    entry.1[i].abs_nonnull = entry.1[i]
                        .abs_nonnull
                        .checked_add(count.unsigned_abs())
                        .ok_or(DenseReason::NegativeOrOverflow)?;
                }
                _ => return Err(DenseReason::UnsupportedNumeric),
            }
        }
    }
    let mut validate_keys = affected;
    validate_keys.extend(groups.iter().map(|(key, _)| key.clone()));
    validate_keys.extend(derived.keys().cloned());
    for key in validate_keys {
        let expected = derived.get(&key);
        let rows = expected.map_or(0, |value| value.0);
        let prior = groups
            .iter()
            .find(|(group_key, _)| *group_key == key)
            .map(|(_, entry)| entry);
        if rows == 0 {
            if prior.is_some() {
                return Err(DenseReason::StateShape);
            }
            continue;
        }
        let prior = prior.ok_or(DenseReason::StateShape)?;
        if i64::try_from(rows).ok() != Some(prior.rows) {
            return Err(DenseReason::StateShape);
        }
        let expected = &expected.ok_or(DenseReason::StateShape)?.1;
        for (i, agg) in aggs.iter().enumerate() {
            let actual = prior.values.get(i).ok_or(DenseReason::StateShape)?;
            match (agg.func, agg.input, actual) {
                (AggFunc::Count, None, AggValue::Count(value))
                    if i128::from(*value) == expected[i].rows => {}
                (AggFunc::Count, Some(_), AggValue::Count(value))
                    if i128::from(*value) == expected[i].nonnull => {}
                (AggFunc::Sum, Some(_), AggValue::SumInteger { sum, count })
                    if *sum == expected[i].sum
                        && i128::from(*count) == expected[i].nonnull
                        && i64::try_from(*sum).is_ok() => {}
                (
                    AggFunc::Avg,
                    Some(_),
                    AggValue::Avg {
                        sum,
                        count,
                        nan,
                        pos_inf,
                        neg_inf,
                    },
                ) => {
                    let derived_sum = expected[i].sum;
                    let derived_count = expected[i].nonnull;
                    if derived_sum.unsigned_abs() > (1_u128 << 53)
                        || expected[i].abs_sum > (1_u128 << 53)
                        || expected[i].abs_nonnull > (1_u128 << 53)
                        || derived_count.unsigned_abs() > (1_u128 << 53)
                        || !exact_i128_f64(derived_sum)
                        || i128::from(*count) != derived_count
                        || *nan != 0
                        || *pos_inf != 0
                        || *neg_inf != 0
                        || !exact_f64_integer(*sum)
                        || sum.to_bits() != (derived_sum as f64).to_bits()
                    {
                        return Err(DenseReason::UnsupportedNumeric);
                    }
                }
                _ => return Err(DenseReason::StateShape),
            }
        }
    }
    Ok(proposed_abs)
}
