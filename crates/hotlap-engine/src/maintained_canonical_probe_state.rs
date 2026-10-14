use arrow::array::{Array, Int64Array};
use arrow::datatypes::i256;
use arrow::row::RowConverter;
use hotlap_core::snapshot::GroupEntry;

use crate::arrange::StagedProbeRow;
use crate::{EngineError, ZSetBatch};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct SourceStats {
    pub(super) support: usize,
    pub(super) weight: i128,
    pub(super) abs_weight: u128,
    pub(super) nonnull: i128,
    pub(super) abs_nonnull: u128,
    pub(super) sum: i256,
    pub(super) abs_sum: i256,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Contribution {
    pub(super) count: i256,
    pub(super) nonnull: i256,
    pub(super) sum: i256,
}

pub(super) struct StagedStats {
    pub(super) by_key: std::collections::HashMap<Vec<u8>, SourceStats>,
    pub(super) touched: std::collections::HashSet<Vec<u8>>,
    pub(super) raw_abs_weight: std::collections::HashMap<Vec<u8>, u128>,
    pub(super) raw_abs_nonnull: std::collections::HashMap<Vec<u8>, u128>,
    pub(super) raw_abs_sum: std::collections::HashMap<Vec<u8>, i256>,
}

pub(super) fn stage_stats(
    delta: &ZSetBatch,
    old_stats: &std::collections::HashMap<Vec<u8>, SourceStats>,
    decoded: &[StagedProbeRow],
) -> Result<StagedStats, EngineError> {
    let diffs = delta
        .diff()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(EngineError::Unsupported("probe requires i64 diffs".into()))?;
    let values = delta
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(EngineError::Unsupported("probe requires i64 values".into()))?;
    let mut staged = std::collections::HashMap::new();
    let mut touched = std::collections::HashSet::new();
    let mut raw_abs = std::collections::HashMap::<Vec<u8>, u128>::new();
    let mut raw_abs_nonnull = std::collections::HashMap::<Vec<u8>, u128>::new();
    let mut raw_abs_sum = std::collections::HashMap::<Vec<u8>, i256>::new();
    let mut sign = 0i8;
    for (row, decoded_row) in decoded.iter().enumerate() {
        let row_sign = diffs.value(row).signum() as i8;
        if row_sign != 0 && sign != 0 && row_sign != sign {
            return Err(EngineError::Unsupported(
                "probe mixed-sign batch requires Dense evaluation".into(),
            ));
        }
        if row_sign != 0 {
            sign = row_sign;
        }
        let key = decoded_row.key.clone();
        touched.insert(key.clone());
        let old = staged
            .get(&key)
            .copied()
            .or_else(|| old_stats.get(&key).copied())
            .unwrap_or_default();
        let old_weight = decoded_row.old_weight;
        let new_weight = decoded_row.new_weight;
        if new_weight < 0 {
            return Err(EngineError::Unsupported(
                "probe negative row weight requires Dense evaluation".into(),
            ));
        }
        let row_abs = raw_abs.entry(key.clone()).or_default();
        *row_abs = row_abs
            .checked_add(u128::from(diffs.value(row).unsigned_abs()))
            .ok_or(EngineError::NumericOverflow("probe raw delta abs"))?;
        let value = values.is_valid(row).then(|| values.value(row));
        if let Some(value) = value {
            let nonnull_total = raw_abs_nonnull.entry(key.clone()).or_default();
            *nonnull_total = nonnull_total
                .checked_add(u128::from(diffs.value(row).unsigned_abs()))
                .ok_or(EngineError::NumericOverflow("probe raw abs nonnull"))?;
            let term = i256::from_string(&value.unsigned_abs().to_string())
                .unwrap()
                .checked_mul(wide(u128::from(diffs.value(row).unsigned_abs())))
                .ok_or(EngineError::NumericOverflow("probe raw abs sum"))?;
            let total = raw_abs_sum.entry(key.clone()).or_default();
            *total = total
                .checked_add(term)
                .ok_or(EngineError::NumericOverflow("probe raw abs sum"))?;
        }
        let weight = old
            .weight
            .checked_add(i128::from(diffs.value(row)))
            .ok_or(EngineError::NumericOverflow("probe stats weight"))?;
        let abs_weight = old
            .abs_weight
            .checked_sub(u128::from(old_weight.unsigned_abs()))
            .and_then(|n| n.checked_add(u128::from(new_weight.unsigned_abs())))
            .ok_or(EngineError::NumericOverflow("probe abs weight"))?;
        let nonnull = old
            .nonnull
            .checked_add(if value.is_some() {
                i128::from(diffs.value(row))
            } else {
                0
            })
            .ok_or(EngineError::NumericOverflow("probe nonnull"))?;
        let abs_nonnull = old
            .abs_nonnull
            .checked_sub(if value.is_some() {
                u128::from(old_weight.unsigned_abs())
            } else {
                0
            })
            .and_then(|n| {
                n.checked_add(if value.is_some() {
                    u128::from(new_weight.unsigned_abs())
                } else {
                    0
                })
            })
            .ok_or(EngineError::NumericOverflow("probe abs nonnull"))?;
        let term = value.map_or(i256::ZERO, |v| {
            i256::from_i128(i128::from(v)) * i256::from_i128(i128::from(diffs.value(row)))
        });
        let abs_term = value.map_or(i256::ZERO, |v| {
            i256::from_string(&v.unsigned_abs().to_string()).unwrap()
                * i256::from_string(&new_weight.unsigned_abs().to_string()).unwrap()
                - i256::from_string(&v.unsigned_abs().to_string()).unwrap()
                    * i256::from_string(&old_weight.unsigned_abs().to_string()).unwrap()
        });
        let sum = old
            .sum
            .checked_add(term)
            .ok_or(EngineError::NumericOverflow("probe sum"))?;
        let abs_sum = old
            .abs_sum
            .checked_add(abs_term)
            .ok_or(EngineError::NumericOverflow("probe abs sum"))?;
        let support = if old_weight == 0 && new_weight != 0 {
            old.support.checked_add(1)
        } else if old_weight != 0 && new_weight == 0 {
            old.support.checked_sub(1)
        } else {
            Some(old.support)
        }
        .ok_or(EngineError::NumericOverflow("probe support count"))?;
        staged.insert(
            key,
            SourceStats {
                support,
                weight,
                abs_weight,
                nonnull,
                abs_nonnull,
                sum,
                abs_sum,
            },
        );
    }
    Ok(StagedStats {
        by_key: staged,
        touched,
        raw_abs_weight: raw_abs,
        raw_abs_nonnull,
        raw_abs_sum,
    })
}

pub(super) fn wide(value: u128) -> i256 {
    i256::from_string(&value.to_string()).expect("u128 fits i256")
}

pub(super) fn batch_sign(batch: &ZSetBatch) -> i8 {
    let Some(diffs) = batch.diff().as_any().downcast_ref::<Int64Array>() else {
        return 0;
    };
    diffs
        .values()
        .iter()
        .find(|&&value| value != 0)
        .map_or(0, |value| value.signum() as i8)
}

pub(super) struct CertificateSources {
    pub(super) left: SourceStats,
    pub(super) right: SourceStats,
}

pub(super) struct RawJoinDeltaBounds {
    pub(super) left_abs: u128,
    pub(super) left_nonnull_abs: u128,
    pub(super) right_abs: u128,
    pub(super) left_sum_abs: i256,
    pub(super) left_sign: i8,
    pub(super) right_sign: i8,
}

pub(super) fn certify(
    old: CertificateSources,
    delta: RawJoinDeltaBounds,
    new: CertificateSources,
) -> Result<(), EngineError> {
    let CertificateSources {
        left: old_left,
        right: old_right,
    } = old;
    let CertificateSources {
        left: new_left,
        right: new_right,
    } = new;
    let RawJoinDeltaBounds {
        left_abs: left_delta_abs,
        left_nonnull_abs: left_delta_nonnull_abs,
        right_abs: right_delta_abs,
        left_sum_abs: left_delta_sum_abs,
        left_sign,
        right_sign,
    } = delta;
    let first = wide(left_delta_abs)
        .checked_mul(wide(old_right.abs_weight))
        .ok_or(EngineError::NumericOverflow("probe count bound"))?;
    let second = wide(old_left.abs_weight)
        .checked_mul(wide(right_delta_abs))
        .ok_or(EngineError::NumericOverflow("probe count bound"))?;
    let cross = wide(left_delta_abs)
        .checked_mul(wide(right_delta_abs))
        .ok_or(EngineError::NumericOverflow("probe count bound"))?;
    let mut positive = i256::ZERO;
    let mut negative = i256::ZERO;
    for (bound, sign) in [
        (first, left_sign),
        (second, right_sign),
        (cross, left_sign * right_sign),
    ] {
        if sign >= 0 {
            positive = positive
                .checked_add(bound)
                .ok_or(EngineError::NumericOverflow("probe count bound"))?;
        } else {
            negative = negative
                .checked_add(bound)
                .ok_or(EngineError::NumericOverflow("probe count bound"))?;
        }
    }
    let old_count = i256::from_i128(old_left.weight)
        .checked_mul(i256::from_i128(old_right.weight))
        .ok_or(EngineError::NumericOverflow("probe old count"))?;
    let old_sum_abs = old_left
        .abs_sum
        .checked_mul(wide(old_right.abs_weight))
        .ok_or(EngineError::NumericOverflow("probe old sum bound"))?;
    let sum_delta_abs = left_delta_sum_abs
        .checked_mul(wide(old_right.abs_weight))
        .and_then(|v| v.checked_add(old_left.abs_sum.checked_mul(wide(right_delta_abs))?))
        .and_then(|v| v.checked_add(left_delta_sum_abs.checked_mul(wide(right_delta_abs))?))
        .ok_or(EngineError::NumericOverflow("probe sum delta bound"))?;
    let sum_prefix_abs = old_sum_abs
        .checked_add(sum_delta_abs)
        .ok_or(EngineError::NumericOverflow("probe sum prefix bound"))?;
    let new_count = i256::from_i128(new_left.weight)
        .checked_mul(i256::from_i128(new_right.weight))
        .ok_or(EngineError::NumericOverflow("probe count"))?;
    let old_nonnull_count = i256::from_i128(old_left.nonnull)
        .checked_mul(i256::from_i128(old_right.weight))
        .ok_or(EngineError::NumericOverflow("probe old nonnull"))?;
    let nonnull_bounds = [
        (
            wide(left_delta_nonnull_abs)
                .checked_mul(wide(old_right.abs_weight))
                .ok_or(EngineError::NumericOverflow("probe nonnull bound"))?,
            left_sign,
        ),
        (
            wide(old_left.abs_nonnull)
                .checked_mul(wide(right_delta_abs))
                .ok_or(EngineError::NumericOverflow("probe nonnull bound"))?,
            right_sign,
        ),
        (
            wide(left_delta_nonnull_abs)
                .checked_mul(wide(right_delta_abs))
                .ok_or(EngineError::NumericOverflow("probe nonnull bound"))?,
            left_sign * right_sign,
        ),
    ];
    let mut nonnull_positive = i256::ZERO;
    let mut nonnull_negative = i256::ZERO;
    for (bound, sign) in nonnull_bounds {
        let target = if sign >= 0 {
            &mut nonnull_positive
        } else {
            &mut nonnull_negative
        };
        *target = target
            .checked_add(bound)
            .ok_or(EngineError::NumericOverflow("probe nonnull bound"))?;
    }
    let new_nonnull = i256::from_i128(new_left.nonnull)
        .checked_mul(i256::from_i128(new_right.weight))
        .ok_or(EngineError::NumericOverflow("probe nonnull"))?;
    if old_left.weight < 0
        || old_right.weight < 0
        || new_left.weight < 0
        || new_right.weight < 0
        || old_count
            .checked_add(positive)
            .is_none_or(|v| v > i256::from_i128(i128::from(i64::MAX)))
        || old_count
            .checked_sub(negative)
            .is_none_or(|v| v < i256::ZERO)
        || new_count < i256::ZERO
        || new_count > i256::from_i128(i128::from(i64::MAX))
        || old_nonnull_count
            .checked_add(nonnull_positive)
            .is_none_or(|v| v > i256::from_i128(i128::from(i64::MAX)))
        || old_nonnull_count
            .checked_sub(nonnull_negative)
            .is_none_or(|v| v < i256::ZERO)
        || new_nonnull < i256::ZERO
        || new_nonnull > i256::from_i128(i128::from(i64::MAX))
        || sum_prefix_abs > i256::from_i128(i128::MAX)
    {
        return Err(EngineError::Unsupported(
            "probe requires Dense row evaluation".into(),
        ));
    }
    Ok(())
}

pub(super) fn contribution(
    left: SourceStats,
    right: SourceStats,
) -> Result<Contribution, EngineError> {
    Ok(Contribution {
        count: i256::from_i128(left.weight)
            .checked_mul(i256::from_i128(right.weight))
            .ok_or(EngineError::NumericOverflow("probe count product"))?,
        nonnull: i256::from_i128(left.nonnull)
            .checked_mul(i256::from_i128(right.weight))
            .ok_or(EngineError::NumericOverflow("probe nonnull product"))?,
        sum: left
            .sum
            .checked_mul(i256::from_i128(right.weight))
            .ok_or(EngineError::NumericOverflow("probe sum product"))?,
    })
}

pub(super) fn add_group(
    groups: &mut std::collections::HashMap<Vec<u8>, GroupEntry>,
    key: &[u8],
    contribution: Contribution,
    sign: i64,
    _converter: &RowConverter,
) -> Result<(), EngineError> {
    let entry = groups.entry(key.to_vec()).or_insert_with(|| GroupEntry {
        rows: 0,
        values: vec![
            hotlap_core::snapshot::AggValue::Count(0),
            hotlap_core::snapshot::AggValue::Count(0),
            hotlap_core::snapshot::AggValue::SumInteger { sum: 0, count: 0 },
        ],
    });
    let (first, rest) = entry.values.split_at_mut(1);
    let hotlap_core::snapshot::AggValue::Count(count) = &mut first[0] else {
        unreachable!()
    };
    let (second, third) = rest.split_at_mut(1);
    let hotlap_core::snapshot::AggValue::Count(nonnull) = &mut second[0] else {
        unreachable!()
    };
    let hotlap_core::snapshot::AggValue::SumInteger {
        sum,
        count: sum_count,
    } = &mut third[0]
    else {
        unreachable!()
    };
    *count = count
        .checked_add(
            contribution
                .count
                .to_string()
                .parse::<i64>()
                .map_err(|_| EngineError::NumericOverflow("probe count"))?
                * sign,
        )
        .ok_or(EngineError::NumericOverflow("probe count"))?;
    *nonnull = nonnull
        .checked_add(
            contribution
                .nonnull
                .to_string()
                .parse::<i64>()
                .map_err(|_| EngineError::NumericOverflow("probe nonnull"))?
                * sign,
        )
        .ok_or(EngineError::NumericOverflow("probe nonnull"))?;
    *sum = sum
        .checked_add(
            contribution
                .sum
                .to_string()
                .parse::<i128>()
                .map_err(|_| EngineError::NumericOverflow("probe sum"))?
                * i128::from(sign),
        )
        .ok_or(EngineError::NumericOverflow("probe sum"))?;
    *sum_count = *nonnull;
    let row_count = *count;
    entry.rows = row_count;
    if *count == 0 {
        groups.remove(key);
    }
    Ok(())
}
