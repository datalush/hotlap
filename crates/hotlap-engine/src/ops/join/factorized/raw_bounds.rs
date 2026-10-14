//! Per-touched-key signed raw-delta bounds used by numeric certification.

use super::group_metrics::{JoinMetricDemand, SourceMeasure};
use crate::{batch::ZSetBatch, error::EngineError, keys::KeyConverter};
use arrow::array::{Array, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow::datatypes::i256;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SignedBound {
    pub(crate) positive: i256,
    pub(crate) negative: i256,
}

impl SignedBound {
    /// Add one original input row before any identity consolidation.
    pub(crate) fn add_raw(&mut self, value: i256) -> Option<()> {
        if value >= i256::ZERO {
            self.positive = self.positive.checked_add(value)?;
        } else {
            self.negative = self.negative.checked_sub(value)?;
        }
        Some(())
    }

    pub(crate) fn within_prefixes(self, old: i256, limit: i256) -> bool {
        old >= i256::ZERO
            && old.checked_add(self.positive).is_some_and(|v| v <= limit)
            && old
                .checked_sub(self.negative)
                .is_some_and(|v| v >= i256::ZERO)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JoinDeltaBounds {
    pub(crate) positive: i256,
    pub(crate) negative: i256,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RawKeyDelta {
    pub(crate) weight: SignedBound,
    pub(crate) nonnull: Vec<SignedBound>,
    pub(crate) sum: Vec<RawSumBound>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RawSumBound {
    pub(crate) signed: SignedBound,
    pub(crate) absolute: i256,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RawColumnDemand {
    pub(crate) index: usize,
    pub(crate) sum: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeyNumericBounds {
    pub(crate) rows: Option<JoinDeltaBounds>,
    pub(crate) metrics: Vec<JoinDeltaBounds>,
    pub(crate) sum_abs_delta: Vec<Option<i256>>,
}

/// Derive the two-sided row/NN prefixes and SUM magnitude bounds for one full
/// canonical key. Negative old physical rows are deliberately ineligible.
pub(crate) fn derive_key_bounds(
    old_left: Option<&crate::arrange::source_stats::SourceStats>,
    old_right: Option<&crate::arrange::source_stats::SourceStats>,
    raw_left: Option<&RawKeyDelta>,
    raw_right: Option<&RawKeyDelta>,
    demands: &[JoinMetricDemand],
) -> Option<KeyNumericBounds> {
    for stats in [old_left, old_right].into_iter().flatten() {
        if stats.negative_support != 0
            || stats.weight < 0
            || stats.abs_weight != stats.weight as u128
            || stats.columns.iter().any(|column| column.nonnull < 0)
        {
            return None;
        }
    }
    let mut metrics = Vec::with_capacity(demands.len());
    let mut sum_abs_delta = Vec::with_capacity(demands.len());
    let old_lw = source_bound(old_left, raw_left, SourceMeasure::Weight)?;
    let old_rw = source_bound(old_right, raw_right, SourceMeasure::Weight)?;
    let rows = Some(join_delta_bounds(
        old_lw.old,
        old_rw.old,
        old_lw.positive,
        old_lw.negative,
        old_rw.positive,
        old_rw.negative,
    )?);
    for demand in demands {
        let left = source_bound(old_left, raw_left, demand.left)?;
        let right = source_bound(old_right, raw_right, demand.right)?;
        if let SourceMeasure::Sum(_) = demand.left {
            let raw_sum = left.sum_abs.unwrap_or(i256::ZERO);
            sum_abs_delta.push(Some(sum_delta_abs_bound(
                raw_sum,
                right.old_abs,
                left.old_abs_sum.unwrap_or(i256::ZERO),
                right.raw_abs_weight,
            )?));
            metrics.push(JoinDeltaBounds {
                positive: i256::ZERO,
                negative: i256::ZERO,
            });
        } else if let SourceMeasure::Sum(_) = demand.right {
            let raw_sum = right.sum_abs.unwrap_or(i256::ZERO);
            sum_abs_delta.push(Some(sum_delta_abs_bound(
                raw_sum,
                left.old_abs,
                right.old_abs_sum.unwrap_or(i256::ZERO),
                left.raw_abs_weight,
            )?));
            metrics.push(JoinDeltaBounds {
                positive: i256::ZERO,
                negative: i256::ZERO,
            });
        } else {
            let joined = join_delta_bounds(
                left.old,
                right.old,
                left.positive,
                left.negative,
                right.positive,
                right.negative,
            )?;
            metrics.push(joined);
            sum_abs_delta.push(None);
        }
    }
    Some(KeyNumericBounds {
        rows,
        metrics,
        sum_abs_delta,
    })
}

#[derive(Clone, Copy)]
struct SourceBound {
    old: i256,
    old_abs: i256,
    positive: i256,
    negative: i256,
    sum_abs: Option<i256>,
    old_abs_sum: Option<i256>,
    raw_abs_weight: i256,
}

fn source_bound(
    stats: Option<&crate::arrange::source_stats::SourceStats>,
    raw: Option<&RawKeyDelta>,
    measure: SourceMeasure,
) -> Option<SourceBound> {
    let (old, old_abs, old_abs_sum) = match (stats, measure) {
        (None, _) => (i256::ZERO, i256::ZERO, None),
        (Some(s), SourceMeasure::Weight) => (
            i256::from_i128(s.weight),
            i256::from_string(&s.abs_weight.to_string())?,
            None,
        ),
        (Some(s), SourceMeasure::NonNull(i)) => (
            i256::from_i128(s.columns.get(i)?.nonnull),
            i256::from_i128(s.columns.get(i)?.nonnull),
            None,
        ),
        (Some(s), SourceMeasure::Sum(i)) => (
            s.columns.get(i)?.sum?,
            s.columns.get(i)?.abs_sum?,
            s.columns.get(i)?.abs_sum,
        ),
    };
    let (positive, negative, sum_abs) = match (raw, measure) {
        (None, _) => (i256::ZERO, i256::ZERO, None),
        (Some(r), SourceMeasure::Weight) => (r.weight.positive, r.weight.negative, None),
        (Some(r), SourceMeasure::NonNull(i)) => {
            let b = *r.nonnull.get(i)?;
            (b.positive, b.negative, None)
        }
        (Some(r), SourceMeasure::Sum(i)) => {
            let b = *r.sum.get(i)?;
            (b.signed.positive, b.signed.negative, Some(b.absolute))
        }
    };
    let raw_abs_weight = raw.map_or(Some(i256::ZERO), |r| {
        r.weight.positive.checked_add(r.weight.negative)
    })?;
    Some(SourceBound {
        old,
        old_abs,
        positive,
        negative,
        sum_abs,
        old_abs_sum,
        raw_abs_weight,
    })
}

/// Reads each original incoming Arrow row exactly once, before Z-set identity
/// consolidation, and aggregates only scalar bounds by canonical join key.
pub(crate) fn collect_raw_delta(
    batch: &ZSetBatch,
    key_indices: &[usize],
    columns: &[RawColumnDemand],
) -> Result<HashMap<Vec<u8>, RawKeyDelta>, EngineError> {
    let key_converter = KeyConverter::new(batch.schema().as_ref(), key_indices)?;
    let keys = key_converter.convert(batch.batch.columns())?;
    let diffs = batch
        .diff()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| EngineError::Unsupported("raw delta weight is not Int64".into()))?;
    let mut result = HashMap::<Vec<u8>, RawKeyDelta>::new();
    for row in 0..batch.len() {
        if diffs.is_null(row) {
            return Err(EngineError::Unsupported("null raw delta weight".into()));
        }
        let diff = diffs.value(row);
        let delta = i256::from_i128(i128::from(diff));
        let entry = result.entry(keys.row(row).as_ref().to_vec()).or_default();
        entry
            .weight
            .add_raw(delta)
            .ok_or(EngineError::NumericOverflow("raw weight bound"))?;
        if entry.nonnull.is_empty() && !columns.is_empty() {
            entry.nonnull.resize(columns.len(), SignedBound::default());
            entry.sum.resize(columns.len(), RawSumBound::default());
        }
        for (slot, demand) in columns.iter().enumerate() {
            let array = batch.batch.column(demand.index);
            if array.is_null(row) {
                continue;
            }
            entry.nonnull[slot]
                .add_raw(delta)
                .ok_or(EngineError::NumericOverflow("raw nonnull bound"))?;
            if demand.sum {
                let value = integer_at(array.as_ref(), row)?;
                let magnitude = abs_i256(i256::from_i128(value))?;
                let contribution = magnitude
                    .checked_mul(abs_i256(delta)?)
                    .ok_or(EngineError::NumericOverflow("raw SUM absolute bound"))?;
                entry.sum[slot].absolute = entry.sum[slot]
                    .absolute
                    .checked_add(contribution)
                    .ok_or(EngineError::NumericOverflow("raw SUM absolute bound"))?;
                let signed = i256::from_i128(value)
                    .checked_mul(delta)
                    .ok_or(EngineError::NumericOverflow("raw SUM signed bound"))?;
                entry.sum[slot]
                    .signed
                    .add_raw(signed)
                    .ok_or(EngineError::NumericOverflow("raw SUM signed bound"))?;
            }
        }
    }
    Ok(result)
}

fn integer_at(array: &dyn Array, row: usize) -> Result<i128, EngineError> {
    macro_rules! read {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .map(|a| i128::from(a.value(row)))
        };
    }
    read!(Int8Array)
        .or_else(|| read!(Int16Array))
        .or_else(|| read!(Int32Array))
        .or_else(|| read!(Int64Array))
        .ok_or_else(|| EngineError::Unsupported("raw SUM value is not signed integer".into()))
}

fn abs_i256(value: i256) -> Result<i256, EngineError> {
    if value < i256::ZERO {
        i256::ZERO
            .checked_sub(value)
            .ok_or(EngineError::NumericOverflow("raw absolute magnitude"))
    } else {
        Ok(value)
    }
}

/// Bounds positive and negative consolidated join changes independently.
/// With old sides `L,R` and raw signed magnitudes `Lp,Ln,Rp,Rn`, positive is
/// `Lp*R + L*Rp + Lp*Rp + Ln*Rn`; negative is
/// `Ln*R + L*Rn + Lp*Rn + Ln*Rp`. The cross terms cover simultaneous deltas.
pub(crate) fn join_delta_bounds(
    old_left: i256,
    old_right: i256,
    lp: i256,
    ln: i256,
    rp: i256,
    rn: i256,
) -> Option<JoinDeltaBounds> {
    let positive = lp
        .checked_mul(old_right)?
        .checked_add(old_left.checked_mul(rp)?)?
        .checked_add(lp.checked_mul(rp)?)?
        .checked_add(ln.checked_mul(rn)?)?;
    let negative = ln
        .checked_mul(old_right)?
        .checked_add(old_left.checked_mul(rn)?)?
        .checked_add(lp.checked_mul(rn)?)?
        .checked_add(ln.checked_mul(rp)?)?;
    Some(JoinDeltaBounds { positive, negative })
}

/// SUM absolute delta for one selected source column across the join:
/// `rawSumAbs*oldOtherAbsWeight + oldSumAbs*rawOtherAbsWeight +
/// rawSumAbs*rawOtherAbsWeight`.
pub(crate) fn sum_delta_abs_bound(
    raw_sum_abs: i256,
    old_other_abs_weight: i256,
    old_sum_abs: i256,
    raw_other_abs_weight: i256,
) -> Option<i256> {
    raw_sum_abs
        .checked_mul(old_other_abs_weight)?
        .checked_add(old_sum_abs.checked_mul(raw_other_abs_weight)?)?
        .checked_add(raw_sum_abs.checked_mul(raw_other_abs_weight)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    #[test]
    fn raw_prefix_bound_rejects_sorted_overflow_even_when_final_count_fits() {
        let max = i256::from_i128(i64::MAX as i128);
        let old = max.checked_sub(i256::ONE).unwrap();
        let mut raw = SignedBound::default();
        raw.add_raw(i256::from_i128(2)).unwrap();
        raw.add_raw(i256::from_i128(-1)).unwrap();
        assert!(!raw.within_prefixes(old, max));
        assert_eq!(
            old.checked_add(raw.positive).unwrap() - raw.negative,
            old + i256::ONE
        );
    }

    #[test]
    fn bilateral_opposite_sign_terms_are_separated_in_three_factor_bound() {
        let bounds = join_delta_bounds(
            i256::from_i128(3),
            i256::from_i128(4),
            i256::from_i128(2),
            i256::from_i128(1),
            i256::from_i128(1),
            i256::from_i128(2),
        )
        .unwrap();
        assert_eq!(bounds.positive, i256::from_i128(15));
        assert_eq!(bounds.negative, i256::from_i128(15));
    }

    #[test]
    fn sum_absolute_bound_includes_bilateral_cross_term() {
        assert_eq!(
            sum_delta_abs_bound(
                i256::from_i128(5),
                i256::from_i128(7),
                i256::from_i128(11),
                i256::from_i128(13),
            ),
            Some(i256::from_i128(243))
        );
    }

    #[test]
    fn raw_arrow_rows_keep_opposite_diffs_before_same_identity_consolidation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = ZSetBatch::new(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![7, 7, 7])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MIN), None]))
                        as ArrayRef,
                ],
            )
            .unwrap(),
            Arc::new(Int64Array::from(vec![1, -1, 3])),
        )
        .unwrap();
        let stats = collect_raw_delta(
            &batch,
            &[0],
            &[RawColumnDemand {
                index: 1,
                sum: true,
            }],
        )
        .unwrap();
        let key = stats.values().next().unwrap();
        assert_eq!(key.weight.positive, i256::from_i128(4));
        assert_eq!(key.weight.negative, i256::ONE);
        assert_eq!(key.nonnull[0].positive, i256::ONE);
        assert_eq!(key.nonnull[0].negative, i256::ONE);
        assert_eq!(key.sum[0].absolute, i256::from_i128(1_i128 << 64));
    }

    #[test]
    fn borrowed_source_facts_derive_count_nn_and_sum_bounds() {
        use crate::arrange::source_stats::{SourceColumnStats, SourceStats};
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
        let mut raw_left = RawKeyDelta::default();
        raw_left.weight.add_raw(i256::ONE).unwrap();
        raw_left.nonnull = vec![SignedBound {
            positive: i256::ONE,
            negative: i256::ZERO,
        }];
        raw_left.sum = vec![RawSumBound {
            signed: SignedBound {
                positive: i256::from_i128(5),
                negative: i256::ZERO,
            },
            absolute: i256::from_i128(5),
        }];
        let mut raw_right = RawKeyDelta::default();
        raw_right.weight.add_raw(-i256::ONE).unwrap();
        raw_right.sum = vec![RawSumBound {
            signed: SignedBound {
                positive: i256::ZERO,
                negative: i256::from_i128(7),
            },
            absolute: i256::from_i128(7),
        }];
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
            JoinMetricDemand {
                left: SourceMeasure::Weight,
                right: SourceMeasure::Sum(0),
                limit: i256::from_i128(i128::MAX),
            },
        ];
        let bounds = derive_key_bounds(
            Some(&left),
            Some(&right),
            Some(&raw_left),
            Some(&raw_right),
            &demands,
        )
        .unwrap();
        assert_eq!(
            bounds.rows,
            Some(JoinDeltaBounds {
                positive: i256::from_i128(4),
                negative: i256::from_i128(4)
            })
        );
        assert_eq!(
            bounds.metrics[1],
            JoinDeltaBounds {
                positive: i256::from_i128(4),
                negative: i256::from_i128(3)
            }
        );
        assert_eq!(bounds.sum_abs_delta[2], Some(i256::from_i128(36)));
        assert_eq!(bounds.sum_abs_delta[3], Some(i256::from_i128(41)));
    }

    #[test]
    fn physical_negative_support_is_not_hidden_by_positive_net_weight() {
        use crate::arrange::source_stats::SourceStats;
        let old = SourceStats {
            weight: 1,
            abs_weight: 3,
            support: 2,
            negative_support: 1,
            ..SourceStats::default()
        };
        let bounds = derive_key_bounds(Some(&old), Some(&SourceStats::default()), None, None, &[]);
        assert!(bounds.is_none());
    }
}
