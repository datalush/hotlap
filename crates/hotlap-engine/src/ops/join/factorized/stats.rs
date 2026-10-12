use std::collections::HashMap;

use arrow::array::{Array, Int64Array};
use hotlap_core::plan::{AggFunc, AggSpec};
use hotlap_core::plan_normalize::ColumnMapping;

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::ops::group_aggregate::read::int_at;

use super::prepare::DenseReason;

pub(super) type GroupAbsBounds = HashMap<Vec<u8>, Vec<(u128, u128)>>;

#[derive(Clone, Debug, Default)]
pub(super) struct Stats {
    pub(super) rows: i128,
    pub(super) abs_rows: u128,
    pub(super) nonnull: i128,
    pub(super) abs_nonnull: u128,
    pub(super) sum: i128,
    pub(super) abs_sum: u128,
    pub(super) float_sum: f64,
    pub(super) nan: i128,
    pub(super) pos_inf: i128,
    pub(super) neg_inf: i128,
}

impl Stats {
    pub(super) fn checked_add(&mut self, other: &Self) -> Option<()> {
        self.rows = self.rows.checked_add(other.rows)?;
        self.abs_rows = self.abs_rows.checked_add(other.abs_rows)?;
        self.nonnull = self.nonnull.checked_add(other.nonnull)?;
        self.abs_nonnull = self.abs_nonnull.checked_add(other.abs_nonnull)?;
        self.sum = self.sum.checked_add(other.sum)?;
        self.abs_sum = self.abs_sum.checked_add(other.abs_sum)?;
        self.float_sum += other.float_sum;
        self.nan = self.nan.checked_add(other.nan)?;
        self.pos_inf = self.pos_inf.checked_add(other.pos_inf)?;
        self.neg_inf = self.neg_inf.checked_add(other.neg_inf)?;
        Some(())
    }
}

pub(super) fn summarize(
    batch: &ZSetBatch,
    side: usize,
    aggs: &[AggSpec],
    _mapping: &ColumnMapping,
    left_width: usize,
) -> Result<Vec<Stats>, EngineError> {
    let diffs = batch
        .diff()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| EngineError::Unsupported("diff is not Int64".into()))?;
    let mut result = vec![Stats::default(); aggs.len()];
    for row in 0..batch.len() {
        let weight = i128::from(diffs.value(row));
        for (i, agg) in aggs.iter().enumerate() {
            let item = &mut result[i];
            item.rows = item
                .rows
                .checked_add(weight)
                .ok_or(EngineError::NumericOverflow("row stats"))?;
            item.abs_rows = item
                .abs_rows
                .checked_add(weight.unsigned_abs())
                .ok_or(EngineError::NumericOverflow("absolute row stats"))?;
            let Some(input) = agg.input else { continue };
            let col = if side == 0 {
                if input >= left_width {
                    continue;
                }
                input
            } else {
                if input < left_width {
                    continue;
                }
                input - left_width
            };
            if col >= batch.batch.num_columns() {
                return Err(EngineError::Unsupported(
                    "aggregate input column out of range".into(),
                ));
            }
            if batch.batch.column(col).is_null(row) {
                continue;
            }
            item.nonnull = item
                .nonnull
                .checked_add(weight)
                .ok_or(EngineError::NumericOverflow("nonnull stats"))?;
            item.abs_nonnull = item
                .abs_nonnull
                .checked_add(weight.unsigned_abs())
                .ok_or(EngineError::NumericOverflow("absolute nonnull stats"))?;
            match agg.func {
                AggFunc::Sum | AggFunc::Avg => {
                    if let Some(value) = int_at(&batch.batch, col, row)? {
                        let term = value
                            .checked_mul(weight)
                            .ok_or(EngineError::NumericOverflow("sum contribution"))?;
                        if agg.func == AggFunc::Avg
                            && (value.unsigned_abs() > (1_u128 << 53)
                                || weight.unsigned_abs() > (1_u128 << 53)
                                || term.unsigned_abs() > (1_u128 << 53))
                        {
                            return Err(EngineError::Unsupported(
                                "AVG integer is outside exact f64 domain".into(),
                            ));
                        }
                        item.sum = item
                            .sum
                            .checked_add(term)
                            .ok_or(EngineError::NumericOverflow("sum stats"))?;
                        item.abs_sum = item
                            .abs_sum
                            .checked_add(term.unsigned_abs())
                            .ok_or(EngineError::NumericOverflow("sum abs bound"))?;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(result)
}

pub(super) fn delta_product(
    old_x: i128,
    dx: i128,
    old_y: i128,
    dy: i128,
    new_x: i128,
    new_y: i128,
) -> Result<i128, DenseReason> {
    let result = old_x
        .checked_mul(dy)
        .and_then(|a| dx.checked_mul(old_y).and_then(|b| a.checked_add(b)))
        .and_then(|a| dx.checked_mul(dy).and_then(|b| a.checked_add(b)))
        .filter(|_| new_x.checked_mul(new_y).is_some())
        .ok_or(DenseReason::NegativeOrOverflow);
    #[cfg(test)]
    if result.is_err() {
        eprintln!("delta product overflow: {old_x} {dx} {old_y} {dy} {new_x} {new_y}");
    }
    result
}

pub(super) fn delta_bound(
    delta_x: u128,
    old_x: u128,
    delta_y: u128,
    old_y: u128,
) -> Result<u128, DenseReason> {
    delta_x
        .checked_mul(old_y)
        .and_then(|a| old_x.checked_mul(delta_y).and_then(|b| a.checked_add(b)))
        .and_then(|a| delta_x.checked_mul(delta_y).and_then(|b| a.checked_add(b)))
        .ok_or(DenseReason::NegativeOrOverflow)
}

pub(super) fn sum_delta_bound(
    delta_sum: u128,
    old_sum: u128,
    delta_rows: u128,
    old_rows: u128,
) -> Result<u128, DenseReason> {
    delta_sum
        .checked_mul(old_rows)
        .and_then(|a| {
            old_sum
                .checked_mul(delta_rows)
                .and_then(|b| a.checked_add(b))
        })
        .and_then(|a| {
            delta_sum
                .checked_mul(delta_rows)
                .and_then(|b| a.checked_add(b))
        })
        .ok_or(DenseReason::NegativeOrOverflow)
}

pub(super) fn validate_i128_abs_bound(bound: u128) -> Result<(), DenseReason> {
    if bound > i128::MAX as u128 {
        Err(DenseReason::NegativeOrOverflow)
    } else {
        Ok(())
    }
}
