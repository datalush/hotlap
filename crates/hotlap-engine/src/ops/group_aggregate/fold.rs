//! Retraction-aware folding of one input row into a group's accumulators.

use arrow::record_batch::RecordBatch;

use hotlap_core::plan::AggSpec;
use hotlap_core::snapshot::{AggValue, GroupEntry, OrderedMultiset};

use crate::error::EngineError;

use super::read::{extreme_at, float_at, int_at};

/// Folds one input row with multiplicity `diff` into `entry`.
pub(super) fn fold_row(
    entry: &mut GroupEntry,
    aggs: &[AggSpec],
    batch: &RecordBatch,
    row: usize,
    diff: i64,
) -> Result<(), EngineError> {
    entry.rows = add(entry.rows, diff, "group row count")?;
    for (agg, value) in aggs.iter().zip(entry.values.iter_mut()) {
        fold_value(agg, value, batch, row, diff)?;
    }
    Ok(())
}

/// Folds one row's cell into a single aggregate accumulator.
fn fold_value(
    agg: &AggSpec,
    value: &mut AggValue,
    batch: &RecordBatch,
    row: usize,
    diff: i64,
) -> Result<(), EngineError> {
    match value {
        AggValue::Count(count) => fold_count(agg, count, batch, row, diff),
        AggValue::SumInteger { sum, count } => fold_integer(agg, sum, count, batch, row, diff),
        AggValue::SumFloat {
            sum,
            count,
            nan,
            pos_inf,
            neg_inf,
        }
        | AggValue::Avg {
            sum,
            count,
            nan,
            pos_inf,
            neg_inf,
        } => fold_float(
            agg,
            FloatParts {
                sum,
                count,
                nan,
                pos_inf,
                neg_inf,
            },
            batch,
            row,
            diff,
        ),
        AggValue::Min(multiset) | AggValue::Max(multiset) => {
            fold_extreme(agg, multiset, batch, row, diff)
        }
    }
}

/// Folds a row into a `count` accumulator, ignoring null inputs when keyed.
fn fold_count(
    agg: &AggSpec,
    count: &mut i64,
    batch: &RecordBatch,
    row: usize,
    diff: i64,
) -> Result<(), EngineError> {
    let increment = match agg.input {
        None => diff,
        Some(col) => match batch.column(col).is_null(row) {
            true => 0,
            false => diff,
        },
    };
    *count = add(*count, increment, "group count")?;
    Ok(())
}

/// Folds a row into an integer `sum` accumulator, failing on overflow.
fn fold_integer(
    agg: &AggSpec,
    sum: &mut i128,
    count: &mut i64,
    batch: &RecordBatch,
    row: usize,
    diff: i64,
) -> Result<(), EngineError> {
    let col = input(agg)?;
    if let Some(value) = int_at(batch, col, row)? {
        let delta = value
            .checked_mul(diff as i128)
            .ok_or_else(|| overflow("group sum"))?;
        *sum = sum
            .checked_add(delta)
            .ok_or_else(|| overflow("group sum"))?;
        *count = add(*count, diff, "group sum count")?;
    }
    Ok(())
}

/// The mutable fields of a float accumulator passed to [`fold_float`].
struct FloatParts<'a> {
    sum: &'a mut f64,
    count: &'a mut i64,
    nan: &'a mut i64,
    pos_inf: &'a mut i64,
    neg_inf: &'a mut i64,
}

/// Folds a row into a float `sum`/`avg` accumulator.
///
/// Finite values extend the running sum; `NaN`/`+Inf`/`-Inf` extend separate
/// multiplicities so a later retraction can recover the finite remainder.
fn fold_float(
    agg: &AggSpec,
    parts: FloatParts<'_>,
    batch: &RecordBatch,
    row: usize,
    diff: i64,
) -> Result<(), EngineError> {
    let col = input(agg)?;
    if let Some(value) = float_at(batch, col, row)? {
        if value.is_nan() {
            *parts.nan = occurrences(*parts.nan, diff, "group float NaN count")?;
        } else if value == f64::INFINITY {
            *parts.pos_inf = occurrences(*parts.pos_inf, diff, "group float +inf count")?;
        } else if value == f64::NEG_INFINITY {
            *parts.neg_inf = occurrences(*parts.neg_inf, diff, "group float -inf count")?;
        } else {
            *parts.sum += value * diff as f64;
        }
        *parts.count = add(*parts.count, diff, "group float count")?;
    }
    Ok(())
}

/// Folds a row into a `min`/`max` multiset, ignoring null inputs.
fn fold_extreme(
    agg: &AggSpec,
    multiset: &mut OrderedMultiset,
    batch: &RecordBatch,
    row: usize,
    diff: i64,
) -> Result<(), EngineError> {
    let col = input(agg)?;
    if let Some(value) = extreme_at(batch, col, row)? {
        multiset.add(value, diff);
    }
    Ok(())
}

/// Applies `delta` to a special-input multiplicity, rejecting a negative result.
///
/// Retracting a `NaN`/`+Inf`/`-Inf` more often than it was inserted is an
/// invalid retraction: a negative multiplicity would corrupt the sum.
fn occurrences(count: i64, delta: i64, what: &str) -> Result<i64, EngineError> {
    let next = count.checked_add(delta).ok_or_else(|| overflow(what))?;
    if next < 0 {
        return Err(EngineError::Infrastructure(format!(
            "{what} retracted below zero"
        )));
    }
    Ok(next)
}

fn input(agg: &AggSpec) -> Result<usize, EngineError> {
    agg.input
        .ok_or_else(|| EngineError::Unsupported(format!("{:?} requires an input column", agg.func)))
}

fn add(value: i64, delta: i64, what: &str) -> Result<i64, EngineError> {
    value.checked_add(delta).ok_or_else(|| overflow(what))
}

fn overflow(what: &str) -> EngineError {
    EngineError::Infrastructure(format!("{what} overflowed"))
}
