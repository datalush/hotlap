//! Accumulator initialization, validation and retraction-aware folding.

use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::plan::{AggFunc, AggSpec, aggregate_output_type};
use hotlap_core::snapshot::{AggValue, GroupEntry, OrderedMultiset};

use crate::error::EngineError;

use super::read::{extreme_at, float_at, int_at};

/// Validates every aggregate against the reducer's input `schema`.
pub(crate) fn validate_aggs(aggs: &[AggSpec], schema: &Schema) -> Result<(), EngineError> {
    if aggs.is_empty() {
        return Err(EngineError::Unsupported(
            "at least one aggregate is required".to_string(),
        ));
    }
    for agg in aggs {
        match agg.func {
            AggFunc::Count => {
                if let Some(index) = agg.input {
                    field(schema, index)?;
                }
            }
            AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
                let index = agg
                    .input
                    .ok_or_else(|| unsupported(agg, "requires an input column"))?;
                let ty = field(schema, index)?;
                aggregate_output_type(agg.func, Some(ty.data_type()))?;
            }
        }
    }
    Ok(())
}

/// Builds a zeroed accumulator set matching `aggs` and the input `schema`.
pub(crate) fn empty_entry(aggs: &[AggSpec], schema: &SchemaRef) -> Result<GroupEntry, EngineError> {
    let mut values = Vec::with_capacity(aggs.len());
    for agg in aggs {
        values.push(zeroed(agg, schema)?);
    }
    Ok(GroupEntry { rows: 0, values })
}

/// Folds one input row with multiplicity `diff` into `entry`.
pub(crate) fn fold_row(
    entry: &mut GroupEntry,
    aggs: &[AggSpec],
    batch: &RecordBatch,
    row: usize,
    diff: i64,
) -> Result<(), EngineError> {
    entry.rows = add(entry.rows, diff, "group row count")?;
    for (agg, value) in aggs.iter().zip(entry.values.iter_mut()) {
        match value {
            AggValue::Count(count) => {
                let increment = match agg.input {
                    None => diff,
                    Some(col) => match batch.column(col).is_null(row) {
                        true => 0,
                        false => diff,
                    },
                };
                *count = add(*count, increment, "group count")?;
            }
            AggValue::SumInteger { sum, count } => {
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
            }
            AggValue::SumFloat {
                sum,
                count,
                nan,
                pos_inf,
                neg_inf,
            } => {
                let col = input(agg)?;
                if let Some(value) = float_at(batch, col, row)? {
                    fold_float(sum, count, nan, pos_inf, neg_inf, value, diff, "group sum")?;
                }
            }
            AggValue::Avg {
                sum,
                count,
                nan,
                pos_inf,
                neg_inf,
            } => {
                let col = input(agg)?;
                if let Some(value) = float_at(batch, col, row)? {
                    fold_float(sum, count, nan, pos_inf, neg_inf, value, diff, "group avg")?;
                }
            }
            AggValue::Min(multiset) | AggValue::Max(multiset) => {
                let col = input(agg)?;
                if let Some(value) = extreme_at(batch, col, row)? {
                    multiset.add(value, diff);
                }
            }
        }
    }
    Ok(())
}

/// Folds one finite or special float into a `sum`/`avg` accumulator.
///
/// Finite values extend the running sum; `NaN`/`+Inf`/`-Inf` extend separate
/// multiplicities so a later retraction can recover the finite remainder.
fn fold_float(
    sum: &mut f64,
    count: &mut i64,
    nan: &mut i64,
    pos_inf: &mut i64,
    neg_inf: &mut i64,
    value: f64,
    diff: i64,
    what: &str,
) -> Result<(), EngineError> {
    if value.is_nan() {
        *nan = occurrences(*nan, diff, &format!("{what} NaN count"))?;
    } else if value == f64::INFINITY {
        *pos_inf = occurrences(*pos_inf, diff, &format!("{what} +inf count"))?;
    } else if value == f64::NEG_INFINITY {
        *neg_inf = occurrences(*neg_inf, diff, &format!("{what} -inf count"))?;
    } else {
        *sum += value * diff as f64;
    }
    *count = add(*count, diff, &format!("{what} count"))?;
    Ok(())
}

/// The zero accumulator matching `agg`'s function and input column type.
fn zeroed(agg: &AggSpec, schema: &SchemaRef) -> Result<AggValue, EngineError> {
    match agg.func {
        AggFunc::Count => Ok(AggValue::Count(0)),
        AggFunc::Sum => match input_type(agg, schema)? {
            DataType::Int32 | DataType::Int64 => Ok(AggValue::SumInteger { sum: 0, count: 0 }),
            DataType::Float64 => Ok(AggValue::SumFloat {
                sum: 0.0,
                count: 0,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            }),
            other => Err(unsupported(agg, &format!("input type {other:?}"))),
        },
        AggFunc::Avg => Ok(AggValue::Avg {
            sum: 0.0,
            count: 0,
            nan: 0,
            pos_inf: 0,
            neg_inf: 0,
        }),
        AggFunc::Min => Ok(AggValue::Min(OrderedMultiset::default())),
        AggFunc::Max => Ok(AggValue::Max(OrderedMultiset::default())),
    }
}

/// The declared data type of the aggregate's input column.
fn input_type(agg: &AggSpec, schema: &SchemaRef) -> Result<DataType, EngineError> {
    let index = agg
        .input
        .ok_or_else(|| unsupported(agg, "requires an input column"))?;
    Ok(field(schema, index)?.data_type().clone())
}

fn input(agg: &AggSpec) -> Result<usize, EngineError> {
    agg.input
        .ok_or_else(|| unsupported(agg, "requires an input column"))
}

fn field(schema: &Schema, index: usize) -> Result<&FieldRef, EngineError> {
    schema.fields().get(index).ok_or_else(|| {
        EngineError::Unsupported(format!("group aggregate column {index} out of range"))
    })
}

fn add(value: i64, delta: i64, what: &str) -> Result<i64, EngineError> {
    value.checked_add(delta).ok_or_else(|| overflow(what))
}

/// Applies `delta` to a special-input multiplicity, rejecting a negative result.
///
/// Retracting a `NaN`/`+Inf`/`-Inf` more often than it was inserted is an
/// invalid retraction, not a state to saturate: a negative multiplicity would
/// silently corrupt the materialized sum.
fn occurrences(count: i64, delta: i64, what: &str) -> Result<i64, EngineError> {
    let next = count.checked_add(delta).ok_or_else(|| overflow(what))?;
    if next < 0 {
        return Err(EngineError::Infrastructure(format!(
            "{what} retracted below zero"
        )));
    }
    Ok(next)
}

fn overflow(what: &str) -> EngineError {
    EngineError::Infrastructure(format!("{what} overflowed"))
}

fn unsupported(agg: &AggSpec, why: &str) -> EngineError {
    EngineError::Unsupported(format!("{:?} {why}", agg.func))
}
