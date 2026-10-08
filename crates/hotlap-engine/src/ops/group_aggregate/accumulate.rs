//! Accumulator initialization, validation and retraction-aware folding.

use arrow::array::{Array, Float64Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::plan::{AggFunc, AggSpec, aggregate_output_type};
use hotlap_core::snapshot::{AggValue, GroupEntry};

use crate::error::EngineError;

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
            AggFunc::Sum | AggFunc::Avg => {
                let index = agg
                    .input
                    .ok_or_else(|| unsupported(agg, "requires an input column"))?;
                let ty = field(schema, index)?;
                aggregate_output_type(agg.func, Some(ty.data_type()))?;
            }
            AggFunc::Min | AggFunc::Max => {
                return Err(EngineError::Unsupported(
                    "min/max aggregates are not implemented yet".to_string(),
                ));
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
            AggValue::SumFloat { sum, count } => {
                let col = input(agg)?;
                if let Some(value) = float_at(batch, col, row)? {
                    *sum += value * diff as f64;
                    *count = add(*count, diff, "group sum count")?;
                }
            }
            AggValue::Avg { sum, count } => {
                let col = input(agg)?;
                if let Some(value) = float_at(batch, col, row)? {
                    *sum += value * diff as f64;
                    *count = add(*count, diff, "group avg count")?;
                }
            }
        }
    }
    Ok(())
}

/// The zero accumulator matching `agg`'s function and input column type.
fn zeroed(agg: &AggSpec, schema: &SchemaRef) -> Result<AggValue, EngineError> {
    match agg.func {
        AggFunc::Count => Ok(AggValue::Count(0)),
        AggFunc::Sum => match input_type(agg, schema)? {
            DataType::Int32 | DataType::Int64 => Ok(AggValue::SumInteger { sum: 0, count: 0 }),
            DataType::Float64 => Ok(AggValue::SumFloat { sum: 0.0, count: 0 }),
            other => Err(unsupported(agg, &format!("input type {other:?}"))),
        },
        AggFunc::Avg => Ok(AggValue::Avg { sum: 0.0, count: 0 }),
        AggFunc::Min | AggFunc::Max => Err(EngineError::Unsupported(
            "min/max aggregates are not implemented yet".to_string(),
        )),
    }
}

/// Reads an integer cell as `i128`, or `None` when it is null.
fn int_at(batch: &RecordBatch, col: usize, row: usize) -> Result<Option<i128>, EngineError> {
    let column = batch.column(col);
    match column.data_type() {
        DataType::Int32 => Ok(option(column, row, |a: &Int32Array| a.value(row) as i128)),
        DataType::Int64 => Ok(option(column, row, |a: &Int64Array| a.value(row) as i128)),
        other => Err(EngineError::Infrastructure(format!(
            "expected an integer column, found {other:?}"
        ))),
    }
}

/// Reads a numeric cell as `f64`, or `None` when it is null.
fn float_at(batch: &RecordBatch, col: usize, row: usize) -> Result<Option<f64>, EngineError> {
    let column = batch.column(col);
    match column.data_type() {
        DataType::Int32 => Ok(option(column, row, |a: &Int32Array| a.value(row) as f64)),
        DataType::Int64 => Ok(option(column, row, |a: &Int64Array| a.value(row) as f64)),
        DataType::Float64 => Ok(option(column, row, |a: &Float64Array| a.value(row))),
        other => Err(EngineError::Infrastructure(format!(
            "expected a numeric column, found {other:?}"
        ))),
    }
}

/// Downcasts `column` and reads `row`, mapping null to `None`.
fn option<T: Array + 'static, V>(
    column: &std::sync::Arc<dyn Array>,
    row: usize,
    read: impl FnOnce(&T) -> V,
) -> Option<V> {
    if column.is_null(row) {
        return None;
    }
    column.as_any().downcast_ref::<T>().map(read)
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

fn overflow(what: &str) -> EngineError {
    EngineError::Infrastructure(format!("{what} overflowed"))
}

fn unsupported(agg: &AggSpec, why: &str) -> EngineError {
    EngineError::Unsupported(format!("{:?} {why}", agg.func))
}
