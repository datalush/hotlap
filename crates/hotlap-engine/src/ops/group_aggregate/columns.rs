//! Builds one output column per aggregate, fixing its Arrow type up front.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, SchemaRef};

use hotlap_core::plan::{AggSpec, aggregate_output_type};
use hotlap_core::snapshot::{AggValue, ExtremeValue};

use crate::error::EngineError;

/// Accumulates one output column, fixing its Arrow type up front.
pub(crate) enum ColumnBuilder {
    Int32(Vec<Option<i32>>),
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
}

impl ColumnBuilder {
    /// Picks the builder matching `agg`'s resolved output type.
    pub(crate) fn new(agg: &AggSpec, schema: &SchemaRef) -> Result<Self, EngineError> {
        let input = agg.input.map(|i| schema.field(i).data_type());
        match aggregate_output_type(agg.func, input)? {
            DataType::Int32 => Ok(ColumnBuilder::Int32(Vec::new())),
            DataType::Int64 => Ok(ColumnBuilder::Int(Vec::new())),
            DataType::Float64 => Ok(ColumnBuilder::Float(Vec::new())),
            other => Err(EngineError::Unsupported(format!(
                "aggregate output type {other:?} is not supported"
            ))),
        }
    }

    /// Appends one accumulated value, mapping empty groups to null.
    pub(crate) fn push(&mut self, value: &AggValue) -> Result<(), EngineError> {
        match (self, value) {
            (ColumnBuilder::Int32(out), AggValue::Min(m)) => out.push(int32_cell(m.min(), "min")?),
            (ColumnBuilder::Int32(out), AggValue::Max(m)) => out.push(int32_cell(m.max(), "max")?),
            (ColumnBuilder::Int(out), AggValue::Count(count)) => out.push(Some(*count)),
            (ColumnBuilder::Int(out), AggValue::SumInteger { sum, count }) => {
                out.push(sum_cell(*count, *sum)?);
            }
            (ColumnBuilder::Int(out), AggValue::Min(m)) => out.push(int64_cell(m.min(), "min")?),
            (ColumnBuilder::Int(out), AggValue::Max(m)) => out.push(int64_cell(m.max(), "max")?),
            (ColumnBuilder::Float(out), AggValue::SumFloat { sum, count }) => {
                out.push(if *count == 0 { None } else { Some(*sum) });
            }
            (ColumnBuilder::Float(out), AggValue::Avg { sum, count }) => {
                out.push(if *count == 0 {
                    None
                } else {
                    Some(*sum / *count as f64)
                });
            }
            (ColumnBuilder::Float(out), AggValue::Min(m)) => out.push(float_cell(m.min(), "min")?),
            (ColumnBuilder::Float(out), AggValue::Max(m)) => out.push(float_cell(m.max(), "max")?),
            _ => {
                return Err(EngineError::Infrastructure(
                    "aggregate output type mismatch".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Freezes the column into an Arrow array.
    pub(crate) fn finish(self) -> ArrayRef {
        match self {
            ColumnBuilder::Int32(values) => Arc::new(Int32Array::from(values)),
            ColumnBuilder::Int(values) => Arc::new(Int64Array::from(values)),
            ColumnBuilder::Float(values) => Arc::new(Float64Array::from(values)),
        }
    }
}

/// The `Int64` sum cell: null when empty, checked for range otherwise.
fn sum_cell(count: i64, sum: i128) -> Result<Option<i64>, EngineError> {
    if count == 0 {
        return Ok(None);
    }
    Ok(Some(range_i64(sum, "sum")?))
}

/// An `Int32` min/max cell, null when the group holds no values.
fn int32_cell(value: Option<ExtremeValue>, what: &str) -> Result<Option<i32>, EngineError> {
    match value {
        None => Ok(None),
        Some(ExtremeValue::Int(v)) => i32::try_from(v)
            .map(Some)
            .map_err(|_| out_of_range(what, "Int32")),
        Some(ExtremeValue::Float(_)) => Err(mismatch(what)),
    }
}

/// An `Int64` min/max cell, null when the group holds no values.
fn int64_cell(value: Option<ExtremeValue>, what: &str) -> Result<Option<i64>, EngineError> {
    match value {
        None => Ok(None),
        Some(ExtremeValue::Int(v)) => Ok(Some(range_i64(v, what)?)),
        Some(ExtremeValue::Float(_)) => Err(mismatch(what)),
    }
}

/// A `Float64` min/max cell, null when the group holds no values.
fn float_cell(value: Option<ExtremeValue>, what: &str) -> Result<Option<f64>, EngineError> {
    match value {
        None => Ok(None),
        Some(ExtremeValue::Float(v)) => Ok(Some(v)),
        Some(ExtremeValue::Int(_)) => Err(mismatch(what)),
    }
}

fn range_i64(value: i128, what: &str) -> Result<i64, EngineError> {
    i64::try_from(value).map_err(|_| out_of_range(what, "Int64"))
}

fn out_of_range(what: &str, ty: &str) -> EngineError {
    EngineError::Infrastructure(format!("group {what} out of range for {ty}"))
}

fn mismatch(what: &str) -> EngineError {
    EngineError::Infrastructure(format!("group {what} value type mismatch"))
}
