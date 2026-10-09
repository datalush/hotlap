//! Builds one output column per aggregate from materialized cells.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, SchemaRef};

use hotlap_core::plan::{AggSpec, aggregate_output_type};
use hotlap_core::snapshot::{AggValue, ExtremeValue};

use crate::error::EngineError;

/// A materialized output cell: what an aggregate renders for one group.
#[derive(Clone, Debug)]
pub(crate) enum OutputCell {
    /// A null cell (empty sum/avg or an empty min/max multiset).
    Empty,
    /// An integer cell (count, integer sum, or integer min/max).
    Int(i64),
    /// A float cell (float sum/avg or float min/max).
    Float(f64),
}

impl PartialEq for OutputCell {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (OutputCell::Empty, OutputCell::Empty) => true,
            (OutputCell::Int(a), OutputCell::Int(b)) => a == b,
            // `total_cmp` keeps two `NaN` cells equal, so a delta that leaves
            // the rendered `NaN` unchanged emits nothing.
            (OutputCell::Float(a), OutputCell::Float(b)) => a.total_cmp(b).is_eq(),
            _ => false,
        }
    }
}

/// Renders every aggregate of one group into its output cell.
pub(crate) fn render(values: &[AggValue]) -> Result<Vec<OutputCell>, EngineError> {
    values.iter().map(cell).collect()
}

/// Renders one aggregate accumulator into its output cell.
fn cell(value: &AggValue) -> Result<OutputCell, EngineError> {
    Ok(match value {
        AggValue::Count(count) => OutputCell::Int(*count),
        AggValue::SumInteger { sum, count } => {
            if *count == 0 {
                OutputCell::Empty
            } else {
                OutputCell::Int(range_i64(*sum, "sum")?)
            }
        }
        AggValue::SumFloat {
            sum,
            count,
            nan,
            pos_inf,
            neg_inf,
        } => {
            if *count == 0 {
                OutputCell::Empty
            } else {
                OutputCell::Float(float_sum(*sum, *nan, *pos_inf, *neg_inf))
            }
        }
        AggValue::Avg {
            sum,
            count,
            nan,
            pos_inf,
            neg_inf,
        } => {
            if *count == 0 {
                OutputCell::Empty
            } else {
                OutputCell::Float(float_sum(*sum, *nan, *pos_inf, *neg_inf) / *count as f64)
            }
        }
        AggValue::Min(multiset) => extreme_cell(multiset.min(), "min")?,
        AggValue::Max(multiset) => extreme_cell(multiset.max(), "max")?,
    })
}

/// Renders an optional extreme, widening integers and keeping floats as-is.
fn extreme_cell(value: Option<ExtremeValue>, what: &str) -> Result<OutputCell, EngineError> {
    Ok(match value {
        None => OutputCell::Empty,
        Some(ExtremeValue::Int(v)) => OutputCell::Int(range_i64(v, what)?),
        Some(ExtremeValue::Float(v)) => OutputCell::Float(v),
    })
}

/// Recombines the finite running sum with the special-input multiplicities.
///
/// Follows IEEE semantics: any `NaN` poisons the result, and mixing `+Inf` with
/// `-Inf` is `NaN`; a single infinite sign dominates. A finite sum that
/// overflowed to infinity is returned as-is (finite-only overflow is out of
/// scope).
fn float_sum(finite: f64, nan: i64, pos_inf: i64, neg_inf: i64) -> f64 {
    if nan > 0 || (pos_inf > 0 && neg_inf > 0) {
        f64::NAN
    } else if pos_inf > 0 {
        f64::INFINITY
    } else if neg_inf > 0 {
        f64::NEG_INFINITY
    } else {
        finite
    }
}

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

    /// Appends one materialized cell, mapping empties to null.
    pub(crate) fn push_cell(&mut self, cell: &OutputCell) -> Result<(), EngineError> {
        match (self, cell) {
            (ColumnBuilder::Int32(out), OutputCell::Empty) => out.push(None),
            (ColumnBuilder::Int32(out), OutputCell::Int(v)) => {
                out.push(Some(
                    i32::try_from(*v).map_err(|_| out_of_range("cell", "Int32"))?,
                ));
            }
            (ColumnBuilder::Int(out), OutputCell::Empty) => out.push(None),
            (ColumnBuilder::Int(out), OutputCell::Int(v)) => out.push(Some(*v)),
            (ColumnBuilder::Float(out), OutputCell::Empty) => out.push(None),
            (ColumnBuilder::Float(out), OutputCell::Float(v)) => out.push(Some(*v)),
            _ => {
                return Err(EngineError::Infrastructure(
                    "aggregate output cell type mismatch".to_string(),
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

fn range_i64(value: i128, what: &str) -> Result<i64, EngineError> {
    i64::try_from(value).map_err(|_| out_of_range(what, "Int64"))
}

fn out_of_range(what: &str, ty: &str) -> EngineError {
    EngineError::Infrastructure(format!("group {what} out of range for {ty}"))
}
