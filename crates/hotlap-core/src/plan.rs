//! Minimal plan IR: the operators the incremental core compiles to.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, Scalar as ArrowScalar,
    StringArray,
};
use arrow::compute::{cast, kernels::cmp};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use crate::ids::InputId;

/// A scalar literal used by [`Predicate`].
///
/// `Eq` is implemented manually because the `F64` payload is not `Eq`; the
/// equality itself still follows `f64` semantics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Scalar {
    Null,
    I64(i64),
    I32(i32),
    F64(f64),
    Str(String),
    Bool(bool),
}

impl Eq for Scalar {}

/// Filter predicate over a row: `Eq` compares a column to a literal, `Gt` is
/// `column > literal` on numeric columns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Predicate {
    Eq(usize, Scalar),
    Gt(usize, i64),
}

impl Predicate {
    /// Evaluates the predicate over `batch`, returning one boolean per row.
    ///
    /// A null comparison yields a null mask entry, which the filter operator
    /// treats as `false`. An out-of-range column is `Unsupported`.
    pub fn eval(&self, batch: &RecordBatch) -> Result<BooleanArray, CoreError> {
        match self {
            Predicate::Gt(col, bound) => gt(column(batch, *col)?, *bound),
            Predicate::Eq(col, want) => {
                let column = column(batch, *col)?;
                if matches!(want, Scalar::Null) {
                    let mask: Vec<bool> = (0..column.len())
                        .map(|index| column.is_null(index))
                        .collect();
                    return Ok(BooleanArray::from(mask));
                }
                let scalar = ArrowScalar::new(literal(want, column.data_type())?);
                Ok(cmp::eq(column, &scalar)?)
            }
        }
    }
}

/// Returns the column at `col`, or `Unsupported` when it is out of range.
fn column(batch: &RecordBatch, col: usize) -> Result<&ArrayRef, CoreError> {
    batch.columns().get(col).ok_or_else(|| {
        CoreError::Unsupported(format!(
            "predicate column {col} out of range (arity {})",
            batch.num_columns()
        ))
    })
}

/// Compares `column > bound`, keeping the bound's numeric value on floats.
///
/// A `Float64` column is compared in `f64` so `1.5 > 1` stays true; any other
/// numeric column is cast to `i64`.
fn gt(column: &ArrayRef, bound: i64) -> Result<BooleanArray, CoreError> {
    if column.data_type() == &DataType::Float64 {
        let casted = cast(column.as_ref(), &DataType::Float64)?;
        let scalar = ArrowScalar::new(Float64Array::from(vec![bound as f64]));
        return Ok(cmp::gt(&casted, &scalar)?);
    }
    let casted = cast(column.as_ref(), &DataType::Int64)?;
    let scalar = ArrowScalar::new(Int64Array::from(vec![bound]));
    Ok(cmp::gt(&casted, &scalar)?)
}

/// Builds a length-one array holding `value`, cast to `data_type`.
fn literal(value: &Scalar, data_type: &DataType) -> Result<ArrayRef, CoreError> {
    let base: ArrayRef = match value {
        Scalar::Null => return Ok(arrow::array::new_null_array(data_type, 1)),
        Scalar::I64(v) => Arc::new(Int64Array::from(vec![*v])),
        Scalar::I32(v) => Arc::new(Int32Array::from(vec![*v])),
        Scalar::F64(v) => Arc::new(Float64Array::from(vec![*v])),
        Scalar::Str(s) => Arc::new(StringArray::from(vec![s.as_str()])),
        Scalar::Bool(b) => Arc::new(BooleanArray::from(vec![*b])),
    };
    if base.data_type() == data_type {
        Ok(base)
    } else {
        Ok(cast(base.as_ref(), data_type)?)
    }
}

/// The operators a plan may contain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Plan {
    /// Leaf: the shared source registered under this id.
    Source(InputId),
    Filter {
        input: Box<Plan>,
        pred: Predicate,
    },
    Project {
        input: Box<Plan>,
        cols: Vec<usize>,
    },
    GroupCount {
        input: Box<Plan>,
        key: Vec<usize>,
    },
    /// Inner equi-join of two plans on `left_key`/`right_key`.
    ///
    /// The output row is every column of the joined left row followed by every
    /// column of the joined right row; multiplicities multiply.
    Join {
        left: Box<Plan>,
        right: Box<Plan>,
        left_key: Vec<usize>,
        right_key: Vec<usize>,
    },
    /// Tumbling-window aggregate of `size` over `time_col`, grouped by `key`.
    /// Output: columns of `key`, `window_start`, `count`; emits each window once
    /// when it closes (append-only).
    TumbleCount {
        input: Box<Plan>,
        key: Vec<usize>,
        time_col: usize,
        size: i64,
    },
}

/// Does the plan contain any window?
pub fn has_window(plan: &Plan) -> bool {
    match plan {
        Plan::TumbleCount { .. } => true,
        Plan::Filter { input, .. }
        | Plan::Project { input, .. }
        | Plan::GroupCount { input, .. } => has_window(input),
        Plan::Join { left, right, .. } => has_window(left) || has_window(right),
        Plan::Source(_) => false,
    }
}

/// Collects the input ids a plan reads from, in traversal order.
pub fn sources(plan: &Plan) -> Vec<InputId> {
    let mut out = Vec::new();
    collect_sources(plan, &mut out);
    out
}

fn collect_sources(plan: &Plan, out: &mut Vec<InputId>) {
    match plan {
        Plan::Source(id) => out.push(*id),
        Plan::Filter { input, .. }
        | Plan::Project { input, .. }
        | Plan::GroupCount { input, .. }
        | Plan::TumbleCount { input, .. } => collect_sources(input, out),
        Plan::Join { left, right, .. } => {
            collect_sources(left, out);
            collect_sources(right, out);
        }
    }
}
