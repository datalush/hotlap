//! Minimal plan IR: the operators the incremental core compiles to.

use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use crate::ids::InputId;
pub use crate::predicate::{CmpOp, Predicate, Scalar};

/// An aggregate function the kernel can maintain incrementally.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunc {
    /// Number of rows (or non-null values when given an input column).
    Count,
    /// Sum of an integer or float column.
    Sum,
    /// Smallest value of a column.
    Min,
    /// Largest value of a column.
    Max,
    /// Arithmetic mean of an integer or float column.
    Avg,
}

/// One aggregate: the function plus the input column it reads, if any.
///
/// `Count` with no input counts every row; every other function requires a
/// column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggSpec {
    /// The function to apply.
    pub func: AggFunc,
    /// Input column index, or `None` for `count(*)`.
    pub input: Option<usize>,
}

impl AggSpec {
    /// A star `count(*)` over the group's rows.
    pub fn count() -> Self {
        Self {
            func: AggFunc::Count,
            input: None,
        }
    }

    /// A `sum` over column `input`.
    pub fn sum(input: usize) -> Self {
        Self {
            func: AggFunc::Sum,
            input: Some(input),
        }
    }

    /// An `avg` over column `input`.
    pub fn avg(input: usize) -> Self {
        Self {
            func: AggFunc::Avg,
            input: Some(input),
        }
    }

    /// A `min` over column `input`.
    pub fn min(input: usize) -> Self {
        Self {
            func: AggFunc::Min,
            input: Some(input),
        }
    }

    /// A `max` over column `input`.
    pub fn max(input: usize) -> Self {
        Self {
            func: AggFunc::Max,
            input: Some(input),
        }
    }
}

impl AggFunc {
    /// Output column name used by the materialized-view schema.
    pub fn output_name(self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::Avg => "avg",
        }
    }
}

/// Output Arrow type of `func` over a column of type `input`.
///
/// Mirrors DataFusion's resolved aggregate outputs for the supported types:
/// integer `sum` widens to `Int64`, `avg` is always `Float64`, and `count` is
/// `Int64`. Unsupported combinations are `Unsupported`.
pub fn aggregate_output_type(
    func: AggFunc,
    input: Option<&DataType>,
) -> Result<DataType, CoreError> {
    match func {
        AggFunc::Count => Ok(DataType::Int64),
        AggFunc::Avg => match input {
            Some(DataType::Int32 | DataType::Int64 | DataType::Float64) => Ok(DataType::Float64),
            _ => Err(unsupported_type("avg", input)),
        },
        AggFunc::Sum => match input {
            Some(DataType::Int32 | DataType::Int64) => Ok(DataType::Int64),
            Some(DataType::Float64) => Ok(DataType::Float64),
            _ => Err(unsupported_type("sum", input)),
        },
        AggFunc::Min | AggFunc::Max => match input {
            Some(ty @ (DataType::Int32 | DataType::Int64 | DataType::Float64)) => Ok(ty.clone()),
            _ => Err(unsupported_type(func.output_name(), input)),
        },
    }
}

fn unsupported_type(func: &str, input: Option<&DataType>) -> CoreError {
    CoreError::Unsupported(format!("{func} over input type {input:?} is not supported"))
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
    /// Grouped aggregate over `key`, emitting one row of `key ++ aggs` per key.
    GroupAggregate {
        input: Box<Plan>,
        key: Vec<usize>,
        aggs: Vec<AggSpec>,
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

/// Whether evaluating the plan may emit retractions (negative diffs).
///
/// A grouped aggregate retracts the previous row of a key when its state
/// changes, and a tumbling window retracts rows that leave the window, so both
/// are treated as retract-producing. Append-only sinks must refuse such a plan
/// before any write or ingestion.
pub fn may_retract(plan: &Plan) -> bool {
    match plan {
        Plan::GroupAggregate { .. } | Plan::TumbleCount { .. } => true,
        Plan::Filter { input, .. } | Plan::Project { input, .. } => may_retract(input),
        Plan::Join { left, right, .. } => may_retract(left) || may_retract(right),
        Plan::Source(_) => false,
    }
}

/// Does the plan contain any window?
pub fn has_window(plan: &Plan) -> bool {
    match plan {
        Plan::TumbleCount { .. } => true,
        Plan::Filter { input, .. }
        | Plan::Project { input, .. }
        | Plan::GroupAggregate { input, .. } => has_window(input),
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
        | Plan::GroupAggregate { input, .. }
        | Plan::TumbleCount { input, .. } => collect_sources(input, out),
        Plan::Join { left, right, .. } => {
            collect_sources(left, out);
            collect_sources(right, out);
        }
    }
}
