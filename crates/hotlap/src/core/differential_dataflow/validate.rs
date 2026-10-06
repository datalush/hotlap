//! Plan validation: index bounds, source registration, and per-batch row checks.

use std::collections::HashSet;

use crate::core::{CoreError, InputId};
use crate::plan::{Plan, Predicate};
use crate::row::ChangeBatch;

/// Validate that `col` is in range for a child row of length `arity`.
fn check_col(col: usize, arity: Option<usize>, what: &str) -> Result<(), CoreError> {
    if let Some(n) = arity
        && col >= n
    {
        return Err(CoreError::Unsupported(format!(
            "{what} column {col} out of range (arity {n})"
        )));
    }
    Ok(())
}

/// Validate that every index in `cols` is in range for a row of length `arity`.
fn check_cols(cols: &[usize], arity: Option<usize>, what: &str) -> Result<(), CoreError> {
    for &col in cols {
        check_col(col, arity, what)?;
    }
    Ok(())
}

/// Validate both sides of a join and return the combined output arity.
fn validate_join(
    left: &Plan,
    right: &Plan,
    left_key: &[usize],
    right_key: &[usize],
    arity: Option<usize>,
    inputs: &HashSet<InputId>,
) -> Result<Option<usize>, CoreError> {
    let left_arity = validate(left, arity, inputs)?;
    let right_arity = validate(right, arity, inputs)?;
    check_cols(left_key, left_arity, "join left key")?;
    check_cols(right_key, right_arity, "join right key")?;
    Ok(left_arity.zip(right_arity).map(|(l, r)| l + r))
}

/// Validate that every index referenced by `plan` is in range for a child row of
/// length `arity`, and that every `Source` is a registered input. `arity` is
/// `None` while validating a plan detached from data.
pub(super) fn validate(
    plan: &Plan,
    arity: Option<usize>,
    inputs: &HashSet<InputId>,
) -> Result<Option<usize>, CoreError> {
    match plan {
        Plan::Source(id) => {
            if !inputs.contains(id) {
                return Err(CoreError::Unsupported(format!("unknown input {id:?}")));
            }
            Ok(arity)
        }
        Plan::Filter { input, pred } => {
            let arity = validate(input, arity, inputs)?;
            let col = match pred {
                Predicate::Eq(col, _) | Predicate::Gt(col, _) => *col,
            };
            check_col(col, arity, "filter")?;
            Ok(arity)
        }
        Plan::Project { input, cols } => {
            let arity = validate(input, arity, inputs)?;
            check_cols(cols, arity, "project")?;
            Ok(Some(cols.len()))
        }
        Plan::GroupCount { input, key } => {
            let arity = validate(input, arity, inputs)?;
            check_cols(key, arity, "group key")?;
            Ok(Some(key.len() + 1))
        }
        Plan::Join {
            left,
            right,
            left_key,
            right_key,
        } => validate_join(left, right, left_key, right_key, arity, inputs),
    }
}

/// Validate every row in `batch` against `plan`.
pub(super) fn validate_rows(
    plan: &Plan,
    batch: &ChangeBatch,
    inputs: &HashSet<InputId>,
) -> Result<(), CoreError> {
    for (row, _) in &batch.rows {
        validate(plan, Some(row.0.len()), inputs)?;
    }
    Ok(())
}
