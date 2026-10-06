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

/// Arity at a `Source`: known only for the input actually being pushed (`pushed`),
/// or when validating detached from data (`pushed` is `None`, all sources assumed
/// to have `arity`). Other sources report an unknown arity, so their branches are
/// not checked against a row they do not read.
fn source_arity(arity: Option<usize>, id: InputId, pushed: Option<InputId>) -> Option<usize> {
    if pushed.is_none() || pushed == Some(id) {
        arity
    } else {
        None
    }
}

/// Validate that every index referenced by `plan` is in range, and that every
/// `Source` is a registered input.
///
/// `arity` is the length of the row fed to `plan` and `pushed` the input that row
/// came from; both are `None` when validating a plan detached from data. With a
/// join, only the branch reading `pushed` sees a known arity, so indices of one
/// side are never compared against the other side's arity. The returned arity is
/// the output arity of `plan`, and is `None` unless it is grounded in `pushed`.
pub(super) fn validate(
    plan: &Plan,
    arity: Option<usize>,
    inputs: &HashSet<InputId>,
    pushed: Option<InputId>,
) -> Result<Option<usize>, CoreError> {
    match plan {
        Plan::Source(id) => {
            if !inputs.contains(id) {
                return Err(CoreError::Unsupported(format!("unknown input {id:?}")));
            }
            Ok(source_arity(arity, *id, pushed))
        }
        Plan::Filter { input, pred } => {
            let arity = validate(input, arity, inputs, pushed)?;
            let col = match pred {
                Predicate::Eq(col, _) | Predicate::Gt(col, _) => *col,
            };
            check_col(col, arity, "filter")?;
            Ok(arity)
        }
        Plan::Project { input, cols } => {
            let arity = validate(input, arity, inputs, pushed)?;
            check_cols(cols, arity, "project")?;
            Ok(arity.map(|_| cols.len()))
        }
        Plan::GroupCount { input, key } => {
            let arity = validate(input, arity, inputs, pushed)?;
            check_cols(key, arity, "group key")?;
            Ok(arity.map(|_| key.len() + 1))
        }
        Plan::Join {
            left,
            right,
            left_key,
            right_key,
        } => {
            if left_key.len() != right_key.len() {
                return Err(CoreError::Unsupported("join key arity mismatch".into()));
            }
            let left_arity = validate(left, arity, inputs, pushed)?;
            let right_arity = validate(right, arity, inputs, pushed)?;
            check_cols(left_key, left_arity, "join left key")?;
            check_cols(right_key, right_arity, "join right key")?;
            Ok(left_arity.zip(right_arity).map(|(l, r)| l + r))
        }
    }
}

/// Validate every row in `batch`, which is a batch for `input`, against `plan`.
pub(super) fn validate_rows(
    plan: &Plan,
    batch: &ChangeBatch,
    inputs: &HashSet<InputId>,
    input: InputId,
) -> Result<(), CoreError> {
    for (row, _) in &batch.rows {
        validate(plan, Some(row.0.len()), inputs, Some(input))?;
    }
    Ok(())
}
