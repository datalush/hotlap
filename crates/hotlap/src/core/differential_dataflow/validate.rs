//! Plan validation: index bounds, source registration, and per-batch row checks.

use std::collections::{HashMap, HashSet};

use crate::core::{CoreError, InputId};
use crate::plan::{Plan, Predicate};

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

/// Validate that every index referenced by `plan` is in range and that every
/// `Source` is a registered input, using the arity learned for each input.
///
/// `arities` maps each input that has received data to the row length observed
/// for it; a source whose arity is still unknown reports `None`, so indices that
/// depend on it are skipped until its data arrives. This includes indices *above*
/// a join: the join's output arity is unknown until both sides' arities are
/// known, at which point it becomes `left + right` and the indices beyond it are
/// checked. The returned arity is `plan`'s output arity, or `None` when it depends
/// on an input whose arity is not yet known.
pub(super) fn validate(
    plan: &Plan,
    arities: &HashMap<InputId, usize>,
    inputs: &HashSet<InputId>,
) -> Result<Option<usize>, CoreError> {
    match plan {
        Plan::Source(id) => {
            if !inputs.contains(id) {
                return Err(CoreError::Unsupported(format!("unknown input {id:?}")));
            }
            Ok(arities.get(id).copied())
        }
        Plan::Filter { input, pred } => {
            let arity = validate(input, arities, inputs)?;
            let col = match pred {
                Predicate::Eq(col, _) | Predicate::Gt(col, _) => *col,
            };
            check_col(col, arity, "filter")?;
            Ok(arity)
        }
        Plan::Project { input, cols } => {
            let arity = validate(input, arities, inputs)?;
            check_cols(cols, arity, "project")?;
            Ok(arity.map(|_| cols.len()))
        }
        Plan::GroupCount { input, key } => {
            let arity = validate(input, arities, inputs)?;
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
            let left_arity = validate(left, arities, inputs)?;
            let right_arity = validate(right, arities, inputs)?;
            check_cols(left_key, left_arity, "join left key")?;
            check_cols(right_key, right_arity, "join right key")?;
            Ok(left_arity.zip(right_arity).map(|(l, r)| l + r))
        }
        Plan::TumbleCount {
            input,
            key,
            time_col,
            size,
        } => {
            if *size <= 0 {
                return Err(CoreError::Unsupported("window size must be > 0".into()));
            }
            let arity = validate(input, arities, inputs)?;
            check_cols(key, arity, "window key")?;
            check_col(*time_col, arity, "window time")?;
            Ok(arity.map(|_| key.len() + 2))
        }
    }
}
