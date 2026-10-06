//! Plan validation, compilation, and batch feeding for the differential core.

use std::collections::{HashMap, HashSet};

use differential_dataflow::VecCollection;
use differential_dataflow::input::InputSession;
use timely::worker::Worker;

use super::MAX_DRAIN_STEPS;
use super::session::ViewState;
use crate::core::{CoreError, InputId, ViewId};
use crate::plan::{Plan, Predicate};
use crate::row::{ChangeBatch, Row, Scalar};

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
                return Err(CoreError::Unsupported(format!("unregistered input {id:?}")));
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
            for &col in cols {
                check_col(col, arity, "project")?;
            }
            Ok(Some(cols.len()))
        }
        Plan::GroupCount { input, key } => {
            let arity = validate(input, arity, inputs)?;
            for &col in key {
                check_col(col, arity, "group key")?;
            }
            Ok(Some(key.len() + 1))
        }
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

/// Collect the input ids a plan reads from, in traversal order.
pub(super) fn sources(plan: &Plan) -> Vec<InputId> {
    let mut out = Vec::new();
    collect_sources(plan, &mut out);
    out
}

fn collect_sources(plan: &Plan, out: &mut Vec<InputId>) {
    match plan {
        Plan::Source(id) => out.push(*id),
        Plan::Filter { input, .. } | Plan::Project { input, .. } => collect_sources(input, out),
        Plan::GroupCount { input, .. } => collect_sources(input, out),
    }
}

/// Advance logical time monotonically and feed the batch into an input session.
pub(super) fn feed(session: &mut InputSession<u64, Row, isize>, batch: &ChangeBatch) {
    let time = *session.time();
    session.advance_to(time);
    for (row, diff) in &batch.rows {
        session.update(row.clone(), *diff as isize);
    }
    // Advance past the batch so DD consolidates and the output frontier moves.
    session.advance_to(time + 1);
    session.flush();
}

/// Fail if draining has consumed the step budget without the output frontier
/// advancing, turning a stuck dataflow into an error instead of a hang.
fn ensure_drain_budget(steps: usize) -> Result<(), CoreError> {
    if steps >= MAX_DRAIN_STEPS {
        return Err(CoreError::Infrastructure(format!(
            "output frontier did not advance after {MAX_DRAIN_STEPS} steps"
        )));
    }
    Ok(())
}

/// Step until every consuming view's probe is at or past `target`, ensuring all
/// output updates at that time are observed before a snapshot reads the Z-set.
pub(super) fn drain(
    worker: &mut Worker,
    views: &HashMap<ViewId, ViewState>,
    consumers: &[ViewId],
    target: u64,
) -> Result<(), CoreError> {
    let mut steps = 0;
    while consumers.iter().any(|v| views[v].probe.less_than(&target)) {
        ensure_drain_budget(steps)?;
        worker.step();
        steps += 1;
    }
    Ok(())
}

/// Compile a plan over the shared collections. Infallible: bounds are checked by
/// [`validate`] when batches are pushed, and unsupported plans are rejected when
/// a view is built.
pub(super) fn compile<'scope>(
    collections: &HashMap<InputId, VecCollection<'scope, u64, Row, isize>>,
    plan: &Plan,
) -> VecCollection<'scope, u64, Row, isize> {
    match plan {
        Plan::Source(id) => collections[id].clone(),
        Plan::Filter { input, pred } => {
            let pred = pred.clone();
            compile(collections, input).filter(move |row| pred.eval(row))
        }
        Plan::Project { input, cols } => {
            let cols = cols.clone();
            compile(collections, input)
                .map(move |row| Row(cols.iter().map(|&col| row.0[col].clone()).collect()))
        }
        Plan::GroupCount { input, key } => {
            let key = key.clone();
            compile(collections, input)
                .map(move |row| Row(key.iter().map(|&col| row.0[col].clone()).collect()))
                .count()
                .map(|(k, count)| {
                    let mut out = k.0;
                    out.push(Scalar::I64(count as i64));
                    Row(out)
                })
        }
    }
}
