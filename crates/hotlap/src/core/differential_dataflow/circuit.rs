//! Plan validation, compilation, and batch feeding for the differential core.

use differential_dataflow::VecCollection;
use timely::worker::Worker;

use super::{CoreError, MAX_DRAIN_STEPS, ViewState};
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
/// length `arity`. `arity` is `None` only while scanning a plan detached from data.
pub(super) fn validate(plan: &Plan, arity: Option<usize>) -> Result<Option<usize>, CoreError> {
    match plan {
        Plan::Scan => Ok(arity),
        Plan::Filter { input, pred } => {
            let arity = validate(input, arity)?;
            let col = match pred {
                Predicate::Eq(col, _) | Predicate::Gt(col, _) => *col,
            };
            check_col(col, arity, "filter")?;
            Ok(arity)
        }
        Plan::Project { input, cols } => {
            let arity = validate(input, arity)?;
            for &col in cols {
                check_col(col, arity, "project")?;
            }
            Ok(Some(cols.len()))
        }
        Plan::GroupCount { input, key } => {
            let arity = validate(input, arity)?;
            for &col in key {
                check_col(col, arity, "group key")?;
            }
            Ok(Some(key.len() + 1))
        }
    }
}

/// Validate that every row in `batch` is in range for `plan` (schemas are
/// row-shaped, not declared in SP1a).
fn validate_rows(plan: &Plan, batch: &ChangeBatch) -> Result<(), CoreError> {
    for (row, _) in &batch.rows {
        validate(plan, Some(row.0.len()))?;
    }
    Ok(())
}

/// Advance logical time monotonically and feed the batch into the view's input.
fn feed(vs: &mut ViewState, batch: &ChangeBatch) {
    let time = vs.next_time;
    vs.next_time += 1;

    vs.input.advance_to(time);
    for (row, diff) in &batch.rows {
        vs.input.update(row.clone(), *diff as isize);
    }
    // Advance past the batch so DD consolidates and the output frontier moves.
    vs.input.advance_to(time + 1);
    vs.input.flush();
}

/// Drain until the output probe is past the batch time, ensuring every output
/// update at that time has been observed before a snapshot reads the Z-set.
fn drain(worker: &mut Worker, vs: &mut ViewState) -> Result<(), CoreError> {
    let mut steps = 0;
    while vs.probe.less_than(vs.input.time()) {
        if steps >= MAX_DRAIN_STEPS {
            return Err(CoreError::Unsupported(format!(
                "output frontier did not advance after {MAX_DRAIN_STEPS} steps"
            )));
        }
        worker.step();
        steps += 1;
    }
    Ok(())
}

/// Validate, feed and drain a single batch into the view's live session.
pub(super) fn push_batch(
    worker: &mut Worker,
    vs: &mut ViewState,
    batch: &ChangeBatch,
) -> Result<(), CoreError> {
    validate_rows(&vs.plan, batch)?;
    feed(vs, batch);
    drain(worker, vs)
}

/// Compile a linear plan over a collection of rows. Infallible: bounds are checked
/// by [`validate`] when batches are pushed, and unsupported roots are rejected by
/// `build_view`.
pub(super) fn compile<'scope>(
    coll: VecCollection<'scope, u64, Row, isize>,
    plan: &Plan,
) -> VecCollection<'scope, u64, Row, isize> {
    match plan {
        Plan::Scan => coll,
        Plan::Filter { input, pred } => {
            let pred = pred.clone();
            compile(coll, input).filter(move |row| pred.eval(row))
        }
        Plan::Project { input, cols } => {
            let cols = cols.clone();
            compile(coll, input)
                .map(move |row| Row(cols.iter().map(|&col| row.0[col].clone()).collect()))
        }
        Plan::GroupCount { input, key } => {
            let key = key.clone();
            compile(coll, input)
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
