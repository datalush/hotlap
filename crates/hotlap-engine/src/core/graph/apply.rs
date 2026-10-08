//! Stateful node helpers and node evaluation dispatch.

use arrow::compute::{concat, concat_batches};

use crate::error::EngineError;
use crate::ops::{GroupCount, Join, TumbleCount, filter, project};
use hotlap_core::{InputId, Predicate, ZSetBatch};

use super::{EvalCtx, Node};

/// Concatenates two Z-sets over the same schema.
pub(super) fn merge(a: &ZSetBatch, b: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
    if a.schema() != b.schema() {
        return Err(EngineError::Unsupported(
            "view delta schema changed between pushes".to_string(),
        ));
    }
    let batch = concat_batches(&a.schema(), [&a.batch, &b.batch])?;
    let diff = concat(&[a.diff.as_ref(), b.diff.as_ref()])?;
    Ok(ZSetBatch::new(batch, diff)?)
}

/// Accumulates `delta` into `current`, returning the merged Z-set.
pub(crate) fn accumulate(
    current: Option<ZSetBatch>,
    delta: &ZSetBatch,
) -> Result<Option<ZSetBatch>, EngineError> {
    match current {
        None => Ok(Some(delta.clone())),
        Some(current) => Ok(Some(merge(&current, delta)?)),
    }
}

impl Node {
    /// Applies this node's operator to its inputs' deltas.
    pub(super) fn eval(
        &mut self,
        ctx: &mut EvalCtx,
    ) -> Result<Option<ZSetBatch>, EngineError> {
        match self {
            Node::Source(id) => source_eval(*id, ctx),
            Node::Filter { input, pred } => filter_eval(input, pred, ctx),
            Node::Project { input, cols } => project_eval(input, cols, ctx),
            Node::Group { input, reducer } => group_eval(input, reducer, ctx),
            Node::Joined {
                left,
                right,
                join,
                left_pending,
                right_pending,
            } => join_apply(left, right, join.as_mut(), left_pending, right_pending, ctx),
            Node::Window { input, reducer } => window_eval(input, reducer, ctx),
        }
    }
}

/// Emits the pushed delta at its source, or an empty delta once its schema is
/// known; `None` while a sibling source's schema is still unknown.
fn source_eval(id: InputId, ctx: &mut EvalCtx) -> Result<Option<ZSetBatch>, EngineError> {
    if id == ctx.pushed {
        Ok(Some(ctx.delta.clone()))
    } else {
        Ok(ctx
            .schemas
            .get(&id)
            .map(|schema| ZSetBatch::empty(schema.clone())))
    }
}

/// Filters the child's delta, keeping the diff column aligned.
fn filter_eval(
    input: &mut Node,
    pred: &Predicate,
    ctx: &mut EvalCtx,
) -> Result<Option<ZSetBatch>, EngineError> {
    match input.eval(ctx)? {
        None => Ok(None),
        Some(z) => {
            let mask = pred.eval(&z.batch)?;
            Ok(Some(filter(&z, &mask)?))
        }
    }
}

/// Projects the child's delta columns.
fn project_eval(
    input: &mut Node,
    cols: &[usize],
    ctx: &mut EvalCtx,
) -> Result<Option<ZSetBatch>, EngineError> {
    match input.eval(ctx)? {
        None => Ok(None),
        Some(z) => Ok(Some(project(&z, cols)?)),
    }
}

/// Feeds the child's delta to the retained, delta-incremental group count.
fn group_eval(
    input: &mut Node,
    reducer: &mut GroupCount,
    ctx: &mut EvalCtx,
) -> Result<Option<ZSetBatch>, EngineError> {
    match input.eval(ctx)? {
        None => Ok(None),
        Some(z) => Ok(Some(reducer.apply(&z)?)),
    }
}

/// Feeds the child's delta to the retained tumbling-window reducer.
fn window_eval(
    input: &mut Node,
    reducer: &mut TumbleCount,
    ctx: &mut EvalCtx,
) -> Result<Option<ZSetBatch>, EngineError> {
    match input.eval(ctx)? {
        None => Ok(None),
        Some(z) => Ok(Some(reducer.apply(&z, ctx.watermark)?)),
    }
}

/// Applies both sides' deltas to the retained join, buffering a side whose
/// sibling schema is still unknown until the join can be initialized.
fn join_apply(
    left: &mut Node,
    right: &mut Node,
    join: &mut Join,
    left_pending: &mut Option<ZSetBatch>,
    right_pending: &mut Option<ZSetBatch>,
    ctx: &mut EvalCtx,
) -> Result<Option<ZSetBatch>, EngineError> {
    if let Some(z) = left.eval(ctx)? {
        *left_pending = accumulate(left_pending.take(), &z)?;
    }
    if let Some(z) = right.eval(ctx)? {
        *right_pending = accumulate(right_pending.take(), &z)?;
    }
    match (left_pending.as_ref(), right_pending.as_ref()) {
        (Some(left), Some(right)) => {
            let output = join.apply(left, right)?;
            *left_pending = None;
            *right_pending = None;
            Ok(Some(output))
        }
        _ => Ok(None),
    }
}
