//! Stateful node helpers and node evaluation dispatch.

use arrow::compute::{concat, concat_batches};

use crate::arrange::KeyedArrangement;
use crate::error::EngineError;
use crate::keys::KeyConverter;
use crate::ops::{GroupCount, Join, filter, project};
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
            Node::Group { input, key, state } => group_eval(input, key, state, ctx),
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
        ctx.rows += ctx.delta.len() as u64;
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
            ctx.rows += z.len() as u64;
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
        Some(z) => {
            ctx.rows += z.len() as u64;
            Ok(Some(project(&z, cols)?))
        }
    }
}

/// Applies the child's delta to the retained group-count arrangement.
fn group_eval(
    input: &mut Node,
    key: &[usize],
    state: &mut Option<Box<GroupState>>,
    ctx: &mut EvalCtx,
) -> Result<Option<ZSetBatch>, EngineError> {
    match input.eval(ctx)? {
        None => Ok(None),
        Some(z) => {
            ctx.rows += z.len() as u64;
            group_apply(state, key, z)
        }
    }
}

/// Feeds the child's delta to the retained tumbling-window reducer.
fn window_eval(
    input: &mut Node,
    reducer: &mut crate::ops::TumbleCount,
    ctx: &mut EvalCtx,
) -> Result<Option<ZSetBatch>, EngineError> {
    match input.eval(ctx)? {
        None => Ok(None),
        Some(z) => {
            ctx.rows += z.len() as u64;
            Ok(Some(reducer.apply(&z, ctx.watermark)?))
        }
    }
}

/// State feeding one group-count node once its input schema is known.
pub(super) struct GroupState {
    arrangement: KeyedArrangement,
    keys: KeyConverter,
    reducer: GroupCount,
}

/// Applies the group-count delta to its retained arrangement.
fn group_apply(
    state: &mut Option<Box<GroupState>>,
    key: &[usize],
    z: ZSetBatch,
) -> Result<Option<ZSetBatch>, EngineError> {
    if state.is_none() {
        let arrangement = KeyedArrangement::new(z.schema(), key)?;
        let keys = KeyConverter::new(z.schema().as_ref(), key)?;
        *state = Some(Box::new(GroupState {
            arrangement,
            keys,
            reducer: GroupCount::new(),
        }));
    }
    let group = state
        .as_mut()
        .ok_or_else(|| EngineError::Infrastructure("group state missing".to_string()))?;
    group.arrangement.apply(&z, &group.keys)?;
    Ok(Some(group.reducer.apply(&group.arrangement)?))
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
        ctx.rows += z.len() as u64;
        *left_pending = accumulate(left_pending.take(), &z)?;
    }
    if let Some(z) = right.eval(ctx)? {
        ctx.rows += z.len() as u64;
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
