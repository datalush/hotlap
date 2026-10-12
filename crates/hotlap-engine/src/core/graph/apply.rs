//! Stateful node helpers and node evaluation dispatch.

use arrow::compute::{concat, concat_batches};

use crate::error::EngineError;
use crate::ops::{GroupAggregate, Join, TumbleCount, filter, project};
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
    pub(super) fn memory_usage(
        &self,
        report: &mut super::super::memory::MemoryReport,
        allocations: &mut std::collections::HashSet<(usize, usize)>,
        external_ranges: &mut std::collections::HashMap<usize, usize>,
    ) -> Result<(), EngineError> {
        match self {
            Node::Source(_) => report.mark_covered("source_nodes"),
            Node::Filter { input, pred } => {
                report.mark_covered("filters");
                report.add_estimate(std::mem::size_of_val(pred))?;
                input.memory_usage(report, allocations, external_ranges)?;
            }
            Node::Project { input, cols } => {
                report.mark_covered("projections");
                report.add_capacity(super::super::memory::checked_capacity_bytes(
                    cols.capacity(),
                    std::mem::size_of::<usize>(),
                )?)?;
                input.memory_usage(report, allocations, external_ranges)?;
            }
            Node::Group { input, reducer } => {
                report.mark_covered("aggregates");
                report.add_group(reducer.memory_usage()?)?;
                if reducer.has_group_map_storage() {
                    report.mark_unknown("aggregate_hash_map_allocator_overhead");
                }
                if reducer.has_minmax_state() {
                    report.mark_unknown("minmax_btree_allocator_overhead");
                }
                input.memory_usage(report, allocations, external_ranges)?;
            }
            Node::Joined {
                left,
                right,
                join,
                left_pending,
                right_pending,
            } => {
                report.mark_covered("joins");
                let (payload, capacity, estimate) = join.memory_usage()?;
                report.add_join(payload, capacity, estimate)?;
                if join.is_initialized() {
                    report.mark_unknown("join_converter_and_schema_metadata");
                }
                if join.has_retained_rows() {
                    report.mark_unknown("join_owned_row_capacity");
                }
                if join.has_arrangement_storage() {
                    report.mark_unknown("join_outer_hash_map_allocator_overhead");
                    report.mark_unknown("join_many_payload_hash_map_allocator_overhead");
                    report.mark_unknown("join_singleton_box_allocator_overhead");
                }
                for pending in [left_pending, right_pending].into_iter().flatten() {
                    report.mark_unknown("join_pending_batch_metadata");
                    report.add_arrow_batch(&pending.batch, allocations, external_ranges)?;
                    report.add_arrow_array(&pending.diff, allocations, external_ranges)?;
                }
                left.memory_usage(report, allocations, external_ranges)?;
                right.memory_usage(report, allocations, external_ranges)?;
            }
            Node::Window { input, reducer } => {
                report.mark_covered("windows");
                if reducer.has_open_windows() {
                    report.mark_unknown("window_owned_row_capacity");
                    report.mark_unknown("window_btree_allocator_overhead");
                }
                if reducer.is_initialized() {
                    report.mark_unknown("window_converter_and_schema_metadata");
                }
                let (payload, capacity, estimate) = reducer.memory_usage()?;
                report.add_window(payload, capacity, estimate)?;
                input.memory_usage(report, allocations, external_ranges)?;
            }
        }
        Ok(())
    }

    /// Applies this node's operator to its inputs' deltas.
    pub(super) fn eval(&mut self, ctx: &mut EvalCtx) -> Result<Option<ZSetBatch>, EngineError> {
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

    /// Sums `late_closed_dropped` over every window operator in this subtree.
    pub(super) fn window_late_closed_dropped(&self) -> u64 {
        match self {
            Node::Source(_) => 0,
            Node::Window { input, reducer } => {
                input.window_late_closed_dropped() + reducer.late_closed_dropped()
            }
            Node::Filter { input, .. }
            | Node::Project { input, .. }
            | Node::Group { input, .. } => input.window_late_closed_dropped(),
            Node::Joined { left, right, .. } => {
                left.window_late_closed_dropped() + right.window_late_closed_dropped()
            }
        }
    }

    /// Sums `open_windows` over every window operator in this subtree.
    pub(super) fn windows_open(&self) -> u64 {
        match self {
            Node::Source(_) => 0,
            Node::Window { input, reducer } => input.windows_open() + reducer.open_windows(),
            Node::Filter { input, .. }
            | Node::Project { input, .. }
            | Node::Group { input, .. } => input.windows_open(),
            Node::Joined { left, right, .. } => left.windows_open() + right.windows_open(),
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
    reducer: &mut GroupAggregate,
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
