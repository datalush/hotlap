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
    pub(super) fn rebuild_derived(
        &mut self,
        schemas: &std::collections::HashMap<hotlap_core::InputId, arrow::datatypes::SchemaRef>,
    ) -> Result<(), EngineError> {
        match self {
            Node::Group {
                input,
                reducer,
                cut_plan: Some(plan),
                outer_project,
                registered_demand,
            } => {
                let (joined, project) = match input.as_mut() {
                    Node::Project { input, cols }
                        if matches!(input.as_ref(), Node::Joined { .. }) =>
                    {
                        (input.as_mut(), Some(cols.as_slice()))
                    }
                    other => (other, None),
                };
                if let Node::Joined { join, .. } = joined {
                    let recognition = hotlap_core::plan_normalize::recognize(plan, schemas);
                    if let hotlap_core::plan_normalize::Recognition::Eligible(mapping) = recognition
                        && let Some((left_schema, right_schema)) = join.source_schemas_for_cut()
                        && let Ok(demand) = crate::ops::join::factorized::demand::compile_demand(
                            &mapping,
                            reducer.aggregates(),
                            &left_schema,
                            &right_schema,
                        )
                    {
                        join.register_demand(demand.clone())?;
                        *registered_demand = Some(demand);
                        let joined_schema = join.joined_schema_for_cut()?;
                        let projection = outer_project.as_deref().or(project);
                        let reducer_schema = if let Some(columns) = projection {
                            project_batch_schema(&joined_schema, columns)?
                        } else {
                            joined_schema
                        };
                        reducer.initialize_cut(reducer_schema)?;
                        let (Some(left), Some(right)) = (join.left_for_cut(), join.right_for_cut())
                        else {
                            return Err(EngineError::Infrastructure(
                                "restored join arrangements are missing".into(),
                            ));
                        };
                        let initialized = if let Some(columns) = projection {
                            reducer.initialize_maintained_cut_projected(
                                &mapping, left, right, columns,
                            )?
                        } else {
                            reducer.initialize_maintained_cut(&mapping, left, right)?
                        };
                        if initialized {
                            reducer.validate_maintained_history()?;
                        }
                    }
                }
                input.rebuild_derived(schemas)
            }
            Node::Group { input, .. }
            | Node::Filter { input, .. }
            | Node::Project { input, .. }
            | Node::Window { input, .. } => input.rebuild_derived(schemas),
            Node::Joined { left, right, .. } => {
                left.rebuild_derived(schemas)?;
                right.rebuild_derived(schemas)
            }
            Node::Source(_) => Ok(()),
        }
    }

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
            Node::Group { input, reducer, .. } => {
                report.mark_covered("aggregates");
                // The reducer now has one stable graph-owned heap allocation;
                // include its object payload in the structural estimate, but
                // not allocator bookkeeping or a claim of exact heap bytes.
                report.add_estimate(std::mem::size_of_val(reducer.as_ref()))?;
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
            Node::Project { input, cols } => {
                let optimized = matches!(
                    input.as_ref(),
                    Node::Filter { input, .. } if matches!(input.as_ref(), Node::Source(_))
                );
                if optimized && should_fuse_filter_project() {
                    let Node::Filter { input, pred } = input.as_mut() else {
                        unreachable!();
                    };
                    #[cfg(test)]
                    super::note_selected_filter_project();
                    filter_project_eval(input, pred, cols, ctx)
                } else {
                    project_eval(input, cols, ctx)
                }
            }
            Node::Group {
                input,
                reducer,
                cut_plan,
                outer_project,
                registered_demand,
            } => group_eval(
                input,
                reducer,
                cut_plan,
                outer_project,
                registered_demand,
                ctx,
            ),
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

fn should_fuse_filter_project() -> bool {
    #[cfg(test)]
    {
        !super::FORCE_DENSE_FILTER_PROJECT.with(|force| force.get())
    }
    #[cfg(not(test))]
    {
        true
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

fn filter_project_eval(
    input: &mut Node,
    pred: &Predicate,
    cols: &[usize],
    ctx: &mut EvalCtx,
) -> Result<Option<ZSetBatch>, EngineError> {
    let Some(z) = input.eval(ctx)? else {
        return Ok(None);
    };
    let mask = pred.eval(&z.batch)?;
    Ok(Some(crate::ops::filter_project::filter_project(
        &z, &mask, cols,
    )?))
}

fn project_batch_schema(
    schema: &arrow::datatypes::SchemaRef,
    columns: &[usize],
) -> Result<arrow::datatypes::SchemaRef, EngineError> {
    Ok(project(&ZSetBatch::empty(schema.clone()), columns)?.schema())
}

/// Feeds the child's delta to the retained, delta-incremental group count.
fn group_eval(
    input: &mut Node,
    reducer: &mut GroupAggregate,
    cut_plan: &mut Option<Box<hotlap_core::Plan>>,
    outer_project: &Option<Vec<usize>>,
    registered_demand: &mut Option<crate::ops::join::factorized::demand::Demand>,
    ctx: &mut EvalCtx,
) -> Result<Option<ZSetBatch>, EngineError> {
    if cut_plan.is_some() {
        let (joined_input, runtime_project) = match &mut *input {
            Node::Project { input, cols } if matches!(input.as_ref(), Node::Joined { .. }) => {
                (input.as_mut(), Some(cols.as_slice()))
            }
            other => (other, None),
        };
        if let Node::Joined {
            left,
            right,
            join,
            left_pending,
            right_pending,
        } = joined_input
        {
            if let Some((left_delta, right_delta)) = buffer_join_inputs(
                left.as_mut(),
                right.as_mut(),
                left_pending,
                right_pending,
                ctx,
            )? {
                let Some(plan) = cut_plan.as_deref() else {
                    unreachable!()
                };
                let recognition = hotlap_core::plan_normalize::recognize(plan, ctx.schemas);
                #[cfg(test)]
                let force_dense = super::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.get());
                #[cfg(not(test))]
                let force_dense = false;
                if let hotlap_core::plan_normalize::Recognition::Eligible(mapping) = &recognition {
                    let joined_schema = join.cut_schema(&left_delta, &right_delta)?;
                    let reducer_schema = if let Some(cols) = outer_project {
                        project(&ZSetBatch::empty(joined_schema.clone()), cols)?.schema()
                    } else if let Some(cols) = runtime_project {
                        project(&ZSetBatch::empty(joined_schema.clone()), cols)?.schema()
                    } else {
                        joined_schema.clone()
                    };
                    if registered_demand.is_none() {
                        *registered_demand = crate::ops::join::factorized::demand::compile_demand(
                            mapping,
                            reducer.aggregates(),
                            &left_delta.schema(),
                            &right_delta.schema(),
                        )
                        .ok();
                    }
                    if let Some(demand) = registered_demand.as_ref() {
                        join.register_demand(demand.clone())?;
                        let sources = join.prepare_source_updates(&left_delta, &right_delta)?;
                        if force_dense {
                            let projection = outer_project.as_deref().or(runtime_project);
                            let memo = crate::ops::join::factorized::certified_consume::prepare_dense_metadata_with_sources(
                                join,
                                reducer,
                                crate::ops::join::factorized::certified_consume::GroupedUpdate {
                                    mapping,
                                    left_delta: &left_delta,
                                    right_delta: &right_delta,
                                    output_schema: reducer_schema,
                                    projection,
                                },
                                &sources,
                            )?;
                            let joined = join.apply(&left_delta, &right_delta)?;
                            #[cfg(test)]
                            record_dense_join_materialization(&joined);
                            let projected = if let Some(columns) = projection {
                                project(&joined, columns)?
                            } else {
                                joined
                            };
                            let output = reducer.apply(&projected)?;
                            if let Some(memo) = memo {
                                reducer.commit_maintained_memo(memo);
                            }
                            *left_pending = None;
                            *right_pending = None;
                            return Ok(Some(output));
                        }
                        let update =
                            crate::ops::join::factorized::certified_consume::GroupedUpdate {
                                mapping,
                                left_delta: &left_delta,
                                right_delta: &right_delta,
                                output_schema: reducer_schema,
                                projection: outer_project.as_deref().or(runtime_project),
                            };
                        let decision =
                            crate::ops::join::factorized::certified_consume::prepare_grouped_update_with_sources(
                                join,
                                reducer,
                                update,
                                &sources,
                            )?;
                        match decision {
                            crate::ops::join::factorized::certified_consume::CertifiedDecision::Certified(
                                prepared,
                            ) => {
                                let output = prepared.output().clone();
                                prepared.commit_group_state(reducer);
                                join.commit_source_updates(sources);
                                *left_pending = None;
                                *right_pending = None;
                                #[cfg(test)]
                                super::FACTORIZED_CUTS.with(|n| n.set(n.get() + 1));
                                return Ok(Some(output));
                            }
                            needs_rows @ crate::ops::join::factorized::certified_consume::CertifiedDecision::NeedsRows(
                                _,
                            ) => {
                                let flat = crate::ops::join::factorized::flat::materialize_flat_delta(
                                    join,
                                    &sources,
                                    &left_delta,
                                    &right_delta,
                                    joined_schema,
                                    crate::ops::join::factorized::ConsumerCaps::DEFAULT_FLAT_CHUNK_ROWS,
                                )?;
                                #[cfg(test)]
                                record_dense_join_materialization(&flat);
                                let flat = if let Some(columns) =
                                    outer_project.as_deref().or(runtime_project)
                                {
                                    project(&flat, columns)?
                                } else {
                                    flat
                                };
                                let output = reducer.apply(&flat)?;
                                needs_rows.commit_dense_metadata(reducer);
                                join.commit_source_updates(sources);
                                *left_pending = None;
                                *right_pending = None;
                                #[cfg(test)]
                                super::FLAT_ROWS_CUTS.with(|count| count.set(count.get() + 1));
                                return Ok(Some(output));
                            }
                        }
                    }
                }
                let output = join.apply(&left_delta, &right_delta)?;
                #[cfg(test)]
                record_dense_join_materialization(&output);
                *left_pending = None;
                *right_pending = None;
                let output = if let Some(cols) = outer_project.as_deref().or(runtime_project) {
                    project(&output, cols)?
                } else {
                    output
                };
                return Ok(Some(reducer.apply(&output)?));
            }
            return Ok(None);
        }
    }
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
    match buffer_join_inputs(left, right, left_pending, right_pending, ctx)? {
        Some((left, right)) => {
            let prepared = crate::ops::join::factorized::flat::prepare_flat_join_update(
                join,
                &left,
                &right,
                crate::ops::join::factorized::ConsumerCaps::DEFAULT_FLAT_CHUNK_ROWS,
            )?;
            let (output, sources) = prepared.into_parts();
            #[cfg(test)]
            record_dense_join_materialization(&output);
            join.commit_source_updates(sources);
            *left_pending = None;
            *right_pending = None;
            #[cfg(test)]
            super::FLAT_ROWS_CUTS.with(|count| count.set(count.get() + 1));
            Ok(Some(output))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
fn record_dense_join_materialization(output: &ZSetBatch) {
    use arrow::array::Array;

    let rows = output.len() as u64;
    let bytes = output
        .batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size())
        .chain(std::iter::once(output.diff().get_array_memory_size()))
        .fold(0usize, usize::saturating_add) as u64;
    super::DENSE_JOIN_MATERIALIZATIONS.with(|count| count.set(count.get() + 1));
    super::DENSE_JOIN_ROWS.with(|count| count.set(count.get().saturating_add(rows)));
    super::DENSE_JOIN_BYTES.with(|count| count.set(count.get().saturating_add(bytes)));
}

fn buffer_join_inputs(
    left: &mut Node,
    right: &mut Node,
    left_pending: &mut Option<ZSetBatch>,
    right_pending: &mut Option<ZSetBatch>,
    ctx: &mut EvalCtx,
) -> Result<Option<(ZSetBatch, ZSetBatch)>, EngineError> {
    if let Some(z) = left.eval(ctx)? {
        *left_pending = accumulate(left_pending.take(), &z)?;
    }
    if let Some(z) = right.eval(ctx)? {
        *right_pending = accumulate(right_pending.take(), &z)?;
    }
    match (left_pending.as_ref(), right_pending.as_ref()) {
        (Some(left), Some(right)) => Ok(Some((left.clone(), right.clone()))),
        _ => Ok(None),
    }
}
