//! Persistent per-view operator graph: propagate only deltas through stateful
//! operators, which retain their state across pushes.

mod apply;
mod restore_validation;
mod snapshot;

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;

use hotlap_core::plan::sources;
use hotlap_core::{InputId, Plan, Predicate, ZSetBatch};

use crate::arrange::ArrangementHandle;
use crate::ops::{GroupAggregate, Join, TumbleCount};

pub(super) use apply::accumulate;

/// One operator in a view's graph.
enum Node {
    Source(InputId),
    Filter {
        input: Box<Node>,
        pred: Predicate,
    },
    Project {
        input: Box<Node>,
        cols: Vec<usize>,
    },
    Group {
        input: Box<Node>,
        reducer: Box<GroupAggregate>,
        cut_plan: Option<Box<Plan>>,
        outer_project: Option<Vec<usize>>,
        registered_demand: Option<crate::ops::join::factorized::demand::Demand>,
    },
    Joined {
        left: Box<Node>,
        right: Box<Node>,
        join: Box<Join>,
        left_pending: Option<ZSetBatch>,
        right_pending: Option<ZSetBatch>,
        left_pending_revision: Option<(u64, u64)>,
        right_pending_revision: Option<(u64, u64)>,
        shared: Option<SharedBinding>,
    },
    Window {
        input: Box<Node>,
        reducer: TumbleCount,
    },
}

/// Per-evaluation context: the pushed delta, known schemas, watermark.
struct EvalCtx<'data, 'frame, 'arena> {
    pushed: InputId,
    delta: &'data ZSetBatch,
    schemas: &'data HashMap<InputId, SchemaRef>,
    watermark: i64,
    source_revision: u64,
    share_candidate: bool,
    shared_frame: Option<&'frame mut crate::arrange::ArrangementFrame<'arena>>,
}

#[derive(Clone, Copy)]
struct SharedBinding {
    left: ArrangementHandle,
    right: ArrangementHandle,
}

#[cfg(test)]
thread_local! {
    pub(super) static FACTORIZED_CUTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static FORCE_DENSE_GROUP_JOIN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(super) static FORCE_DENSE_FILTER_PROJECT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(super) static SELECTED_FILTER_PROJECTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static DENSE_JOIN_MATERIALIZATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static DENSE_JOIN_ROWS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static DENSE_JOIN_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static FLAT_ROWS_CUTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static FORCE_PRIVATE_SHARED_ARRANGEMENTS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn note_selected_filter_project() -> bool {
    SELECTED_FILTER_PROJECTS.with(|count| {
        count.set(count.get() + 1);
        true
    })
}

#[cfg(test)]
pub(super) fn selected_filter_project_count() -> u64 {
    SELECTED_FILTER_PROJECTS.with(std::cell::Cell::get)
}

/// A persistent plan compiled to stateful nodes for one view.
pub(crate) struct ViewGraph {
    root: Node,
    sources: Vec<InputId>,
}

impl ViewGraph {
    /// Compiles `plan` into a graph; no operator state is created yet.
    pub(super) fn build(plan: &Plan) -> Self {
        Self {
            root: compile(plan),
            sources: sources(plan),
        }
    }

    /// Input ids this graph reads from.
    pub(super) fn sources(&self) -> &[InputId] {
        &self.sources
    }

    pub(super) fn release_shared(
        &mut self,
        arena: &mut crate::arrange::ArrangementArena,
    ) -> Result<(), crate::error::EngineError> {
        self.root.release_shared(arena)
    }

    #[cfg(test)]
    pub(super) fn root_shared_handles_for_test(
        &self,
    ) -> Option<(ArrangementHandle, ArrangementHandle)> {
        match &self.root {
            Node::Joined { shared, .. } => shared.map(|binding| (binding.left, binding.right)),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn root_join_sources_for_test(&self) -> Option<(InputId, InputId)> {
        match &self.root {
            Node::Joined { left, right, .. } => match (left.as_ref(), right.as_ref()) {
                (Node::Source(left), Node::Source(right)) => Some((*left, *right)),
                _ => None,
            },
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn root_private_arrangement_rows_for_test(&self) -> Option<(usize, usize)> {
        match &self.root {
            Node::Joined { join, shared, .. } if shared.is_none() => {
                join.arrangement_row_counts_for_test()
            }
            Node::Joined {
                shared: Some(_), ..
            } => Some((0, 0)),
            _ => None,
        }
    }

    /// Total deltas dropped by this graph's window operators because their
    /// window had already closed.
    pub(super) fn window_late_closed_dropped(&self) -> u64 {
        self.root.window_late_closed_dropped()
    }

    /// Number of tumbling windows still open across this graph's operators.
    pub(super) fn windows_open(&self) -> u64 {
        self.root.windows_open()
    }

    /// Evaluates the graph over the pushed `delta`, returning the view's output
    /// delta (if any).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn eval(
        &mut self,
        pushed: InputId,
        delta: &ZSetBatch,
        schemas: &HashMap<InputId, SchemaRef>,
        watermark: i64,
        _epoch: u64,
        source_revision: u64,
        share_candidate: bool,
        shared_frame: Option<&mut crate::arrange::ArrangementFrame<'_>>,
    ) -> Result<Option<ZSetBatch>, crate::error::EngineError> {
        let mut ctx = EvalCtx {
            pushed,
            delta,
            schemas,
            watermark,
            source_revision,
            share_candidate,
            shared_frame,
        };
        self.root.eval(&mut ctx)
    }

    pub(super) fn rebuild_derived(
        &mut self,
        schemas: &HashMap<InputId, SchemaRef>,
    ) -> Result<(), crate::error::EngineError> {
        self.root.rebuild_derived(schemas)
    }

    pub(super) fn memory_usage(
        &self,
        report: &mut super::memory::MemoryReport,
        allocations: &mut std::collections::HashSet<(usize, usize)>,
        external_ranges: &mut std::collections::HashMap<usize, usize>,
    ) -> Result<(), crate::error::EngineError> {
        self.root.memory_usage(report, allocations, external_ranges)
    }
}

/// Recursively compiles a plan into nodes.
fn compile(plan: &Plan) -> Node {
    match plan {
        Plan::Source(id) => Node::Source(*id),
        Plan::Filter { input, pred } => Node::Filter {
            input: Box::new(compile(input)),
            pred: pred.clone(),
        },
        Plan::Project { input, cols } => Node::Project {
            input: Box::new(compile(input)),
            cols: cols.clone(),
        },
        Plan::GroupAggregate { input, key, aggs } => Node::Group {
            input: Box::new(compile(input)),
            reducer: Box::new(GroupAggregate::new(key, aggs.clone())),
            cut_plan: is_factorized_shape(input).then(|| Box::new(plan.clone())),
            registered_demand: None,
            outer_project: match input.as_ref() {
                Plan::Project { cols, input } if matches!(input.as_ref(), Plan::Join { .. }) => {
                    Some(cols.clone())
                }
                _ => None,
            },
        },
        Plan::Join {
            left,
            right,
            left_key,
            right_key,
        } => Node::Joined {
            left: Box::new(compile(left)),
            right: Box::new(compile(right)),
            join: Box::new(Join::new(left_key, right_key)),
            left_pending: None,
            right_pending: None,
            left_pending_revision: None,
            right_pending_revision: None,
            shared: None,
        },
        Plan::TumbleCount {
            input,
            key,
            time_col,
            size,
        } => Node::Window {
            input: Box::new(compile(input)),
            reducer: TumbleCount::new(key, *time_col, *size),
        },
    }
}

fn is_factorized_shape(plan: &Plan) -> bool {
    match plan {
        Plan::Join { left, right, .. } => is_relation_shape(left) && is_relation_shape(right),
        Plan::Project { input, .. } => {
            matches!(input.as_ref(), Plan::Join { left, right, .. } if is_relation_shape(left) && is_relation_shape(right))
        }
        _ => false,
    }
}

fn is_relation_shape(plan: &Plan) -> bool {
    match plan {
        Plan::Source(_) => true,
        Plan::Project { input, .. } => matches!(input.as_ref(), Plan::Source(_)),
        _ => false,
    }
}

#[cfg(test)]
mod layout_tests {
    use super::Node;
    use crate::ops::GroupAggregate;
    use crate::ops::join::factorized::certified_consume::CertifiedDecision;

    #[test]
    fn graph_node_is_smaller_than_group_reducer() {
        let node = std::mem::size_of::<Node>();
        let reducer = std::mem::size_of::<GroupAggregate>();
        eprintln!("layout baseline/candidate: Node={node}, GroupAggregate={reducer}");
        assert!(
            node < reducer,
            "Node={node} retains GroupAggregate inline ({reducer})"
        );
    }

    #[test]
    fn certified_decision_stays_below_test_owned_payload_budget() {
        let decision = std::mem::size_of::<CertifiedDecision>();
        eprintln!("layout baseline/candidate: CertifiedDecision={decision}");
        assert!(decision <= 192, "CertifiedDecision={decision} bytes");
    }
}
