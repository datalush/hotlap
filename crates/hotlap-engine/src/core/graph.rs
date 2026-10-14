//! Persistent per-view operator graph: propagate only deltas through stateful
//! operators, which retain their state across pushes.

mod apply;
mod snapshot;

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;

use hotlap_core::plan::sources;
use hotlap_core::{InputId, Plan, Predicate, ZSetBatch};

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
        reducer: GroupAggregate,
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
    },
    Window {
        input: Box<Node>,
        reducer: TumbleCount,
    },
}

/// Per-evaluation context: the pushed delta, known schemas, watermark.
struct EvalCtx<'a> {
    pushed: InputId,
    delta: &'a ZSetBatch,
    schemas: &'a HashMap<InputId, SchemaRef>,
    watermark: i64,
    epoch: u64,
}

#[cfg(test)]
thread_local! {
    pub(super) static FACTORIZED_CUTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static FORCE_DENSE_GROUP_JOIN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(super) static DENSE_JOIN_MATERIALIZATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static DENSE_JOIN_ROWS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static DENSE_JOIN_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static FLAT_ROWS_CUTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
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
    pub(super) fn eval(
        &mut self,
        pushed: InputId,
        delta: &ZSetBatch,
        schemas: &HashMap<InputId, SchemaRef>,
        watermark: i64,
        epoch: u64,
    ) -> Result<Option<ZSetBatch>, crate::error::EngineError> {
        let mut ctx = EvalCtx {
            pushed,
            delta,
            schemas,
            watermark,
            epoch,
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
            reducer: GroupAggregate::new(key, aggs.clone()),
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
