//! Persistent per-view operator graph: propagate only deltas through stateful
//! operators, which retain their state across pushes.

mod apply;

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;

use hotlap_core::plan::sources;
use hotlap_core::{InputId, Plan, Predicate, ZSetBatch};

use crate::ops::{Join, TumbleCount};

use apply::GroupState;

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
        key: Vec<usize>,
        state: Option<Box<GroupState>>,
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

/// Per-evaluation context: the pushed delta, known schemas, watermark, work.
struct EvalCtx<'a> {
    pushed: InputId,
    delta: &'a ZSetBatch,
    schemas: &'a HashMap<InputId, SchemaRef>,
    watermark: i64,
    rows: u64,
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

    /// Evaluates the graph over the pushed `delta`, returning the view's output
    /// delta (if any) and the number of rows fed to operators.
    pub(super) fn eval(
        &mut self,
        pushed: InputId,
        delta: &ZSetBatch,
        schemas: &HashMap<InputId, SchemaRef>,
        watermark: i64,
    ) -> Result<(Option<ZSetBatch>, u64), crate::error::EngineError> {
        let mut ctx = EvalCtx {
            pushed,
            delta,
            schemas,
            watermark,
            rows: 0,
        };
        let output = self.root.eval(&mut ctx)?;
        Ok((output, ctx.rows))
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
        Plan::GroupCount { input, key } => Node::Group {
            input: Box::new(compile(input)),
            key: key.clone(),
            state: None,
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
