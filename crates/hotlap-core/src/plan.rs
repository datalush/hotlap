//! Minimal plan IR: the operators the incremental core compiles to.

use serde::{Deserialize, Serialize};

use crate::ids::InputId;
pub use crate::predicate::{CmpOp, Predicate, Scalar};

/// The operators a plan may contain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Plan {
    /// Leaf: the shared source registered under this id.
    Source(InputId),
    Filter {
        input: Box<Plan>,
        pred: Predicate,
    },
    Project {
        input: Box<Plan>,
        cols: Vec<usize>,
    },
    GroupCount {
        input: Box<Plan>,
        key: Vec<usize>,
    },
    /// Inner equi-join of two plans on `left_key`/`right_key`.
    ///
    /// The output row is every column of the joined left row followed by every
    /// column of the joined right row; multiplicities multiply.
    Join {
        left: Box<Plan>,
        right: Box<Plan>,
        left_key: Vec<usize>,
        right_key: Vec<usize>,
    },
    /// Tumbling-window aggregate of `size` over `time_col`, grouped by `key`.
    /// Output: columns of `key`, `window_start`, `count`; emits each window once
    /// when it closes (append-only).
    TumbleCount {
        input: Box<Plan>,
        key: Vec<usize>,
        time_col: usize,
        size: i64,
    },
}

/// Does the plan contain any window?
pub fn has_window(plan: &Plan) -> bool {
    match plan {
        Plan::TumbleCount { .. } => true,
        Plan::Filter { input, .. }
        | Plan::Project { input, .. }
        | Plan::GroupCount { input, .. } => has_window(input),
        Plan::Join { left, right, .. } => has_window(left) || has_window(right),
        Plan::Source(_) => false,
    }
}

/// Collects the input ids a plan reads from, in traversal order.
pub fn sources(plan: &Plan) -> Vec<InputId> {
    let mut out = Vec::new();
    collect_sources(plan, &mut out);
    out
}

fn collect_sources(plan: &Plan, out: &mut Vec<InputId>) {
    match plan {
        Plan::Source(id) => out.push(*id),
        Plan::Filter { input, .. }
        | Plan::Project { input, .. }
        | Plan::GroupCount { input, .. }
        | Plan::TumbleCount { input, .. } => collect_sources(input, out),
        Plan::Join { left, right, .. } => {
            collect_sources(left, out);
            collect_sources(right, out);
        }
    }
}
