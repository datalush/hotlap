//! Per-view state: graph, plan and incremental output.

use hotlap_core::plan::has_window;
use hotlap_core::{Plan, ZSetBatch};

use super::graph::ViewGraph;
use super::output::ViewOutput;

/// A view's persistent graph plus its incremental output and pending changes.
pub(crate) struct ViewState {
    pub(super) graph: ViewGraph,
    pub(super) plan: Plan,
    pub(super) windowed: bool,
    pub(super) tapped: bool,
    pub(in crate::core) output: ViewOutput,
    /// Buffered changelog for a tapped view: every propagated output delta is
    /// appended (via `graph::accumulate`) until `take_changes` drains it. An
    /// untapped view leaves it `None`, so it never grows with history.
    ///
    /// [`take_changes`]: crate::IncrementalCore::take_changes
    pub(super) pending: Option<ZSetBatch>,
}

impl ViewState {
    /// Builds a view's state from its graph, plan and initial output.
    pub(in crate::core) fn new(graph: ViewGraph, plan: Plan, output: ViewOutput) -> Self {
        Self {
            windowed: has_window(&plan),
            graph,
            plan,
            tapped: false,
            output,
            pending: None,
        }
    }
}
