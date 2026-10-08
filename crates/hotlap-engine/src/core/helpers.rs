//! Engine helpers that stay off the [`IncrementalCore`] contract.

use hotlap_core::{CoreError, ViewId};

use super::EngineCore;
use super::graph::ViewGraph;
use super::output::ViewOutput;

impl EngineCore {
    /// Initial output of a freshly built view: replayed from retained inputs
    /// when the engine is already running, empty otherwise.
    pub(in crate::core) fn replay_or_empty(
        &self,
        graph: &mut ViewGraph,
    ) -> Result<ViewOutput, CoreError> {
        if !self.frozen {
            return Ok(ViewOutput::default());
        }
        let sources = graph.sources().to_vec();
        self.retention
            .replay(graph, &self.schemas, &sources)
            .map_err(CoreError::from)
    }

    /// Deltas dropped by `view`'s window operators because their window had
    /// already closed when the delta arrived (append-only output cannot retract
    /// an emitted window).
    pub fn window_late_closed(&self, view: ViewId) -> Result<u64, CoreError> {
        let state = self
            .views
            .get(&view)
            .ok_or_else(|| CoreError::Unsupported(format!("unknown view {view:?}")))?;
        Ok(state.graph.window_late_closed_dropped())
    }
}
