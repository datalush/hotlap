//! [`IncrementalCore`] implementation for [`EngineCore`].

use std::sync::Arc;

use arrow::datatypes::Schema;

use hotlap_core::{
    CoreError, EngineSnapshot, IncrementalCore, InputId, Plan, ViewId, WatermarkSpec, ZSetBatch,
};

use super::graph::ViewGraph;
use super::output::ViewOutput;
use super::{EngineCore, ViewState};
use crate::zset::consolidate;

impl IncrementalCore for EngineCore {
    fn register_input(&mut self, input: InputId) -> Result<(), CoreError> {
        if self.frozen {
            return Err(CoreError::Unsupported("engine already running".into()));
        }
        if !self.registered.insert(input) {
            return Err(CoreError::Unsupported(format!(
                "input {input:?} already registered"
            )));
        }
        Ok(())
    }

    fn build_view(&mut self, view: ViewId, plan: &Plan) -> Result<(), CoreError> {
        if self.views.contains_key(&view) {
            return Err(CoreError::Unsupported(format!(
                "view {view:?} already built"
            )));
        }
        let mut graph = ViewGraph::build(plan);
        for src in graph.sources() {
            if !self.registered.contains(src) {
                return Err(CoreError::Unsupported(format!("unknown input {src:?}")));
            }
        }
        let output = self.replay_or_empty(&mut graph)?;
        self.views
            .insert(view, ViewState::new(graph, plan.clone(), output));
        Ok(())
    }

    fn set_input_retention(&mut self, events: usize) -> Result<(), CoreError> {
        if self.frozen {
            return Err(CoreError::Unsupported("engine already running".into()));
        }
        if events == 0 {
            return Err(CoreError::Unsupported(
                "input retention must keep at least one delta".into(),
            ));
        }
        self.retention = super::retention::InputRetention::new(events);
        Ok(())
    }

    fn declare_watermark(&mut self, input: InputId, spec: WatermarkSpec) -> Result<(), CoreError> {
        if self.frozen {
            return Err(CoreError::Unsupported("engine already running".into()));
        }
        if !self.registered.contains(&input) {
            return Err(CoreError::Unsupported(format!("unknown input {input:?}")));
        }
        if spec.lag < 0 {
            return Err(CoreError::Unsupported("watermark lag must be >= 0".into()));
        }
        if self.specs.contains_key(&input) {
            return Err(CoreError::Unsupported(format!(
                "watermark already declared for {input:?}"
            )));
        }
        self.specs.insert(input, spec);
        Ok(())
    }

    fn push(&mut self, input: InputId, batch: &ZSetBatch) -> Result<(), CoreError> {
        if !self.registered.contains(&input) {
            return Err(CoreError::Unsupported(format!("unknown input {input:?}")));
        }
        if !self.frozen {
            self.freeze()?;
        }
        self.schemas.insert(input, batch.schema());
        let kept = self.filter_late(input, batch).map_err(CoreError::from)?;
        let watermark = self.watermarks.get(&input).copied().unwrap_or(0);
        self.retention.record(input, &kept, watermark);
        let targets: Vec<ViewId> = self
            .views
            .iter()
            .filter(|(_, view)| view.graph.sources().contains(&input))
            .map(|(id, _)| *id)
            .collect();
        for id in targets {
            self.push_view(id, input, &kept)?;
        }
        self.epoch = self.epoch.wrapping_add(1);
        Ok(())
    }

    fn snapshot(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError> {
        let state = self
            .views
            .get(&view)
            .ok_or_else(|| CoreError::Unsupported(format!("unknown view {view:?}")))?;
        // The output map holds the consolidated current state, so a snapshot
        // materializes it once: its cost tracks state, not accumulated history.
        state.output.snapshot().map_err(CoreError::from)
    }

    fn late_dropped(&self, input: InputId) -> Result<u64, CoreError> {
        Ok(self.late.get(&input).copied().unwrap_or(0))
    }

    fn take_changes(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError> {
        let state = self
            .views
            .get_mut(&view)
            .ok_or_else(|| CoreError::Unsupported(format!("unknown view {view:?}")))?;
        if !state.tapped {
            return Ok(ZSetBatch::empty(Arc::new(Schema::empty())));
        }
        match state.pending.take() {
            Some(pending) => consolidate(&pending).map_err(CoreError::from),
            None => Ok(ZSetBatch::empty(Arc::new(Schema::empty()))),
        }
    }

    fn tap_view(&mut self, view: ViewId) -> Result<(), CoreError> {
        if self.frozen {
            return Err(CoreError::Unsupported("engine already running".into()));
        }
        match self.views.get_mut(&view) {
            Some(state) => {
                state.tapped = true;
                Ok(())
            }
            None => Err(CoreError::Unsupported(format!("unknown view {view:?}"))),
        }
    }

    fn checkpoint(&self) -> Result<EngineSnapshot, CoreError> {
        // Method-call syntax picks the inherent `EngineCore::checkpoint`.
        self.checkpoint().map_err(CoreError::from)
    }

    fn restore(&mut self, snapshot: &EngineSnapshot) -> Result<(), CoreError> {
        // Method-call syntax picks the inherent `EngineCore::restore`.
        self.restore(snapshot).map_err(CoreError::from)
    }
}

/// Engine-specific helpers that stay off the [`IncrementalCore`] contract.
impl EngineCore {
    /// Initial output of a freshly built view: replayed from retained inputs
    /// when the engine is already running, empty otherwise.
    fn replay_or_empty(&self, graph: &mut ViewGraph) -> Result<ViewOutput, CoreError> {
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
