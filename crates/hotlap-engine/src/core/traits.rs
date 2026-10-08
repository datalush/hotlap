//! [`IncrementalCore`] implementation for [`EngineCore`].

use std::sync::Arc;

use arrow::datatypes::Schema;

use hotlap_core::{
    CoreError, EngineSnapshot, IncrementalCore, InputId, Plan, SplitId, ViewId, WatermarkSpec,
    ZSetBatch,
};

use super::graph::ViewGraph;
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
        self.ensure_healthy()?;
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
        self.refresh_windows_open();
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

    fn declare_splits(&mut self, input: InputId, splits: &[SplitId]) -> Result<(), CoreError> {
        self.ensure_healthy()?;
        if self.frozen {
            return Err(CoreError::Unsupported("engine already running".into()));
        }
        if !self.registered.contains(&input) {
            return Err(CoreError::Unsupported(format!("unknown input {input:?}")));
        }
        // A declared split joins the map at its initial zero watermark so the
        // input minimum accounts for it before its first batch arrives.
        for &split in splits {
            self.split_watermarks.entry((input, split)).or_insert(0);
        }
        self.refresh_watermark(input);
        Ok(())
    }

    fn push(&mut self, input: InputId, batch: &ZSetBatch) -> Result<(), CoreError> {
        self.push_split(input, 0, batch)
    }

    fn push_split(
        &mut self,
        input: InputId,
        split: SplitId,
        batch: &ZSetBatch,
    ) -> Result<(), CoreError> {
        self.ensure_healthy()?;
        if !self.registered.contains(&input) {
            return Err(CoreError::Unsupported(format!("unknown input {input:?}")));
        }
        if !self.frozen {
            self.freeze()?;
        }
        self.count_ingested(batch.len());
        self.schemas.insert(input, batch.schema());
        let kept = self
            .filter_late(input, split, batch)
            .map_err(CoreError::from)?;
        let watermark = self.watermarks.get(&input).copied().unwrap_or(0);
        self.retention.record(input, &kept, watermark);
        let targets: Vec<ViewId> = self
            .views
            .iter()
            .filter(|(_, view)| view.graph.sources().contains(&input))
            .map(|(id, _)| *id)
            .collect();
        for id in targets {
            // A failure may leave earlier views applied; poison the core
            // instead of serving that partial state.
            if let Err(error) = self.push_view(id, input, &kept) {
                self.fail();
                return Err(error);
            }
        }
        self.refresh_late_closed();
        self.refresh_windows_open();
        self.epoch = self.epoch.wrapping_add(1);
        Ok(())
    }

    fn snapshot(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError> {
        self.ensure_healthy()?;
        let state = self
            .views
            .get(&view)
            .ok_or_else(|| CoreError::Unsupported(format!("unknown view {view:?}")))?;
        // The output map holds the consolidated current state, so a snapshot
        // materializes it once: its cost tracks state, not accumulated history.
        state.output.snapshot().map_err(CoreError::from)
    }

    fn late_dropped(&self, input: InputId) -> Result<u64, CoreError> {
        self.ensure_healthy()?;
        if !self.registered.contains(&input) {
            return Err(CoreError::Unsupported(format!("unknown input {input:?}")));
        }
        Ok(self.late.get(&input).copied().unwrap_or(0))
    }

    fn take_changes(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError> {
        self.ensure_healthy()?;
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
