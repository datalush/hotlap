//! [`IncrementalCore`] implementation for [`EngineCore`].

use std::sync::Arc;

use arrow::datatypes::Schema;

use hotlap_core::plan::sources;
use hotlap_core::{CoreError, IncrementalCore, InputId, Plan, ViewId, WatermarkSpec, ZSetBatch};

use super::{EngineCore, ViewState};
use crate::core_eval::{delta, merge};
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
        if self.frozen {
            return Err(CoreError::Unsupported("engine already running".into()));
        }
        if self.views.contains_key(&view) {
            return Err(CoreError::Unsupported(format!("view {view:?} already built")));
        }
        for src in sources(plan) {
            if !self.registered.contains(&src) {
                return Err(CoreError::Unsupported(format!("unknown input {src:?}")));
            }
        }
        self.views.insert(
            view,
            ViewState {
                plan: plan.clone(),
                tapped: false,
                current: None,
                drained: None,
            },
        );
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
        let kept = self.filter_late(input, batch).map_err(CoreError::from)?;
        let merged = match self.inputs.get(&input) {
            Some(previous) => merge(previous, &kept),
            None => Ok(kept),
        }
        .map_err(CoreError::from)?;
        let accumulated = consolidate(&merged).map_err(CoreError::from)?;
        self.inputs.insert(input, accumulated);
        self.refresh_views()
    }

    fn snapshot(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError> {
        let state = self
            .views
            .get(&view)
            .ok_or_else(|| CoreError::Unsupported(format!("unknown view {view:?}")))?;
        match &state.current {
            Some(current) => Ok(current.clone()),
            None => Ok(ZSetBatch::empty(Arc::new(Schema::empty()))),
        }
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
        let changes =
            delta(state.current.as_ref(), state.drained.as_ref()).map_err(CoreError::from)?;
        state.drained = state.current.clone();
        match changes {
            Some(changes) => Ok(changes),
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
}
