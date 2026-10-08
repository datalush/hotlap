//! Public [`Hotlap`] facade over the stateful [`IncrementalCore`].
//!
//! `Hotlap` owns a boxed [`IncrementalCore`] plus registries mapping caller names
//! to core handles: input names -> [`InputId`], view names -> [`ViewId`]. Ids are
//! assigned monotonically and only advance once the core accepts the declaration.
//!
//! The concrete core is injected with [`Hotlap::open_with`]; this crate only
//! depends on the `hotlap-core` contract, never on a specific kernel.

use std::collections::HashMap;

use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, WatermarkSpec, ZSetBatch};

/// Error returned by the public [`Hotlap`] API.
#[derive(Debug)]
pub struct HotlapError(pub String);

/// Stateful engine facade: one live core, many named inputs and views.
pub struct Hotlap {
    core: Box<dyn IncrementalCore>,
    next_input: u32,
    next_view: u32,
    inputs: HashMap<String, InputId>,
    views: HashMap<String, ViewId>,
}

impl Hotlap {
    /// Open an engine over the injected `core`.
    pub fn open_with(core: Box<dyn IncrementalCore>) -> Self {
        Self {
            core,
            next_input: 0,
            next_view: 0,
            inputs: Default::default(),
            views: Default::default(),
        }
    }

    /// Declare a source under `name`; plans reference it as `Plan::Source(id)`.
    pub fn register_input(&mut self, name: &str) -> Result<(), HotlapError> {
        if self.inputs.contains_key(name) {
            return Err(HotlapError(format!("input already exists: {name}")));
        }
        let id = InputId(self.next_input);
        self.core
            .register_input(id)
            .map_err(|e| HotlapError(format!("{e}")))?;
        self.next_input += 1;
        self.inputs.insert(name.to_string(), id);
        Ok(())
    }

    /// Declare `input`'s watermark (event-time column and lag `lag`).
    pub fn declare_watermark(
        &mut self,
        input: &str,
        time_col: usize,
        lag: i64,
    ) -> Result<(), HotlapError> {
        let id = *self
            .inputs
            .get(input)
            .ok_or_else(|| HotlapError("no such input".into()))?;
        self.core
            .declare_watermark(id, WatermarkSpec { time_col, lag })
            .map_err(|e| HotlapError(format!("{e}")))
    }

    /// Compile `plan` into a new view registered under `name`.
    pub fn create_view(&mut self, name: &str, plan: Plan) -> Result<(), HotlapError> {
        // Reject duplicate names up front: overwriting the mapping would orphan
        // the previously-built view (still alive in the core, unreachable here).
        if self.views.contains_key(name) {
            return Err(HotlapError(format!("view already exists: {name}")));
        }
        let id = ViewId(self.next_view);
        // Only claim the id once the core accepts the plan; a failed build must
        // not burn an id or shadow an existing name with a dangling mapping.
        self.core
            .build_view(id, &plan)
            .map_err(|e| HotlapError(format!("{e}")))?;
        self.next_view += 1;
        self.views.insert(name.to_string(), id);
        Ok(())
    }

    /// Subscribe the view registered as `name` to its output changelog. Only
    /// before the first push; read the buffered deltas with [`take_changes`].
    pub fn tap_view(&mut self, name: &str) -> Result<(), HotlapError> {
        let id = self.view_id(name)?;
        self.core
            .tap_view(id)
            .map_err(|e| HotlapError(format!("{e}")))
    }

    /// Drain the `ZSetBatch` changes of the tapped view registered as `name`,
    /// accumulated by the last push. Empty for an untapped view.
    pub fn take_changes(&mut self, name: &str) -> Result<ZSetBatch, HotlapError> {
        let id = self.view_id(name)?;
        self.core
            .take_changes(id)
            .map_err(|e| HotlapError(format!("{e}")))
    }

    /// Feed `batch` into the input registered as `name`.
    pub fn push(&mut self, input: &str, batch: &ZSetBatch) -> Result<(), HotlapError> {
        let id = *self
            .inputs
            .get(input)
            .ok_or_else(|| HotlapError("no such input".into()))?;
        self.core
            .push(id, batch)
            .map_err(|e| HotlapError(format!("{e}")))
    }

    /// Read the consolidated output of the view registered as `name`.
    pub fn snapshot(&mut self, name: &str) -> Result<ZSetBatch, HotlapError> {
        let id = self.view_id(name)?;
        self.core
            .snapshot(id)
            .map_err(|e| HotlapError(format!("{e}")))
    }

    /// Events dropped as late in `input` (event-time mode).
    pub fn late_dropped(&self, input: &str) -> Result<u64, HotlapError> {
        let id = *self
            .inputs
            .get(input)
            .ok_or_else(|| HotlapError("no such input".into()))?;
        self.core
            .late_dropped(id)
            .map_err(|e| HotlapError(format!("{e}")))
    }

    /// Shut the engine down. Dropping `Hotlap` also releases the core.
    pub fn shutdown(self) -> Result<(), HotlapError> {
        Ok(())
    }

    /// Resolve a registered view name to its core handle.
    fn view_id(&self, name: &str) -> Result<ViewId, HotlapError> {
        self.views
            .get(name)
            .copied()
            .ok_or_else(|| HotlapError("no such view".into()))
    }
}

#[cfg(test)]
mod tests;
