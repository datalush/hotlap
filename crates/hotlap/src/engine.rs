//! Public [`Hotlap`] facade over the stateful [`IncrementalCore`].
//!
//! `Hotlap` owns a boxed [`IncrementalCore`] plus registries mapping caller names
//! to core handles: input names -> [`InputId`], view names -> [`ViewId`]. Ids are
//! assigned monotonically and only advance once the core accepts the declaration.
//!
//! The concrete core is injected with [`Hotlap::open_with`]; this crate only
//! depends on the `hotlap-core` contract, never on a specific kernel.

use std::collections::HashMap;

use hotlap_core::{
    EngineSnapshot, IncrementalCore, InputId, Plan, SplitId, ViewId, WatermarkSpec, ZSetBatch,
};

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

    /// Declare the splits (read units) `input` may produce, before the first
    /// push. Keeps the input watermark from advancing past a split that has not
    /// produced its first batch yet; see [`IncrementalCore::declare_splits`].
    pub fn declare_splits(&mut self, input: &str, splits: &[SplitId]) -> Result<(), HotlapError> {
        let id = *self
            .inputs
            .get(input)
            .ok_or_else(|| HotlapError("no such input".into()))?;
        self.core
            .declare_splits(id, splits)
            .map_err(|e| HotlapError(format!("{e}")))
    }

    /// Retain the last `events` input deltas so a view can be created after the
    /// first push. Only before the first push; off by default.
    pub fn set_input_retention(&mut self, events: usize) -> Result<(), HotlapError> {
        self.core
            .set_input_retention(events)
            .map_err(|error| HotlapError(format!("{error}")))
    }

    /// Compile `plan` into a new view registered under `name`.
    ///
    /// After the first push this succeeds only when input retention covers the
    /// whole run; see [`IncrementalCore::build_view`].
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
    ///
    /// Equivalent to [`push_split`](Self::push_split) with the implicit split 0.
    pub fn push(&mut self, input: &str, batch: &ZSetBatch) -> Result<(), HotlapError> {
        self.push_split(input, 0, batch)
    }

    /// Feed `batch`, read from `split`, into the input registered as `name`.
    ///
    /// Keeping the split lets the engine track one watermark per split and use
    /// their minimum, so a fast split cannot mark a slower one's records late.
    pub fn push_split(
        &mut self,
        input: &str,
        split: SplitId,
        batch: &ZSetBatch,
    ) -> Result<(), HotlapError> {
        let id = *self
            .inputs
            .get(input)
            .ok_or_else(|| HotlapError("no such input".into()))?;
        self.core
            .push_split(id, split, batch)
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

    /// Capture the engine's durable state as a versioned snapshot.
    pub fn checkpoint(&self) -> Result<EngineSnapshot, HotlapError> {
        self.core
            .checkpoint()
            .map_err(|error| HotlapError(format!("{error}")))
    }

    /// Rebuild the engine from a snapshot captured by [`Self::checkpoint`].
    ///
    /// Only the core state is restored: the snapshot stores numeric ids, not the
    /// caller-facing names, so the `inputs`/`views` maps cannot be rebuilt. The
    /// caller must register the same names through [`Self::register_input`] and
    /// [`Self::create_view`] before restoring (the runtime does this during
    /// setup). The id counters are advanced past the restored ids so later
    /// declarations cannot collide.
    pub fn restore(&mut self, snapshot: &EngineSnapshot) -> Result<(), HotlapError> {
        self.core
            .restore(snapshot)
            .map_err(|error| HotlapError(format!("{error}")))?;
        for input in &snapshot.inputs {
            self.next_input = self.next_input.max(input.id.0 + 1);
        }
        for view in &snapshot.views {
            self.next_view = self.next_view.max(view.id.0 + 1);
        }
        Ok(())
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
