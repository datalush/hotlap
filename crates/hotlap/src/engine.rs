//! Public [`Hotlap`] facade over the stateful [`IncrementalCore`].
//!
//! `Hotlap` owns a boxed [`IncrementalCore`] plus registries mapping caller names
//! to core handles: input names -> [`InputId`], view names -> [`ViewId`]. Ids are
//! assigned monotonically and only advance once the core accepts the declaration.

use std::collections::HashMap;

use crate::core::differential_dataflow::DifferentialCore;
use crate::core::{IncrementalCore, InputId, ViewId};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row};

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
    /// Open an engine and start its backing worker.
    pub fn open() -> Result<Self, HotlapError> {
        Ok(Self {
            core: Box::new(DifferentialCore::new().map_err(|e| HotlapError(format!("{e:?}")))?),
            next_input: 0,
            next_view: 0,
            inputs: Default::default(),
            views: Default::default(),
        })
    }

    /// Declare a source under `name`; plans reference it as `Plan::Source(id)`.
    pub fn register_input(&mut self, name: &str) -> Result<(), HotlapError> {
        if self.inputs.contains_key(name) {
            return Err(HotlapError(format!("input already exists: {name}")));
        }
        let id = InputId(self.next_input);
        self.core
            .register_input(id)
            .map_err(|e| HotlapError(format!("{e:?}")))?;
        self.next_input += 1;
        self.inputs.insert(name.to_string(), id);
        Ok(())
    }

    /// Declara el watermark de `input` (columna de event-time y retardo `lag`).
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
            .declare_watermark(id, crate::core::WatermarkSpec { time_col, lag })
            .map_err(|e| HotlapError(format!("{e:?}")))
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
            .map_err(|e| HotlapError(format!("{e:?}")))?;
        self.next_view += 1;
        self.views.insert(name.to_string(), id);
        Ok(())
    }

    /// Feed `batch` into the input registered as `name`.
    pub fn push(&mut self, input: &str, batch: &ChangeBatch) -> Result<(), HotlapError> {
        let id = *self
            .inputs
            .get(input)
            .ok_or_else(|| HotlapError("no such input".into()))?;
        self.core
            .push(id, batch)
            .map_err(|e| HotlapError(format!("{e:?}")))
    }

    /// Read the consolidated output of the view registered as `name`.
    pub fn snapshot(&mut self, name: &str) -> Result<Vec<Row>, HotlapError> {
        let id = *self
            .views
            .get(name)
            .ok_or_else(|| HotlapError("no such view".into()))?;
        self.core
            .snapshot(id)
            .map_err(|e| HotlapError(format!("{e:?}")))
    }

    /// Eventos descartados por tardíos en `input` (modo event-time).
    pub fn late_dropped(&self, input: &str) -> Result<u64, HotlapError> {
        let id = *self
            .inputs
            .get(input)
            .ok_or_else(|| HotlapError("no such input".into()))?;
        self.core
            .late_dropped(id)
            .map_err(|e| HotlapError(format!("{e:?}")))
    }

    /// Shut the engine down. Dropping `Hotlap` also stops the worker.
    pub fn shutdown(self) -> Result<(), HotlapError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
