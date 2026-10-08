//! Public [`Hotlap`] facade over the stateful [`IncrementalCore`].
//!
//! `Hotlap` owns a boxed [`IncrementalCore`] plus registries mapping caller names
//! to core handles: input names -> [`InputId`], view names -> [`ViewId`]. Ids are
//! assigned monotonically and only advance once the core accepts the declaration.
//!
//! The concrete core is injected with [`Hotlap::open_with`]; this crate only
//! depends on the `hotlap-core` contract, never on a specific kernel.

use std::collections::HashMap;

use hotlap_core::{IncrementalCore, InputId, ViewId};

mod api;

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
