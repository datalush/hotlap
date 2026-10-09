//! Public [`Hotlap`] facade over the stateful [`IncrementalCore`].
//!
//! `Hotlap` owns a boxed [`IncrementalCore`] plus registries mapping caller names
//! to core handles: input names -> [`InputId`], view names -> [`ViewId`]. Ids are
//! assigned monotonically and only advance once the core accepts the declaration.
//!
//! The concrete core is injected with [`Hotlap::open_with`]; this crate only
//! depends on the `hotlap-core` contract, never on a specific kernel.

use std::collections::HashMap;

use hotlap_core::{EngineSnapshot, IncrementalCore, InputId, Plan, ViewId};

mod api;

/// Error returned by the public [`Hotlap`] API.
#[derive(Debug)]
pub struct HotlapError(pub String);

/// A compiled view: its core handle and the plan it was built from.
struct ViewSlot {
    id: ViewId,
    plan: Plan,
}

/// Stateful engine facade: one live core, many named inputs and views.
pub struct Hotlap {
    core: Box<dyn IncrementalCore>,
    next_input: u32,
    next_view: u32,
    inputs: HashMap<String, InputId>,
    views: HashMap<String, ViewSlot>,
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
            .map(|slot| slot.id)
            .ok_or_else(|| HotlapError("no such view".into()))
    }

    /// The named view registry: `(name, handle, plan)` ordered by handle.
    ///
    /// The caller-facing names are not stored in an [`EngineSnapshot`], so this
    /// is what a checkpoint persists to associate a name with the numeric handle
    /// and plan it was compiled from.
    pub fn view_registry(&self) -> Vec<(String, ViewId, Plan)> {
        let mut registry: Vec<(String, ViewId, Plan)> = self
            .views
            .iter()
            .map(|(name, slot)| (name.clone(), slot.id, slot.plan.clone()))
            .collect();
        registry.sort_by_key(|(_, id, _)| *id);
        registry
    }

    /// Reject a snapshot that cannot back every registered view.
    ///
    /// The snapshot stores only numeric handles, so without this a renamed or
    /// reordered declaration could be silently rebound to another view's state.
    pub(super) fn check_view_identity(&self, snapshot: &EngineSnapshot) -> Result<(), HotlapError> {
        for (name, slot) in &self.views {
            let saved = snapshot
                .views
                .iter()
                .find(|view| view.id == slot.id)
                .ok_or_else(|| {
                    HotlapError(format!(
                        "view `{name}` has no snapshot state for handle {}",
                        slot.id.0
                    ))
                })?;
            if saved.plan != slot.plan {
                return Err(HotlapError(format!(
                    "view `{name}` does not match the plan saved for handle {}",
                    slot.id.0
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
