//! Cross-checks a checkpoint's named view registry against the declaration and
//! the engine snapshot.

use std::collections::{BTreeMap, BTreeSet};

use hotlap::{Plan, ViewId};
use hotlap_connectors::error::ConnectorError;
use hotlap_engine::EngineSnapshot;

use super::unsupported;
use crate::runtime::source_checkpoint::{SavedView, SourcesCheckpoint};

/// Validate the saved named views against `declared` and `engine`.
///
/// The registry, the engine snapshot and the declaration must name the exact
/// same views: same count, names, handles and plans. A missing or extra view on
/// either side is rejected instead of silently dropped, so a name is never lost
/// and an undeclared view is never restored. Duplicate snapshot handles are
/// rejected before they could validate one plan and restore another.
pub(crate) fn validate_views(
    checkpoint: &SourcesCheckpoint,
    declared: &[SavedView],
    engine: &EngineSnapshot,
) -> Result<(), ConnectorError> {
    let saved = saved_views_by_name(&checkpoint.views)?;
    let engine_plans = engine_views_by_id(engine)?;
    if engine_plans.len() != checkpoint.views.len() {
        return Err(unsupported(
            "engine snapshot views do not match the checkpoint registry",
        ));
    }
    for view in &checkpoint.views {
        let plan = engine_plans
            .get(&view.id)
            .ok_or_else(|| unsupported(format!("view `{}` has no engine state", view.name)))?;
        if **plan != view.plan {
            return Err(unsupported(format!(
                "view `{}` does not match its saved engine plan",
                view.name
            )));
        }
    }
    if declared.len() != checkpoint.views.len() {
        return Err(unsupported(
            "declared views do not match the checkpoint registry",
        ));
    }
    let mut declared_ids = BTreeSet::new();
    let mut declared_names = BTreeSet::new();
    for view in declared {
        if !declared_ids.insert(view.id) || !declared_names.insert(view.name.as_str()) {
            return Err(unsupported("declared views share a handle or name"));
        }
        let saved_view = saved
            .get(view.name.as_str())
            .ok_or_else(|| unsupported(format!("view `{}` is not in the checkpoint", view.name)))?;
        if saved_view.id != view.id || saved_view.plan != view.plan {
            return Err(unsupported(format!(
                "view `{}` does not match its saved handle and plan",
                view.name
            )));
        }
    }
    Ok(())
}

/// Index the saved views by name, rejecting duplicate names or handles.
fn saved_views_by_name(views: &[SavedView]) -> Result<BTreeMap<&str, &SavedView>, ConnectorError> {
    let mut by_name = BTreeMap::new();
    let mut ids = BTreeSet::new();
    for view in views {
        if !ids.insert(view.id) {
            return Err(unsupported(format!("duplicate view handle {:?}", view.id)));
        }
        if by_name.insert(view.name.as_str(), view).is_some() {
            return Err(unsupported(format!("duplicate view name {}", view.name)));
        }
    }
    Ok(by_name)
}

/// Index the engine snapshot's view plans by handle, rejecting duplicates.
fn engine_views_by_id(engine: &EngineSnapshot) -> Result<BTreeMap<ViewId, &Plan>, ConnectorError> {
    let mut by_id = BTreeMap::new();
    for snapshot in &engine.views {
        if by_id.insert(snapshot.id, &snapshot.plan).is_some() {
            return Err(unsupported(format!(
                "duplicate view handle {:?} in engine snapshot",
                snapshot.id
            )));
        }
    }
    Ok(by_id)
}
