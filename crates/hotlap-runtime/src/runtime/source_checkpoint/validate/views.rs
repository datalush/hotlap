//! Cross-checks a checkpoint's named view registry against the declaration and
//! the engine snapshot.

use std::collections::{BTreeMap, BTreeSet};

use hotlap_connectors::error::ConnectorError;
use hotlap_engine::EngineSnapshot;

use super::unsupported;
use crate::runtime::source_checkpoint::{SavedView, SourcesCheckpoint};

/// Validate the saved named views against `declared` and `engine`.
///
/// Every declared view must find the same name in the registry with the same
/// handle and plan, and every saved handle must still carry the same plan in
/// the engine snapshot. Saved views with no declaration (for example a view
/// created after `START` in an earlier run) are restored but not rebound.
pub(crate) fn validate_views(
    checkpoint: &SourcesCheckpoint,
    declared: &[SavedView],
    engine: &EngineSnapshot,
) -> Result<(), ConnectorError> {
    let saved = saved_views_by_name(&checkpoint.views)?;
    for view in &checkpoint.views {
        let snapshot = engine
            .views
            .iter()
            .find(|snapshot| snapshot.id == view.id)
            .ok_or_else(|| unsupported(format!("view `{}` has no engine state", view.name)))?;
        if snapshot.plan != view.plan {
            return Err(unsupported(format!(
                "view `{}` does not match its saved engine plan",
                view.name
            )));
        }
    }
    let mut declared_ids = BTreeSet::new();
    for view in declared {
        if !declared_ids.insert(view.id) {
            return Err(unsupported("declared views share a handle"));
        }
        let saved_view = saved
            .get(view.name.as_str())
            .ok_or_else(|| unsupported(format!("view `{}` is not in the checkpoint", view.name)))?;
        if saved_view.id != view.id {
            return Err(unsupported(format!(
                "view `{}` was declared with a different handle",
                view.name
            )));
        }
        if saved_view.plan != view.plan {
            return Err(unsupported(format!(
                "view `{}` was declared with a different plan",
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
