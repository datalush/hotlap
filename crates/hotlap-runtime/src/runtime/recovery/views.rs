//! Reject a recovery decision whose checkpointed views disagree with the live
//! declaration, before the engine is restored or a commit is re-driven.

use crate::runtime::checkpoint::Checkpoint;
use hotlap::{CoreError, Hotlap};
use hotlap_connectors::error::ConnectorError;

use super::RecoveryDecision;
use crate::runtime::source_checkpoint::SavedView;

/// Compare the decision's checkpoint views with `hotlap`'s declaration.
pub(super) fn validate(hotlap: &Hotlap, decision: &RecoveryDecision) -> Result<(), ConnectorError> {
    let declared = SavedView::from_registry(&hotlap.view_registry());
    let checkpoint = match decision {
        RecoveryDecision::Resume(checkpoint) | RecoveryDecision::Promote(checkpoint) => {
            Some(checkpoint)
        }
        RecoveryDecision::Discard { fallback, .. } => fallback.as_ref(),
        RecoveryDecision::Clean | RecoveryDecision::Reject { .. } => None,
    };
    if let Some(checkpoint) = checkpoint {
        validate_checkpoint(&declared, checkpoint)?;
        validate_engine_for_hotlap(hotlap, checkpoint)?;
        validate_tap_continuity(hotlap, checkpoint)?;
    }
    Ok(())
}

/// Restore may not overwrite a live sink tap with an untapped saved flag.
pub(super) fn validate_tap_continuity(
    hotlap: &Hotlap,
    checkpoint: &Checkpoint,
) -> Result<(), ConnectorError> {
    let live = hotlap
        .checkpoint()
        .map_err(|error| ConnectorError::Infrastructure(error.0))?;
    for view in live.views.iter().filter(|view| view.tapped) {
        let saved = checkpoint
            .engine
            .views
            .iter()
            .find(|saved| saved.id == view.id);
        if !saved.is_some_and(|saved| saved.tapped && saved.plan == view.plan) {
            return Err(ConnectorError::Unsupported(format!(
                "checkpoint would remove the active tap from view {:?}",
                view.id
            )));
        }
    }
    Ok(())
}

fn validate_engine_for_hotlap(
    hotlap: &Hotlap,
    checkpoint: &Checkpoint,
) -> Result<(), ConnectorError> {
    hotlap
        .validate_snapshot(&checkpoint.engine)
        .map_err(|error| match error {
            CoreError::Unsupported(message) => ConnectorError::Unsupported(message),
            CoreError::Infrastructure(message) => ConnectorError::Corruption(message),
        })
}

pub(super) fn validate_checkpoint(
    declared: &[SavedView],
    checkpoint: &Checkpoint,
) -> Result<(), ConnectorError> {
    checkpoint
        .sources
        .validate_views(declared, &checkpoint.engine)
}
