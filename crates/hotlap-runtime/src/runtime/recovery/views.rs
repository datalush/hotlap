//! Reject a recovery decision whose checkpointed views disagree with the live
//! declaration, before the engine is restored or a commit is re-driven.

use hotlap::Hotlap;
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
        checkpoint
            .sources
            .validate_views(&declared, &checkpoint.engine)?;
    }
    Ok(())
}
