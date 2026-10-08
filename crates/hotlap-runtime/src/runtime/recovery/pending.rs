//! Detection and discard of a checkpoint interrupted mid-commit.

use std::sync::Mutex;

use hotlap_engine::MetricsRegistry;

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::pipeline;
use hotlap_connectors::error::ConnectorError;

/// Newest checkpoint above `floor` with a `commit` marker but no `valid` one.
///
/// The floor is the newest valid id: an interrupted commit is always later, so
/// older markers are ignored.
pub(super) fn pending_commit(
    checkpointer: &Checkpointer,
    floor: Option<u64>,
) -> Result<Option<u64>, ConnectorError> {
    let floor = floor.unwrap_or(0);
    for id in checkpointer.ids_descending()? {
        if id <= floor {
            break;
        }
        let base = format!("checkpoint/{id}");
        if checkpointer.has_key(&format!("{base}/commit"))?
            && !checkpointer.has_key(&format!("{base}/valid"))?
        {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

/// Record the discard of `pending`, delete it, and return the checkpoint to
/// replay from (`None` means start clean).
///
/// The warning names the actual destination: replaying an older checkpoint is
/// not the same as a clean start, and the message must not claim otherwise.
pub(super) fn discard(
    checkpointer: &mut Checkpointer,
    metrics: &MetricsRegistry,
    signal: &Mutex<Option<String>>,
    pending: u64,
    fallback: Option<Checkpoint>,
    reason: &str,
) -> Result<Option<Checkpoint>, ConnectorError> {
    metrics.inc("checkpoints_discarded");
    let destination = match &fallback {
        Some(checkpoint) => format!("replaying from checkpoint {}", checkpoint.id),
        None => "starting clean".to_string(),
    };
    pipeline::record_error(
        signal,
        ConnectorError::Infrastructure(format!(
            "discarded interrupted checkpoint {pending}: {reason}, {destination}"
        )),
    );
    checkpointer.discard_commit(pending)?;
    Ok(fallback)
}
