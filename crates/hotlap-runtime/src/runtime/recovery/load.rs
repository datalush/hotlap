//! Select the newest valid checkpoint from the store.

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::sources::Sources;
use hotlap_connectors::error::ConnectorError;

use super::Recovery;

impl Recovery {
    /// Newest valid checkpoint, or `None` for a clean start.
    ///
    /// The `latest` pointer is read only to classify it: a damaged pointer is
    /// tolerated, but an operational read failure is fatal. Selection always
    /// scans the namespace newest-first, so a pointer that lags behind a
    /// checkpoint already published as `valid` cannot hide it. A checkpoint that
    /// is corrupted is skipped only when this checkpointer's participants declare
    /// replay safe. Otherwise corruption is fatal to prevent transaction replay.
    /// An incompatible checkpoint is always fatal `Unsupported`.
    pub fn load(
        checkpointer: &Checkpointer,
        sources: &Sources,
    ) -> Result<Option<Checkpoint>, ConnectorError> {
        // Participant manifests are authoritative even when a newer published
        // body is missing or corrupt and would otherwise be skipped.
        checkpointer.validate_runtime_participants(sources)?;
        match checkpointer.latest() {
            // A damaged `latest` pointer is current-format corruption: scan the
            // store for a valid checkpoint instead of aborting startup. An
            // operational failure propagates and is never treated as absence.
            Ok(_) | Err(ConnectorError::Corruption(_) | ConnectorError::Missing(_)) => {}
            Err(error) => return Err(error),
        }
        let Some(checkpoint) = checkpointer.newest_valid()? else {
            return Ok(None);
        };
        checkpointer.validate_participant_manifest(checkpoint.id, sources)?;
        super::sources::validate(sources, &checkpoint.sources, &checkpoint.engine)?;
        Ok(Some(checkpoint))
    }
}
