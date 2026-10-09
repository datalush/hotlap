//! Select the newest valid checkpoint from the store.

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::sources::Sources;
use hotlap_connectors::error::ConnectorError;

use super::Recovery;
use super::sources::read_valid;

impl Recovery {
    /// Newest valid checkpoint, or `None` for a clean start.
    ///
    /// The `latest` pointer is read only to classify it: a damaged pointer is
    /// tolerated, but an operational read failure is fatal. Selection always
    /// scans the namespace newest-first, so a pointer that lags behind a
    /// checkpoint already published as `valid` cannot hide it. A checkpoint that
    /// fails to decode as the current format is skipped; an incompatible one is a
    /// fatal `Unsupported` rather than a silent skip.
    pub fn load(
        checkpointer: &Checkpointer,
        sources: &Sources,
    ) -> Result<Option<Checkpoint>, ConnectorError> {
        match checkpointer.latest() {
            // A damaged `latest` pointer is current-format corruption: scan the
            // store for a valid checkpoint instead of aborting startup. An
            // operational failure propagates and is never treated as absence.
            Ok(_) | Err(ConnectorError::Corruption(_) | ConnectorError::Missing(_)) => {}
            Err(error) => return Err(error),
        }
        for id in checkpointer.ids_descending()? {
            if let Some(checkpoint) = read_valid(checkpointer, id)? {
                super::sources::validate(sources, &checkpoint.sources, &checkpoint.engine)?;
                return Ok(Some(checkpoint));
            }
        }
        Ok(None)
    }
}
