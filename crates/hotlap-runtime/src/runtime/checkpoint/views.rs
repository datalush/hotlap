//! Selection and validation of checkpoint views.

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::source_checkpoint::SavedView;
use hotlap_connectors::error::ConnectorError;

impl Checkpointer {
    /// Every candidate checkpoint's named views must match `declared`.
    pub fn validate_views(&self, declared: &[SavedView]) -> Result<(), ConnectorError> {
        self.validate_views_with_replay_safety(declared, self.replay_safe())
    }

    /// Validate names using the recovery-safety declaration available before
    /// session sink factories are opened.
    pub(crate) fn validate_views_with_replay_safety(
        &self,
        declared: &[SavedView],
        replay_safe: bool,
    ) -> Result<(), ConnectorError> {
        let valid = self.newest_valid()?;
        if let Some(checkpoint) = &valid {
            checkpoint
                .sources
                .validate_views(declared, &checkpoint.engine)?;
        }
        let floor = valid.as_ref().map(|checkpoint| checkpoint.id);
        if let Some(id) = self.pending_commit(floor)? {
            let Some(checkpoint) = self.read_body_for_recovery(id)? else {
                if !replay_safe {
                    return Err(ConnectorError::Unsupported(
                        "a transactional sink cannot safely recover a corrupt pending checkpoint"
                            .into(),
                    ));
                }
                return Ok(());
            };
            checkpoint
                .sources
                .validate_views(declared, &checkpoint.engine)?;
        }
        Ok(())
    }

    /// Newest checkpoint with a valid marker that decodes, or `None`.
    pub fn newest_valid(&self) -> Result<Option<Checkpoint>, ConnectorError> {
        for id in self.ids_descending()? {
            match self.read(id) {
                Ok(checkpoint) => return Ok(Some(checkpoint)),
                Err(error @ ConnectorError::Unsupported(_)) => return Err(error),
                Err(ConnectorError::Corruption(_) | ConnectorError::Missing(_)) => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }
}
