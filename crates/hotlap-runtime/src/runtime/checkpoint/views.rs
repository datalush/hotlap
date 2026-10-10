//! Selection and validation of checkpoint views.

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::source_checkpoint::SavedView;
use crate::runtime::sources::Sources;
use hotlap_connectors::error::ConnectorError;

impl Checkpointer {
    /// Validate the newest durable source and view identities before factories
    /// with external effects are opened.
    pub(crate) fn validate_sources_and_views(
        &self,
        sources: &Sources,
        declared: &[SavedView],
        replay_safe: bool,
        redriable: bool,
    ) -> Result<(), ConnectorError> {
        if let Some(checkpoint) = self.selected_checkpoint(replay_safe, redriable)? {
            checkpoint.sources.validate(sources, &checkpoint.engine)?;
            checkpoint
                .sources
                .validate_views(declared, &checkpoint.engine)?;
        }
        Ok(())
    }

    /// Every candidate checkpoint's named views must match `declared`.
    pub fn validate_views(&self, declared: &[SavedView]) -> Result<(), ConnectorError> {
        self.validate_views_with_capabilities(declared, self.replay_safe(), self.redriable())
    }

    /// Validate names using the recovery-safety declaration available before
    /// session sink factories are opened.
    pub(crate) fn validate_views_with_capabilities(
        &self,
        declared: &[SavedView],
        replay_safe: bool,
        redriable: bool,
    ) -> Result<(), ConnectorError> {
        if let Some(checkpoint) = self.selected_checkpoint(replay_safe, redriable)? {
            checkpoint
                .sources
                .validate_views(declared, &checkpoint.engine)?;
        }
        Ok(())
    }

    fn selected_checkpoint(
        &self,
        replay_safe: bool,
        redriable: bool,
    ) -> Result<Option<Checkpoint>, ConnectorError> {
        let valid = self.newest_valid()?;
        let floor = valid.as_ref().map(|checkpoint| checkpoint.id);
        let Some(id) = self.pending_commit(floor)? else {
            return Ok(valid);
        };
        if self.prepare_pending(id)? {
            if !replay_safe {
                return Err(ConnectorError::Unsupported(
                    "an interrupted prepare cannot be recovered with transactional sinks".into(),
                ));
            }
            return Ok(valid);
        }
        let pending = self.read_body_for_recovery(id)?;
        if redriable && pending.is_some() {
            return Ok(pending);
        }
        if !replay_safe {
            return Err(ConnectorError::Unsupported(
                "a transactional sink cannot safely recover an interrupted checkpoint".into(),
            ));
        }
        Ok(valid)
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
