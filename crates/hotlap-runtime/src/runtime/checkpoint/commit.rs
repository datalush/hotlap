//! Commit protocol: two-phase sink commit plus durable marker handling.

use hotlap::Hotlap;

use crate::runtime::checkpoint::Checkpointer;
use crate::runtime::checkpoint_body::{
    checkpoint_prefix, clear_commit, id_exhausted, mark_valid, reserve, state_err, write,
};
use crate::runtime::sources::Sources;
use hotlap_connectors::error::ConnectorError;

impl Checkpointer {
    /// Capture `engine` and `source` and persist a new valid checkpoint,
    /// coordinating the sinks in two-phase-commit order.
    ///
    /// The order is compute+advance id -> reserve id -> drain -> prepare ->
    /// snapshot + write body + durable commit marker -> commit -> mark valid ->
    /// clear marker. A failure aborts the prepared sinks and clears the marker,
    /// so the engine keeps its last valid checkpoint; a storage failure while
    /// writing the body preserves the marker and body untouched. A crash between
    /// the marker and validity leaves the marker for recovery to resolve.
    ///
    /// The marker is cleared *before* the abort on a commit failure: otherwise a
    /// crash between the abort and the clear would leave a complete body with a
    /// marker over rolled-back sinks, which recovery might promote.
    ///
    /// Returns the id of the checkpoint; later reads must use [`Self::read`].
    pub async fn take(
        &mut self,
        engine: &Hotlap,
        sources: &Sources,
    ) -> Result<u64, ConnectorError> {
        // Resolve the starting id and advance the in-memory sequence before the
        // durable reservation: an ambiguous reservation that persists then
        // reports failure must not let this checkpointer retry the same id.
        self.ensure_id_floor()?;
        let id = self.next_id;
        let next = id.checked_add(1).ok_or_else(id_exhausted)?;
        self.next_id = next;
        reserve(self.backend.as_mut(), id)?;

        self.sinks.drain().await?;
        let prepared = self.sinks.prepare().await?;
        if let Err(error) = write(self.backend.as_mut(), id, engine, sources).await {
            // A storage failure may already have persisted part of the body or
            // the commit marker; preserve them as evidence for an uncertain
            // commit instead of clearing the marker or aborting the sinks.
            if !matches!(error, ConnectorError::Storage(_)) {
                let _ = clear_commit(self.backend.as_mut(), id);
                self.sinks.abort(&prepared).await;
            }
            return Err(error);
        }
        if let Err((error, remaining)) = self.sinks.commit(&prepared).await {
            let _ = clear_commit(self.backend.as_mut(), id);
            self.sinks.abort(&remaining).await;
            return Err(error);
        }
        mark_valid(self.backend.as_mut(), id, self.retain)?;
        let _ = clear_commit(self.backend.as_mut(), id);
        Ok(id)
    }

    /// Finish an interrupted commit detected by recovery: re-drive the sinks'
    /// `commit` (idempotent, only valid when every sink is re-drivable), publish
    /// `valid` and clear the marker.
    pub async fn promote(&mut self, id: u64) -> Result<(), ConnectorError> {
        self.sinks.redrive_commit().await?;
        mark_valid(self.backend.as_mut(), id, self.retain)?;
        let _ = clear_commit(self.backend.as_mut(), id);
        Ok(())
    }

    /// Delete an interrupted checkpoint that recovery cannot promote.
    pub fn discard_commit(&mut self, id: u64) -> Result<(), ConnectorError> {
        let prefix = format!("{}/", checkpoint_prefix(id));
        for key in self.backend.list(prefix.as_bytes()).map_err(state_err)? {
            self.backend.delete(&key).map_err(state_err)?;
        }
        Ok(())
    }

    /// Drop stale commit markers left by a crash after [`Self::promote`] or
    /// [`Self::take`] published `valid` but before the marker was cleared.
    ///
    /// A checkpoint with both `valid` and `commit` is already published, so the
    /// marker is redundant. A storage failure while deleting it propagates
    /// rather than being ignored.
    pub fn sweep_stale_commits(&mut self) -> Result<(), ConnectorError> {
        for id in self.ids_descending()? {
            let base = checkpoint_prefix(id);
            if self.has_key(&format!("{base}/valid"))? && self.has_key(&format!("{base}/commit"))? {
                clear_commit(self.backend.as_mut(), id).map_err(state_err)?;
            }
        }
        Ok(())
    }
}
