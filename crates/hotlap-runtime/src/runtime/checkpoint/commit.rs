//! Commit protocol: two-phase sink commit plus durable marker handling.

use hotlap::Hotlap;

use crate::runtime::checkpoint::{CheckpointState, Checkpointer};
use crate::runtime::checkpoint_body::{
    checkpoint_prefix, clear_commit, id_exhausted, publish_valid, reserve, state_err, write,
};
use crate::runtime::retention::prune;
use crate::runtime::sink_barrier::Prepared;
use crate::runtime::sources::Sources;
use hotlap_connectors::error::ConnectorError;

impl Checkpointer {
    /// Capture `engine` and `sources` and persist a new valid checkpoint,
    /// coordinating the sinks in two-phase-commit order.
    ///
    /// The order is advance id -> reserve id -> drain -> prepare -> snapshot +
    /// write body + durable commit marker -> commit -> publish valid -> prune ->
    /// clear marker. The engine and source offsets are never rolled back with
    /// the sinks, so a failure after the sinks were prepared marks the state
    /// inconsistent and blocks later attempts until a restart resolves it;
    /// reservation and drain failures, which discard no write, stay retryable.
    ///
    /// Returns the id of the checkpoint; later reads must use [`Self::read`].
    pub async fn take(
        &mut self,
        engine: &Hotlap,
        sources: &Sources,
    ) -> Result<u64, ConnectorError> {
        self.block_if_failed()?;
        // Resolve the starting id and advance the in-memory sequence before the
        // durable reservation: an ambiguous reservation that persists then
        // reports failure must not let this checkpointer retry the same id.
        self.ensure_id_floor()?;
        let id = self.next_id;
        let next = id.checked_add(1).ok_or_else(id_exhausted)?;
        self.next_id = next;
        reserve(self.backend.as_mut(), id)?;

        if let Err(error) = self.sinks.drain(&self.cancel).await {
            // An abandoned drain has prepared nothing, but the attempt was
            // interrupted: block continuation until a restart resolves it.
            if self.cancel.tripped() {
                self.fail(CheckpointState::Failed, &error);
            }
            return Err(error);
        }
        let prepared = match self.sinks.prepare(&self.cancel).await {
            Ok(prepared) => prepared,
            Err(error) => {
                self.fail(CheckpointState::Failed, &error);
                return Err(error);
            }
        };

        if let Err(error) = write(self.backend.as_mut(), id, engine, sources).await {
            return self.capture_failed(id, prepared, error).await;
        }

        if let Err(error) = self.sinks.commit(&prepared, &self.cancel).await {
            // A participant may already have confirmed its commit, so no sink
            // may be rolled back. The marker and body written above stay for
            // recovery to re-drive or discard and replay.
            self.fail(CheckpointState::CommitUncertain, &error);
            return Err(error);
        }

        if let Err(error) = publish_valid(self.backend.as_mut(), id) {
            // `valid` may or may not be durable, so the marker still identifies
            // the attempt; recovery resolves it.
            self.fail(CheckpointState::CommitUncertain, &error);
            return Err(error);
        }
        // The checkpoint is published; pruning and clearing the redundant
        // marker are cleanup, so a failure here does not make a later attempt
        // unsafe and the published checkpoint stays readable.
        prune(self.backend.as_mut(), self.retain).map_err(state_err)?;
        clear_commit(self.backend.as_mut(), id).map_err(state_err)?;
        Ok(id)
    }

    /// Resolve a capture (`write`) failure.
    ///
    /// A storage failure may have persisted part of the body or the marker, so
    /// the commit is uncertain and the prepared sinks are kept. Any other
    /// failure wrote no durable body: the marker is cleared and only then are
    /// the prepared sinks aborted. If clearing fails, aborting is not safe (a
    /// marker over rolled-back sinks could be promoted), so the sinks and the
    /// evidence are kept and the storage error surfaces.
    async fn capture_failed(
        &mut self,
        id: u64,
        prepared: Prepared,
        error: ConnectorError,
    ) -> Result<u64, ConnectorError> {
        if matches!(error, ConnectorError::Storage(_)) {
            self.fail(CheckpointState::CommitUncertain, &error);
            return Err(error);
        }
        match clear_commit(self.backend.as_mut(), id) {
            Ok(()) => {
                self.sinks.abort(&prepared).await;
                self.fail(CheckpointState::Failed, &error);
                Err(error)
            }
            Err(state_error) => {
                let storage = ConnectorError::Storage(state_error);
                self.fail(CheckpointState::CommitUncertain, &storage);
                Err(storage)
            }
        }
    }

    /// Finish an interrupted commit detected by recovery: re-drive the sinks'
    /// `commit` (idempotent, only valid when every sink is re-drivable), publish
    /// `valid`, prune and clear the marker.
    pub async fn promote(&mut self, id: u64) -> Result<(), ConnectorError> {
        self.sinks.redrive_commit().await?;
        publish_valid(self.backend.as_mut(), id)?;
        prune(self.backend.as_mut(), self.retain).map_err(state_err)?;
        clear_commit(self.backend.as_mut(), id).map_err(state_err)?;
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
