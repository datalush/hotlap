//! Commit protocol: two-phase sink commit plus durable marker handling.

use hotlap::Hotlap;

use crate::runtime::checkpoint::{CheckpointState, Checkpointer};
use crate::runtime::checkpoint_body::{
    checkpoint_prefix, clear_commit, clear_prepare_intent, id_exhausted, mark_commit_intent,
    mark_prepare_intent, publish_valid, reserve, state_err, write_body, write_participant_manifest,
};
use crate::runtime::participants::ParticipantsManifest;
use crate::runtime::retention::prune;
use crate::runtime::sink_barrier::Prepared;
use crate::runtime::sources::Sources;
use hotlap_connectors::error::ConnectorError;

impl Checkpointer {
    /// Capture `engine` and `sources` and persist a new valid checkpoint,
    /// coordinating the sinks in two-phase-commit order.
    ///
    /// The order is reserve -> drain -> prepare marker -> prepare -> body ->
    /// commit marker -> commit -> publish valid. Confirmed rollback clears the
    /// prepare marker only after every abort succeeds.
    ///
    /// Returns the id of the checkpoint; later reads must use [`Self::read`].
    pub async fn take(
        &mut self,
        engine: &Hotlap,
        sources: &Sources,
    ) -> Result<u64, ConnectorError> {
        self.block_if_failed()?;
        let manifest = ParticipantsManifest::capture(sources, &self.sinks)?;
        let snapshot = engine
            .checkpoint()
            .map_err(crate::runtime::checkpoint_body::hotlap_err)?;
        let views =
            crate::runtime::source_checkpoint::SavedView::from_registry(&engine.view_registry());
        manifest.validate_tapped_views(&views, &snapshot)?;
        // Resolve the starting id and advance the in-memory sequence before the
        // durable reservation: an ambiguous reservation that persists then
        // reports failure must not let this checkpointer retry the same id.
        self.ensure_id_floor()?;
        let id = self.next_id;
        let next = id.checked_add(1).ok_or_else(id_exhausted)?;
        self.next_id = next;
        reserve(self.backend.as_mut(), id)?;
        write_participant_manifest(self.backend.as_mut(), id, &manifest)?;

        if let Err(error) = self.sinks.drain(&self.cancel).await {
            // The engine and source offsets may already have advanced, and a
            // writer may have rolled back after a write failure. No drain error
            // is safe to retry on this runtime, whether or not it was cancelled.
            self.fail(CheckpointState::Failed, &error);
            return Err(error);
        }
        if let Err(error) = mark_prepare_intent(self.backend.as_mut(), id) {
            self.fail(
                if matches!(error, ConnectorError::Storage(_)) {
                    CheckpointState::CommitUncertain
                } else {
                    CheckpointState::Failed
                },
                &error,
            );
            return Err(error);
        }
        let prepared = match self.sinks.prepare(&self.cancel).await {
            Ok(prepared) => prepared,
            Err(failure) => {
                if failure.aborted
                    && let Err(error) = clear_prepare_intent(self.backend.as_mut(), id)
                {
                    let storage = ConnectorError::Storage(error);
                    self.fail(CheckpointState::CommitUncertain, &storage);
                    return Err(storage);
                }
                let state = if failure.aborted {
                    CheckpointState::Failed
                } else {
                    CheckpointState::CommitUncertain
                };
                self.fail(state, &failure.error);
                return Err(failure.error);
            }
        };

        if let Err(error) = manifest.matches_sinks(&self.sinks) {
            return self.capture_failed(id, prepared, error).await;
        }

        if let Err(error) = write_body(self.backend.as_mut(), id, engine, sources).await {
            return self.capture_failed(id, prepared, error).await;
        }
        if let Err(error) = mark_commit_intent(self.backend.as_mut(), id) {
            self.fail(CheckpointState::CommitUncertain, &error);
            return Err(error);
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
    /// Body-capture errors happen before commit intent, so abort first; clear
    /// the prepare marker only when every participant confirms rollback.
    async fn capture_failed(
        &mut self,
        id: u64,
        prepared: Prepared,
        error: ConnectorError,
    ) -> Result<u64, ConnectorError> {
        if let Err(abort_error) = self.sinks.abort(&prepared).await {
            self.fail(CheckpointState::CommitUncertain, &abort_error);
            return Err(abort_error);
        }
        match clear_prepare_intent(self.backend.as_mut(), id) {
            Ok(()) => {
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
        let manifest =
            crate::runtime::checkpoint_body::read_participant_manifest(self.backend.as_ref(), id)?;
        manifest.matches_sinks(&self.sinks)?;
        let checkpoint = self
            .read_body(id)?
            .ok_or_else(|| crate::runtime::checkpoint_body::invalid(id))?;
        crate::runtime::checkpoint_body::validate_engine_snapshot(&checkpoint.engine)?;
        manifest.matches_saved_sources(&checkpoint.sources.entries)?;
        manifest.validate_tapped_views(&checkpoint.sources.views, &checkpoint.engine)?;
        self.sinks.redrive_commit().await?;
        publish_valid(self.backend.as_mut(), id)?;
        prune(self.backend.as_mut(), self.retain).map_err(state_err)?;
        clear_commit(self.backend.as_mut(), id).map_err(state_err)?;
        Ok(())
    }

    /// Delete an interrupted checkpoint that recovery cannot promote.
    pub fn discard_commit(&mut self, id: u64) -> Result<(), ConnectorError> {
        let prefix = format!("{}/", checkpoint_prefix(id));
        let keys = self.backend.list(prefix.as_bytes()).map_err(state_err)?;
        if keys.is_empty() {
            return Ok(());
        }
        let manifest =
            crate::runtime::checkpoint_body::read_participant_manifest(self.backend.as_ref(), id)?;
        manifest.matches_sinks(&self.sinks)?;
        for key in keys {
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
        self.sweep_stale_commits_with_replay_safety(self.replay_safe())
    }

    pub(crate) fn sweep_stale_commits_with_replay_safety(
        &mut self,
        replay_safe: bool,
    ) -> Result<(), ConnectorError> {
        self.validate_sink_participant_manifests()?;
        for id in self.ids_descending()? {
            let base = checkpoint_prefix(id);
            if self.has_key(&format!("{base}/valid"))?
                && (self.has_key(&format!("{base}/commit"))?
                    || self.has_key(&format!("{base}/prepare"))?)
            {
                match self.read(id) {
                    Ok(_) => {}
                    Err(ConnectorError::Corruption(_) | ConnectorError::Missing(_))
                        if replay_safe =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error),
                }
                clear_commit(self.backend.as_mut(), id).map_err(state_err)?;
            }
        }
        Ok(())
    }
}
