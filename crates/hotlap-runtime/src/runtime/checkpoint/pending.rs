//! Lookup for unresolved prepare and commit attempts.

use super::{Checkpoint, Checkpointer};
use crate::runtime::checkpoint_body::{
    COMMIT_MARKER, PREPARE_MARKER, checkpoint_prefix, decode_err, invalid,
    read_body as decode_body, validate_engine_snapshot,
};
use crate::runtime::source_checkpoint::decode_sources;
use hotlap_connectors::error::ConnectorError;
use hotlap_engine::decode_snapshot;

impl Checkpointer {
    /// Newest id above `floor` with prepare or commit evidence and no valid marker.
    pub(super) fn pending_commit(&self, floor: Option<u64>) -> Result<Option<u64>, ConnectorError> {
        let floor = floor.unwrap_or(0);
        for id in self.ids_descending()? {
            if id <= floor {
                break;
            }
            let base = format!("checkpoint/{id}");
            if (self.has_key(&format!("{base}/commit"))?
                || self.has_key(&format!("{base}/prepare"))?)
                && !self.has_key(&format!("{base}/valid"))?
            {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    /// Whether an interrupted attempt has not durably entered commit.
    pub(crate) fn pending_phase(&self, id: u64) -> Result<PendingPhase, ConnectorError> {
        let base = checkpoint_prefix(id);
        if let Some(commit) = self.get(&format!("{base}/commit"))? {
            return Ok(if commit == COMMIT_MARKER {
                PendingPhase::Commit
            } else {
                PendingPhase::Invalid
            });
        }
        Ok(match self.get(&format!("{base}/prepare"))? {
            None => PendingPhase::Missing,
            Some(prepare) if prepare == PREPARE_MARKER => PendingPhase::Prepare,
            Some(_) => PendingPhase::Invalid,
        })
    }

    /// Decode a body without requiring its `valid` marker.
    pub(crate) fn read_body(&self, id: u64) -> Result<Option<Checkpoint>, ConnectorError> {
        let body = decode_body(self.backend.as_ref(), id)?;
        Ok(body.map(|(engine, sources)| Checkpoint {
            id,
            engine,
            sources,
        }))
    }

    /// Treat only missing or corrupt current-format bodies as absent.
    pub(crate) fn read_body_for_recovery(
        &self,
        id: u64,
    ) -> Result<Option<Checkpoint>, ConnectorError> {
        let manifest =
            crate::runtime::checkpoint_body::read_participant_manifest(self.backend.as_ref(), id)?;
        let body = self.read_body(id).and_then(|body| match body {
            Some(checkpoint) => {
                validate_engine_snapshot(&checkpoint.engine).map(|()| Some(checkpoint))
            }
            None => Ok(None),
        });
        let body = match body {
            Ok(body) => body,
            Err(error @ ConnectorError::Unsupported(_)) => return Err(error),
            Err(ConnectorError::Corruption(_) | ConnectorError::Missing(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if let Some(checkpoint) = &body {
            manifest.matches_saved_sources(&checkpoint.sources.entries)?;
            manifest.validate_tapped_views(&checkpoint.sources.views, &checkpoint.engine)?;
        }
        Ok(body)
    }

    /// Whether `key` is present in the store.
    pub(crate) fn has_key(&self, key: &str) -> Result<bool, ConnectorError> {
        Ok(self.get(key)?.is_some())
    }

    /// Read and decode the checkpoint `id`, rejecting an incomplete one.
    pub fn read(&self, id: u64) -> Result<Checkpoint, ConnectorError> {
        self.read_with_sink_validation(id, true)
    }

    /// Decode for internal selection after the caller validated participants by
    /// their metadata-only descriptions instead of open sink instances.
    pub(crate) fn read_for_selection(&self, id: u64) -> Result<Checkpoint, ConnectorError> {
        self.read_with_sink_validation(id, false)
    }

    fn read_with_sink_validation(
        &self,
        id: u64,
        validate_sinks: bool,
    ) -> Result<Checkpoint, ConnectorError> {
        let base = checkpoint_prefix(id);
        if self.get(&format!("{base}/valid"))?.is_none() {
            return Err(invalid(id));
        }
        let manifest =
            crate::runtime::checkpoint_body::read_participant_manifest(self.backend.as_ref(), id)?;
        if validate_sinks {
            manifest.matches_sinks(&self.sinks)?;
        }
        let engine = self
            .get(&format!("{base}/engine"))?
            .ok_or_else(|| invalid(id))?;
        let engine = decode_snapshot(&engine).map_err(decode_err)?;
        validate_engine_snapshot(&engine)?;
        let sources = self
            .get(&format!("{base}/sources"))?
            .ok_or_else(|| invalid(id))?;
        let sources = decode_sources(&sources)?;
        manifest.matches_saved_sources(&sources.entries)?;
        manifest.validate_tapped_views(&sources.views, &engine)?;
        Ok(Checkpoint {
            id,
            engine,
            sources,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PendingPhase {
    Missing,
    Prepare,
    Commit,
    Invalid,
}
