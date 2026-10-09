//! Lookup for unresolved prepare and commit attempts.

use super::{Checkpoint, Checkpointer};
use crate::runtime::checkpoint_body::{
    PREPARE_MARKER, checkpoint_prefix, decode_err, invalid, read_body as decode_body,
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
    pub(crate) fn prepare_pending(&self, id: u64) -> Result<bool, ConnectorError> {
        let base = checkpoint_prefix(id);
        let preparing = self.get(&format!("{base}/prepare"))?;
        let committing = self.has_key(&format!("{base}/commit"))?;
        Ok(!committing && preparing.as_deref() == Some(PREPARE_MARKER))
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
        match self.read_body(id) {
            Ok(body) => Ok(body),
            Err(error @ ConnectorError::Unsupported(_)) => Err(error),
            Err(ConnectorError::Corruption(_) | ConnectorError::Missing(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Whether `key` is present in the store.
    pub(crate) fn has_key(&self, key: &str) -> Result<bool, ConnectorError> {
        Ok(self.get(key)?.is_some())
    }

    /// Read and decode the checkpoint `id`, rejecting an incomplete one.
    pub fn read(&self, id: u64) -> Result<Checkpoint, ConnectorError> {
        let base = checkpoint_prefix(id);
        if self.get(&format!("{base}/valid"))?.is_none() {
            return Err(invalid(id));
        }
        let engine = self
            .get(&format!("{base}/engine"))?
            .ok_or_else(|| invalid(id))?;
        let engine = decode_snapshot(&engine).map_err(decode_err)?;
        let sources = self
            .get(&format!("{base}/sources"))?
            .ok_or_else(|| invalid(id))?;
        let sources = decode_sources(&sources)?;
        Ok(Checkpoint {
            id,
            engine,
            sources,
        })
    }
}
