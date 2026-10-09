//! Checkpoint barrier: capture engine and source state into a [`StateBackend`].
//!
//! A checkpoint is visible only once the body, `valid` marker and `latest`
//! pointer are written; older ones are pruned to the `retain` newest.

mod commit;
mod config;
mod state;

pub use config::{CheckpointConfig, DEFAULT_RETAIN};
pub use state::CheckpointState;

use hotlap::state::StateBackend;
use hotlap_engine::{EngineSnapshot, decode_snapshot};

use crate::runtime::cancel::Cancel;
use crate::runtime::checkpoint_body::{
    LATEST_KEY, RESERVED_KEY, checkpoint_prefix, decode_err, id_exhausted, invalid, parse_id,
    read_body as decode_body, state_err,
};
use crate::runtime::sink::SinkSync;
use crate::runtime::sink_barrier::SinkBarrier;
use crate::runtime::source_checkpoint::{SavedView, SourcesCheckpoint, decode_sources};
use hotlap_connectors::error::ConnectorError;

/// A decoded checkpoint: engine snapshot plus resumable source offsets.
#[derive(Debug)]
pub struct Checkpoint {
    /// Monotonic checkpoint id.
    pub id: u64,
    /// Engine state captured at the barrier.
    pub engine: EngineSnapshot,
    /// Source read positions and identity captured at the barrier.
    pub sources: SourcesCheckpoint,
}

/// Writes coherent, versioned checkpoints to a [`StateBackend`].
pub struct Checkpointer {
    backend: Box<dyn StateBackend + Send>,
    next_id: u64,
    initialized: bool,
    retain: usize,
    sinks: SinkBarrier,
    state: CheckpointState,
    failure: Option<String>,
    cancel: Cancel,
}

impl Checkpointer {
    /// Open a checkpointer over `backend`, keeping the `retain` newest.
    pub fn new(backend: Box<dyn StateBackend + Send>, retain: usize) -> Self {
        Self {
            backend,
            next_id: 1,
            initialized: false,
            retain,
            sinks: SinkBarrier::new(Vec::new()),
            state: CheckpointState::Ready,
            failure: None,
            cancel: Cancel::new(),
        }
    }

    /// Coordinate the given sinks with the two-phase-commit protocol.
    ///
    /// Each [`SinkSync`] also carries its changelog channel, so a checkpoint
    /// drains queued deltas before committing the sink.
    pub fn with_sinks(mut self, sinks: Vec<SinkSync>) -> Self {
        self.sinks = SinkBarrier::new(sinks);
        self
    }

    /// Abandon a barrier await when the engine is stopping.
    ///
    /// Without it, a checkpoint parked on a stalled sink would hold the engine
    /// thread and block shutdown. A cancelled attempt is marked inconsistent so
    /// no later attempt continues over uncertain sink state.
    pub(crate) fn with_cancel(mut self, cancel: Cancel) -> Self {
        self.cancel = cancel;
        self
    }

    /// Continue the id sequence after a recovered checkpoint `id`, so the next
    /// checkpoint does not overwrite an existing one.
    ///
    /// An exhausted id space keeps the sequence at `u64::MAX`, so the next
    /// [`Self::take`] fails instead of wrapping around to reuse id zero.
    ///
    /// The store's durable high-water mark still applies: clearing `initialized`
    /// makes the next [`Self::take`] raise the sequence above any id reserved
    /// after this checkpoint, so a recovered id cannot be handed out again.
    pub fn resume_after(&mut self, id: u64) {
        self.next_id = match id.checked_add(1) {
            Some(next) => self.next_id.max(next),
            None => u64::MAX,
        };
        self.initialized = false;
    }

    /// Raise `next_id` above every id already present and every id reserved.
    ///
    /// Runs once, lazily, so a fresh [`Checkpointer`] over a non-empty store
    /// never starts at an id another run may already own, and does not pay a
    /// scan on every checkpoint. The reservation key survives pruning, so the
    /// floor holds even when only the high-water mark is left.
    fn ensure_id_floor(&mut self) -> Result<(), ConnectorError> {
        if self.initialized {
            return Ok(());
        }
        let mut floor = self.reserved_high_water()?;
        for id in
            crate::runtime::retention::checkpoint_ids(self.backend.as_ref()).map_err(state_err)?
        {
            floor = floor.max(id);
        }
        self.next_id = self
            .next_id
            .max(floor.checked_add(1).ok_or_else(id_exhausted)?);
        self.initialized = true;
        Ok(())
    }

    /// Highest id ever reserved, or zero when none is recorded.
    fn reserved_high_water(&self) -> Result<u64, ConnectorError> {
        match self.get_bytes(RESERVED_KEY)? {
            None => Ok(0),
            Some(bytes) => parse_id(&bytes),
        }
    }

    /// Whether every coordinated sink declares a re-drivable `commit`.
    pub(crate) fn redriable(&self) -> bool {
        self.sinks.redriable()
    }

    /// Whether discarding and replaying an interrupted commit is safe.
    pub(crate) fn replay_safe(&self) -> bool {
        self.sinks.replay_safe()
    }

    /// Every candidate checkpoint's named views must match `declared`.
    ///
    /// Checks the newest valid checkpoint and any interrupted commit above it,
    /// without consuming the store, so a caller can reject an incompatible
    /// declaration before opening writers or re-driving a commit.
    pub fn validate_views(&self, declared: &[SavedView]) -> Result<(), ConnectorError> {
        let valid = self.newest_valid()?;
        if let Some(checkpoint) = &valid {
            checkpoint
                .sources
                .validate_views(declared, &checkpoint.engine)?;
        }
        let floor = valid.as_ref().map(|checkpoint| checkpoint.id);
        if let Some(id) = self.pending_commit(floor)?
            && let Some(checkpoint) = self.read_body(id)?
        {
            checkpoint
                .sources
                .validate_views(declared, &checkpoint.engine)?;
        }
        Ok(())
    }

    /// Newest checkpoint with a `valid` marker that decodes, or `None`.
    ///
    /// An incompatible format is fatal; current-format corruption or an absent
    /// marker is skipped so an older checkpoint can still be selected.
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

    /// Newest id above `floor` with a commit marker but no valid marker.
    fn pending_commit(&self, floor: Option<u64>) -> Result<Option<u64>, ConnectorError> {
        let floor = floor.unwrap_or(0);
        for id in self.ids_descending()? {
            if id <= floor {
                break;
            }
            let base = format!("checkpoint/{id}");
            if self.has_key(&format!("{base}/commit"))?
                && !self.has_key(&format!("{base}/valid"))?
            {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    /// Recover the store, for example to move it into a new checkpointer.
    pub fn into_backend(self) -> Box<dyn StateBackend + Send> {
        self.backend
    }

    /// Decode the body of `id` without requiring the `valid` marker.
    pub(crate) fn read_body(&self, id: u64) -> Result<Option<Checkpoint>, ConnectorError> {
        let body = decode_body(self.backend.as_ref(), id)?;
        Ok(body.map(|(engine, sources)| Checkpoint {
            id,
            engine,
            sources,
        }))
    }

    /// Whether `key` is present in the store.
    pub(crate) fn has_key(&self, key: &str) -> Result<bool, ConnectorError> {
        Ok(self.get(key)?.is_some())
    }

    /// Id of the newest complete checkpoint, or `None` when none exists.
    pub fn latest(&self) -> Result<Option<u64>, ConnectorError> {
        match self.get_bytes(LATEST_KEY)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(parse_id(&bytes)?)),
        }
    }

    /// Every checkpoint id present in the store, newest first.
    pub fn ids_descending(&self) -> Result<Vec<u64>, ConnectorError> {
        let mut ids =
            crate::runtime::retention::checkpoint_ids(self.backend.as_ref()).map_err(state_err)?;
        ids.sort_unstable();
        ids.dedup();
        ids.reverse();
        Ok(ids)
    }

    /// Read and decode the checkpoint `id`, rejecting an incomplete one.
    pub fn read(&self, id: u64) -> Result<Checkpoint, ConnectorError> {
        let base = checkpoint_prefix(id);
        if self.get(&format!("{base}/valid"))?.is_none() {
            return Err(invalid(id));
        }
        let engine_bytes = self
            .get(&format!("{base}/engine"))?
            .ok_or_else(|| invalid(id))?;
        let engine = decode_snapshot(&engine_bytes).map_err(decode_err)?;
        let source_bytes = self
            .get(&format!("{base}/sources"))?
            .ok_or_else(|| invalid(id))?;
        let sources = decode_sources(&source_bytes)?;
        Ok(Checkpoint {
            id,
            engine,
            sources,
        })
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConnectorError> {
        self.get_bytes(key.as_bytes())
    }

    fn get_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ConnectorError> {
        self.backend.get(key).map_err(state_err)
    }
}
