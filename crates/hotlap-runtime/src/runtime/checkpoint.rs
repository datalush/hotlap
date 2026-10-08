//! Checkpoint barrier: capture engine and source state into a [`StateBackend`].
//!
//! A checkpoint becomes visible only after every part is written: the engine
//! snapshot and source offsets land first, then a `valid` marker, and only then
//! the `latest` pointer. An interrupted write therefore never exposes a partial
//! checkpoint as the current one. Older checkpoints are pruned, keeping at
//! most `retain` of the newest.

use std::time::Duration;

use hotlap::Hotlap;
use hotlap::state::StateBackend;
use hotlap_engine::{EngineSnapshot, decode_framed, decode_snapshot};

use crate::runtime::checkpoint_body::{
    LATEST_KEY, checkpoint_prefix, engine_err, invalid, mark_valid, parse_id, state_err, write,
};
use crate::runtime::sink::SinkSync;
use crate::runtime::sink_barrier::SinkBarrier;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceState};

/// How many checkpoints [`CheckpointConfig`] keeps by default.
pub const DEFAULT_RETAIN: usize = 3;

/// Periodic checkpoint settings for a running engine.
pub struct CheckpointConfig {
    /// Minimum time between periodic checkpoints.
    pub interval: Duration,
    /// Destination store, moved into the engine thread.
    pub backend: Box<dyn StateBackend + Send>,
    /// Number of newest checkpoints to keep; older ones are deleted.
    ///
    /// Clamped to at least one, so the `latest` checkpoint always survives.
    pub retain: usize,
}

/// A decoded checkpoint: engine snapshot plus resumable source offsets.
#[derive(Debug)]
pub struct Checkpoint {
    /// Monotonic checkpoint id.
    pub id: u64,
    /// Engine state captured at the barrier.
    pub engine: EngineSnapshot,
    /// Source read positions captured at the barrier.
    pub sources: SourceState,
}

/// Writes coherent, versioned checkpoints to a [`StateBackend`].
pub struct Checkpointer {
    backend: Box<dyn StateBackend + Send>,
    next_id: u64,
    retain: usize,
    sinks: SinkBarrier,
}

impl Checkpointer {
    /// Open a checkpointer over `backend`, keeping the `retain` newest.
    pub fn new(backend: Box<dyn StateBackend + Send>, retain: usize) -> Self {
        Self {
            backend,
            next_id: 1,
            retain,
            sinks: SinkBarrier::new(Vec::new()),
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

    /// Continue the id sequence after a recovered checkpoint `id`, so the next
    /// checkpoint does not overwrite an existing one.
    pub fn resume_after(&mut self, id: u64) {
        self.next_id = self.next_id.max(id.saturating_add(1));
    }

    /// Capture `engine` and `source` and persist a new valid checkpoint,
    /// coordinating the sinks in two-phase-commit order.
    ///
    /// The order is drain -> prepare -> snapshot + write body -> commit -> mark
    /// valid. A failure before the commit finishes aborts the prepared sinks and
    /// discards the checkpoint, so the engine keeps its last valid one.
    ///
    /// Returns the id of the checkpoint; later reads must use [`Self::read`].
    pub async fn take(
        &mut self,
        engine: &Hotlap,
        source: &dyn Source,
    ) -> Result<u64, ConnectorError> {
        let id = self.next_id;
        self.sinks
            .around(|| write(self.backend.as_mut(), id, engine, source))
            .await?;
        mark_valid(self.backend.as_mut(), id, self.retain)?;
        self.next_id = self.next_id.saturating_add(1);
        Ok(id)
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
        let engine = decode_snapshot(&engine_bytes).map_err(engine_err)?;
        let source_bytes = self
            .get(&format!("{base}/sources"))?
            .ok_or_else(|| invalid(id))?;
        let sources = decode_framed(&source_bytes).map_err(engine_err)?;
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
