//! Checkpoint barrier: capture engine and source state into a [`StateBackend`].
//!
//! A checkpoint becomes visible only after every part is written: the engine
//! snapshot and source offsets land first, then a `valid` marker, and only then
//! the `latest` pointer. An interrupted write therefore never exposes a partial
//! checkpoint as the current one. Older checkpoints are pruned, keeping at
//! most `retain` of the newest.

use std::sync::Arc;
use std::time::Duration;

use hotlap::Hotlap;
use hotlap::state::{StateBackend, StateError};
use hotlap_engine::{
    EngineError, EngineSnapshot, decode_framed, decode_snapshot, encode_framed, encode_snapshot,
};

use crate::error::ConnectorError;
use crate::runtime::sink_barrier::SinkBarrier;
use crate::sink::Sink;
use crate::source::{Source, SourceState};

/// Value stored under `checkpoint/<id>/valid` once a checkpoint is complete.
const VALID_MARKER: &[u8] = b"1";
/// Key holding the id of the newest fully written checkpoint.
const LATEST_KEY: &[u8] = b"checkpoint/latest";
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
    pub fn with_sinks(mut self, sinks: Vec<Arc<dyn Sink>>) -> Self {
        self.sinks = SinkBarrier::new(sinks);
        self
    }

    /// Capture `engine` and `source` and persist a new valid checkpoint,
    /// coordinating the sinks in two-phase-commit order.
    ///
    /// The order is prepare -> snapshot + write body -> commit -> mark valid.
    /// A failure before the commit finishes aborts the prepared sinks and
    /// discards the checkpoint, so the engine keeps its last valid one.
    ///
    /// Returns the id of the checkpoint; later reads must use [`Self::read`].
    pub async fn take(
        &mut self,
        engine: &Hotlap,
        source: &dyn Source,
    ) -> Result<u64, ConnectorError> {
        let id = self.next_id;
        let backend = &mut self.backend;
        let result = self
            .sinks
            .around(|| async {
                let snapshot = engine.checkpoint().map_err(hotlap_err)?;
                let engine_bytes = encode_snapshot(&snapshot).map_err(engine_err)?;
                let source_bytes = encode_framed(&source.state()).map_err(engine_err)?;
                let base = checkpoint_prefix(id);
                backend
                    .put(format!("{base}/engine").as_bytes(), engine_bytes)
                    .map_err(state_err)?;
                backend
                    .put(format!("{base}/sources").as_bytes(), source_bytes)
                    .map_err(state_err)
            })
            .await;
        result?;
        self.mark_valid(id)?;
        self.next_id = self.next_id.saturating_add(1);
        Ok(id)
    }

    fn mark_valid(&mut self, id: u64) -> Result<(), ConnectorError> {
        let base = checkpoint_prefix(id);
        self.put(&format!("{base}/valid"), VALID_MARKER.to_vec())?;
        self.put_bytes(LATEST_KEY, id.to_le_bytes().to_vec())?;
        // The checkpoint is committed; pruning only trims older ones.
        crate::runtime::retention::prune(self.backend.as_mut(), self.retain).map_err(state_err)
    }

    /// Id of the newest complete checkpoint, or `None` when none exists.
    pub fn latest(&self) -> Result<Option<u64>, ConnectorError> {
        match self.get_bytes(LATEST_KEY)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(parse_id(&bytes)?)),
        }
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

    /// Store `value` under the UTF-8 key `key`.
    fn put(&mut self, key: &str, value: Vec<u8>) -> Result<(), ConnectorError> {
        self.put_bytes(key.as_bytes(), value)
    }

    fn put_bytes(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), ConnectorError> {
        self.backend.put(key, value).map_err(state_err)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConnectorError> {
        self.get_bytes(key.as_bytes())
    }

    fn get_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ConnectorError> {
        self.backend.get(key).map_err(state_err)
    }
}

fn checkpoint_prefix(id: u64) -> String {
    format!("checkpoint/{id}")
}

/// Decode an 8-byte little-endian checkpoint id.
fn parse_id(bytes: &[u8]) -> Result<u64, ConnectorError> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| ConnectorError::Infrastructure("checkpoint id is not 8 bytes".into()))?;
    Ok(u64::from_le_bytes(array))
}

/// Error for an unknown or incomplete checkpoint.
fn invalid(id: u64) -> ConnectorError {
    ConnectorError::Infrastructure(format!("checkpoint {id} is missing or not valid"))
}

/// Map a hotlap facade error onto the connector error type.
fn hotlap_err(error: hotlap::HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(error.0)
}

/// Map an engine codec error onto the connector error type.
fn engine_err(error: EngineError) -> ConnectorError {
    ConnectorError::Infrastructure(error.to_string())
}

/// Map a state backend error onto the connector error type.
fn state_err(error: StateError) -> ConnectorError {
    ConnectorError::Infrastructure(error.to_string())
}
