//! Recovery: restore the newest valid checkpoint, reopen sources and replay.
//!
//! A restart loads the last valid checkpoint (engine snapshot plus captured
//! source offsets), restores the engine, reopens each source split at its
//! captured offset and feeds the resulting stream back in. Because the engine
//! is restored to the checkpoint before the offset is replayed, replaying the
//! log from that offset yields exactly the state the crashed run had reached.

use hotlap::Hotlap;
use hotlap_engine::EngineSnapshot;

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::checkpoint_body::hotlap_err;
use crate::runtime::pipeline;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceStream};

/// The last valid checkpoint, if the store holds one.
pub struct Recovery;

impl Recovery {
    /// Newest valid checkpoint, or `None` for a clean start.
    ///
    /// The `latest` pointer is tried first. If its checkpoint fails to decode
    /// or validate, older checkpoints are tried newest-first, so a corrupt tip
    /// does not abort startup while a valid predecessor remains. A checkpoint
    /// is only visible once every part (engine, sources, `valid` marker) is
    /// written, so a readable one is always coherent.
    pub fn load(checkpointer: &Checkpointer) -> Result<Option<Checkpoint>, ConnectorError> {
        if let Some(id) = checkpointer.latest()?
            && let Ok(checkpoint) = checkpointer.read(id)
        {
            return Ok(Some(checkpoint));
        }
        for id in checkpointer.ids_descending()? {
            if let Ok(checkpoint) = checkpointer.read(id) {
                return Ok(Some(checkpoint));
            }
        }
        Ok(None)
    }

    /// Restore `checkpoint` into `hotlap` and reopen `source` at the captured
    /// offsets, yielding a stream that replays from the checkpoint.
    ///
    /// [`SourceState`](hotlap_connectors::source::SourceState) holds the offset of the
    /// **next** record to read, advanced only after a batch is applied. The
    /// checkpoint therefore already contains every record below that offset and
    /// replay must start exactly there: starting one record earlier duplicates,
    /// one later loses. The runtime commits offsets after ingestion, so no
    /// in-flight batch can break the invariant.
    ///
    /// Errors when the source can no longer serve a captured offset, so
    /// insufficient retention fails loudly instead of losing records.
    pub fn resume(
        hotlap: &mut Hotlap,
        source: &dyn Source,
        checkpoint: &Checkpoint,
    ) -> Result<SourceStream, ConnectorError> {
        restore(hotlap, &checkpoint.engine)?;
        let splits = source.resume(&checkpoint.sources)?;
        pipeline::merged_stream_from(source, &splits)
    }
}

/// Restore the engine snapshot through the public facade.
fn restore(hotlap: &mut Hotlap, snapshot: &EngineSnapshot) -> Result<(), ConnectorError> {
    hotlap.restore(snapshot).map_err(hotlap_err)
}
