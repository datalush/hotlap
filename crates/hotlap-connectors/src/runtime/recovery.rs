//! Recovery: restore the newest valid checkpoint, reopen sources and replay.
//!
//! A restart loads the last valid checkpoint (engine snapshot plus captured
//! source offsets), restores the engine, reopens each source split at its
//! captured offset and feeds the resulting stream back in. Because the engine
//! is restored to the checkpoint before the offset is replayed, replaying the
//! log from that offset yields exactly the state the crashed run had reached.

use hotlap::Hotlap;
use hotlap_engine::EngineSnapshot;

use crate::error::ConnectorError;
use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::checkpoint_body::hotlap_err;
use crate::runtime::pipeline;
use crate::source::{Source, SourceStream};

/// The last valid checkpoint, if the store holds one.
pub struct Recovery;

impl Recovery {
    /// Newest valid checkpoint, or `None` for a clean start.
    ///
    /// An interrupted write never becomes `latest`, so a returned checkpoint is
    /// always complete and decodable.
    pub fn load(checkpointer: &Checkpointer) -> Result<Option<Checkpoint>, ConnectorError> {
        match checkpointer.latest()? {
            Some(id) => Ok(Some(checkpointer.read(id)?)),
            None => Ok(None),
        }
    }

    /// Restore `checkpoint` into `hotlap` and reopen `source` at the captured
    /// offsets, yielding a stream that replays from the checkpoint.
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
