//! Restore a checkpoint into the engine and reopen its sources.

use hotlap::Hotlap;

use super::Recovery;
use super::restore::restore;
use super::sources;
use crate::runtime::checkpoint::Checkpoint;
use crate::runtime::sources::{InputStream, Sources};
use hotlap_connectors::error::ConnectorError;

impl Recovery {
    /// Restore `checkpoint` into `hotlap` and reopen `sources` at the captured
    /// offsets, yielding a stream that replays from the checkpoint.
    ///
    /// `SourceState` holds the offset of the **next** record to read, advanced
    /// only after a batch is applied. The checkpoint therefore already contains
    /// every record below that offset and replay must start exactly there:
    /// starting one record earlier duplicates, one later loses. The runtime
    /// commits offsets after ingestion, so no in-flight batch can break it.
    ///
    /// The checkpoint is validated before the engine is restored or any source
    /// is reopened. Errors when a source can no longer serve a captured offset,
    /// so insufficient retention fails loudly instead of losing records.
    pub fn resume(
        hotlap: &mut Hotlap,
        sources: &Sources,
        checkpoint: &Checkpoint,
    ) -> Result<InputStream, ConnectorError> {
        sources::validate(sources, &checkpoint.sources, &checkpoint.engine)?;
        restore(hotlap, &checkpoint.engine)?;
        let splits = sources::resume(sources, &checkpoint.sources)?;
        sources.stream_with(&splits)
    }
}
