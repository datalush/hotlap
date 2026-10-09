//! Per-source validation and resume helpers for recovery.
//!
//! Every source is checked against the checkpoint and reopened at its own
//! applied offset, so two sources never share offsets or override each other.

use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Offset, Source, Split, SplitId};

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::source_checkpoint::SourcesCheckpoint;
use crate::runtime::sources::Sources;

/// Reject a checkpoint whose source identities, schemas or offsets disagree
/// with the declared sources or the engine snapshot.
///
/// This runs before any engine restore or source resume, so an incompatible
/// declaration fails loudly instead of consuming records against wrong state.
pub(super) fn validate(
    sources: &Sources,
    checkpoint: &SourcesCheckpoint,
    engine: &hotlap_engine::EngineSnapshot,
) -> Result<(), ConnectorError> {
    checkpoint.validate(sources, engine)
}

/// Reopen every declared source at its saved applied offset and seed that
/// position, returning one split list per source in entry order.
///
/// `validate` must have accepted the checkpoint first; this only resolves
/// offsets. A source that cannot serve a captured offset fails here, before the
/// runtime starts consuming, so partial streams are never served.
pub(super) fn resume(
    sources: &Sources,
    checkpoint: &SourcesCheckpoint,
) -> Result<Vec<Vec<Split>>, ConnectorError> {
    let mut per_source = Vec::with_capacity(sources.entries().len());
    for entry in sources.entries() {
        let saved = checkpoint.entry(entry.id)?;
        let splits = entry.source.resume(&saved.state)?;
        seed_applied(entry.source.as_ref(), &saved.state.offsets)?;
        per_source.push(splits);
    }
    Ok(per_source)
}

/// Seed the source's applied position with the captured offsets.
///
/// `Source::resume` reopens the splits at the captured offsets, but a custom
/// source may not record them as applied. Without this seed a checkpoint taken
/// before the first post-recovery commit captures a stale position, and a later
/// crash replays the log over the restored snapshot (double-apply). `commit`
/// only advances the position, so seeding an offset the source already recorded
/// is a no-op.
fn seed_applied(
    source: &dyn Source,
    offsets: &std::collections::BTreeMap<SplitId, Offset>,
) -> Result<(), ConnectorError> {
    for (&split, &offset) in offsets {
        source.commit(split, offset)?;
    }
    Ok(())
}

/// Read a valid checkpoint, tolerating current-format corruption and absence.
///
/// An `Unsupported` error (foreign or incompatible format) is fatal and
/// propagates, and so is any operational failure: a store error must never be
/// mistaken for corruption or absence. Only a `Corruption` of the current
/// format or a `Missing` marker/part means the caller may fall back to an older
/// checkpoint.
pub(super) fn read_valid(
    checkpointer: &Checkpointer,
    id: u64,
) -> Result<Option<Checkpoint>, ConnectorError> {
    match checkpointer.read(id) {
        Ok(checkpoint) => Ok(Some(checkpoint)),
        Err(error @ ConnectorError::Unsupported(_)) => Err(error),
        Err(ConnectorError::Corruption(_) | ConnectorError::Missing(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Read a checkpoint body without its `valid` marker, same tolerance as above.
pub(super) fn read_body(
    checkpointer: &Checkpointer,
    id: u64,
) -> Result<Option<Checkpoint>, ConnectorError> {
    match checkpointer.read_body(id) {
        Ok(body) => Ok(body),
        Err(error @ ConnectorError::Unsupported(_)) => Err(error),
        Err(ConnectorError::Corruption(_) | ConnectorError::Missing(_)) => Ok(None),
        Err(error) => Err(error),
    }
}
