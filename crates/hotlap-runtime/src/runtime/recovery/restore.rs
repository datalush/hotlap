//! Restore an engine snapshot and seed source offsets when resuming.

use hotlap::Hotlap;
use hotlap_engine::EngineSnapshot;

use crate::runtime::checkpoint_body::hotlap_err;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceState};

/// Seeds the source's applied position with the captured offsets.
///
/// `Source::resume` reopens the splits at the captured offsets, but a custom
/// source may not record them as applied. Without this seed a checkpoint taken
/// before the first post-recovery commit captures a stale position, and a later
/// crash replays the log over the restored snapshot (double-apply). `commit`
/// only advances the position, so seeding an offset the source already recorded
/// is a no-op.
pub(super) fn seed_applied(source: &dyn Source, state: &SourceState) -> Result<(), ConnectorError> {
    for (&split, &offset) in &state.offsets {
        source.commit(split, offset)?;
    }
    Ok(())
}

/// Restore the engine snapshot through the public facade.
pub(super) fn restore(
    hotlap: &mut Hotlap,
    snapshot: &EngineSnapshot,
) -> Result<(), ConnectorError> {
    hotlap.restore(snapshot).map_err(hotlap_err)
}
