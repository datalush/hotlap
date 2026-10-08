//! Restore an engine snapshot when resuming.

use hotlap::Hotlap;
use hotlap_engine::EngineSnapshot;

use crate::runtime::checkpoint_body::hotlap_err;
use hotlap_connectors::error::ConnectorError;

/// Restore the engine snapshot through the public facade.
pub(super) fn restore(
    hotlap: &mut Hotlap,
    snapshot: &EngineSnapshot,
) -> Result<(), ConnectorError> {
    hotlap.restore(snapshot).map_err(hotlap_err)
}
