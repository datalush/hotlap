//! Checkpoint body: encode state + offsets and publish validity.

use hotlap::Hotlap;
use hotlap::state::{StateBackend, StateError};
use hotlap_engine::{EngineError, encode_framed, encode_snapshot};

use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::Source;

/// Value stored under `checkpoint/<id>/valid` once a checkpoint is complete.
pub(crate) const VALID_MARKER: &[u8] = b"1";
/// Key holding the id of the newest fully written checkpoint.
pub(crate) const LATEST_KEY: &[u8] = b"checkpoint/latest";

pub(crate) fn checkpoint_prefix(id: u64) -> String {
    format!("checkpoint/{id}")
}

/// Encode the engine snapshot and source offsets under `checkpoint/<id>/`.
///
/// Only the body is written here; [`mark_valid`] publishes it afterwards, so an
/// interrupted body never becomes the current checkpoint.
pub(crate) async fn write(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
    engine: &Hotlap,
    source: &dyn Source,
) -> Result<(), ConnectorError> {
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
}

/// Write the `valid` marker and `latest` pointer, then prune older checkpoints.
pub(crate) fn mark_valid(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
    retain: usize,
) -> Result<(), ConnectorError> {
    let base = checkpoint_prefix(id);
    backend
        .put(format!("{base}/valid").as_bytes(), VALID_MARKER.to_vec())
        .map_err(state_err)?;
    backend
        .put(LATEST_KEY, id.to_le_bytes().to_vec())
        .map_err(state_err)?;
    // The checkpoint is committed; pruning only trims older ones.
    crate::runtime::retention::prune(backend, retain).map_err(state_err)
}

/// Decode an 8-byte little-endian checkpoint id.
pub(crate) fn parse_id(bytes: &[u8]) -> Result<u64, ConnectorError> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| ConnectorError::Infrastructure("checkpoint id is not 8 bytes".into()))?;
    Ok(u64::from_le_bytes(array))
}

/// Error for an unknown or incomplete checkpoint.
pub(crate) fn invalid(id: u64) -> ConnectorError {
    ConnectorError::Infrastructure(format!("checkpoint {id} is missing or not valid"))
}

/// Map a hotlap facade error onto the connector error type.
pub(crate) fn hotlap_err(error: hotlap::HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(error.0)
}

/// Map an engine codec error onto the connector error type.
pub(crate) fn engine_err(error: EngineError) -> ConnectorError {
    ConnectorError::Infrastructure(error.to_string())
}

/// Map a state backend error onto the connector error type.
pub(crate) fn state_err(error: StateError) -> ConnectorError {
    ConnectorError::Infrastructure(error.to_string())
}
