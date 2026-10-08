//! Checkpoint body: encode state + offsets and publish validity.

use hotlap::Hotlap;
use hotlap::state::{StateBackend, StateError};
use hotlap_engine::{EngineError, EngineSnapshot, decode_snapshot, encode_snapshot};

use crate::runtime::source_checkpoint::{SourcesCheckpoint, decode_sources, encode_sources};
use crate::runtime::sources::Sources;
use hotlap_connectors::error::ConnectorError;

/// Value stored under `checkpoint/<id>/valid` once a checkpoint is complete.
pub(crate) const VALID_MARKER: &[u8] = b"1";
/// Value stored under `checkpoint/<id>/commit` once a commit is intended.
pub(crate) const COMMIT_MARKER: &[u8] = b"1";
/// Key holding the id of the newest fully written checkpoint.
pub(crate) const LATEST_KEY: &[u8] = b"checkpoint/latest";

pub(crate) fn checkpoint_prefix(id: u64) -> String {
    format!("checkpoint/{id}")
}

/// Encode the engine snapshot and source offsets under `checkpoint/<id>/`.
///
/// The body is written first and the durable commit marker last, before the
/// sinks commit; [`mark_valid`] publishes the checkpoint afterwards. An
/// interrupted body never becomes the current checkpoint, and a marker left by
/// a crash tells recovery that the current checkpoint was mid-commit.
pub(crate) async fn write(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
    engine: &Hotlap,
    sources: &Sources,
) -> Result<(), ConnectorError> {
    let snapshot = engine.checkpoint().map_err(hotlap_err)?;
    let engine_bytes = encode_snapshot(&snapshot).map_err(engine_err)?;
    let sources_checkpoint = SourcesCheckpoint::capture(sources)?;
    let source_bytes = encode_sources(&sources_checkpoint)?;
    let base = checkpoint_prefix(id);
    backend
        .put(format!("{base}/engine").as_bytes(), engine_bytes)
        .map_err(state_err)?;
    backend
        .put(format!("{base}/sources").as_bytes(), source_bytes)
        .map_err(state_err)?;
    backend
        .put(format!("{base}/commit").as_bytes(), COMMIT_MARKER.to_vec())
        .map_err(state_err)
}

/// Remove the durable commit marker of `id`; a no-op when it is absent.
pub(crate) fn clear_commit(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
) -> Result<(), StateError> {
    backend.delete(format!("{}/commit", checkpoint_prefix(id)).as_bytes())
}

/// Decode the body of `id` without requiring the `valid` marker.
///
/// Returns `None` when either part is missing, so a marker over an incomplete
/// body is not mistaken for a committed checkpoint.
pub(crate) fn read_body(
    backend: &dyn StateBackend,
    id: u64,
) -> Result<Option<(EngineSnapshot, SourcesCheckpoint)>, ConnectorError> {
    let base = checkpoint_prefix(id);
    let Some(engine_bytes) = backend
        .get(format!("{base}/engine").as_bytes())
        .map_err(state_err)?
    else {
        return Ok(None);
    };
    let engine = decode_snapshot(&engine_bytes).map_err(engine_err)?;
    let Some(source_bytes) = backend
        .get(format!("{base}/sources").as_bytes())
        .map_err(state_err)?
    else {
        return Ok(None);
    };
    let sources = decode_sources(&source_bytes)?;
    Ok(Some((engine, sources)))
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
