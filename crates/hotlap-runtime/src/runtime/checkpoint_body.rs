//! Checkpoint body: encode state + offsets and publish validity.

use hotlap::Hotlap;
use hotlap::state::{StateBackend, StateError};
use hotlap_engine::{EngineCore, EngineError, EngineSnapshot, decode_snapshot, encode_snapshot};

use crate::runtime::source_checkpoint::{
    SavedView, SourcesCheckpoint, decode_sources, encode_sources,
};
use crate::runtime::sources::Sources;
use hotlap_connectors::error::ConnectorError;

/// Value stored under `checkpoint/<id>/valid` once a checkpoint is complete.
pub(crate) const VALID_MARKER: &[u8] = b"1";
/// Value stored under `checkpoint/<id>/commit` once a commit is intended.
pub(crate) const COMMIT_MARKER: &[u8] = b"1";
/// Value stored under `checkpoint/<id>/prepare` before external prepare starts.
pub(crate) const PREPARE_MARKER: &[u8] = b"prepare-v1";
/// Key holding the id of the newest fully written checkpoint.
pub(crate) const LATEST_KEY: &[u8] = b"checkpoint/latest";
/// Key holding the highest checkpoint id ever reserved.
///
/// Written before an attempt touches the sinks or the body, so a crash during
/// an ambiguous attempt cannot make a later run reuse that id even if every
/// trace of the attempt is pruned.
pub(crate) const RESERVED_KEY: &[u8] = b"checkpoint/reserved";

pub(crate) fn checkpoint_prefix(id: u64) -> String {
    format!("checkpoint/{id}")
}

/// Encode the engine snapshot and source offsets under `checkpoint/<id>/`.
///
/// The engine snapshot and source offsets are written before the commit marker.
pub(crate) async fn write_body(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
    engine: &Hotlap,
    sources: &Sources,
) -> Result<(), ConnectorError> {
    let snapshot = engine.checkpoint().map_err(hotlap_err)?;
    let engine_bytes = encode_snapshot(&snapshot).map_err(encode_err)?;
    let views = SavedView::from_registry(&engine.view_registry());
    let sources_checkpoint = SourcesCheckpoint::capture_with_views(sources, &views)?;
    let source_bytes = encode_sources(&sources_checkpoint)?;
    let base = checkpoint_prefix(id);
    backend
        .put(format!("{base}/engine").as_bytes(), engine_bytes)
        .map_err(state_err)?;
    backend
        .put(format!("{base}/sources").as_bytes(), source_bytes)
        .map_err(state_err)
}

/// Remove the durable commit marker of `id`; a no-op when it is absent.
pub(crate) fn clear_commit(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
) -> Result<(), StateError> {
    let base = checkpoint_prefix(id);
    backend.delete(format!("{base}/commit").as_bytes())?;
    backend.delete(format!("{base}/prepare").as_bytes())
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
    let engine = decode_snapshot(&engine_bytes).map_err(decode_err)?;
    let Some(source_bytes) = backend
        .get(format!("{base}/sources").as_bytes())
        .map_err(state_err)?
    else {
        return Ok(None);
    };
    let sources = decode_sources(&source_bytes)?;
    Ok(Some((engine, sources)))
}

/// Publish `id`: write the `valid` marker, then point `latest` at it.
///
/// Publication is separate from pruning so only a failure here leaves the
/// checkpoint's visibility in doubt; a later cleanup failure cannot un-publish
/// it.
pub(crate) fn publish_valid(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
) -> Result<(), ConnectorError> {
    let base = checkpoint_prefix(id);
    backend
        .put(format!("{base}/valid").as_bytes(), VALID_MARKER.to_vec())
        .map_err(state_err)?;
    backend
        .put(LATEST_KEY, id.to_le_bytes().to_vec())
        .map_err(state_err)
}

/// Durably reserve `id` before any ambiguous checkpoint attempt.
///
/// The reservation is a plain key outside the `checkpoint/<id>/` namespace, so
/// pruning never removes it and a later checkpointer can read the high-water
/// mark even when every body was pruned.
pub(crate) fn reserve(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
) -> Result<(), ConnectorError> {
    backend
        .put(RESERVED_KEY, id.to_le_bytes().to_vec())
        .map_err(state_err)
}

/// Record a checkpoint that may have entered an external prepare phase.
pub(crate) fn mark_prepare_intent(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
) -> Result<(), ConnectorError> {
    backend
        .put(
            format!("{}/prepare", checkpoint_prefix(id)).as_bytes(),
            PREPARE_MARKER.to_vec(),
        )
        .map_err(state_err)
}

/// Record a complete body whose prepared sinks may now enter commit.
pub(crate) fn mark_commit_intent(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
) -> Result<(), ConnectorError> {
    backend
        .put(
            format!("{}/commit", checkpoint_prefix(id)).as_bytes(),
            COMMIT_MARKER.to_vec(),
        )
        .map_err(state_err)
}

pub(crate) fn clear_prepare_intent(
    backend: &mut (dyn StateBackend + Send),
    id: u64,
) -> Result<(), StateError> {
    backend.delete(format!("{}/prepare", checkpoint_prefix(id)).as_bytes())
}

/// Decode an 8-byte little-endian checkpoint id.
pub(crate) fn parse_id(bytes: &[u8]) -> Result<u64, ConnectorError> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| ConnectorError::Corruption("checkpoint id is not 8 bytes".into()))?;
    Ok(u64::from_le_bytes(array))
}

/// Error for an unknown or incomplete checkpoint.
pub(crate) fn invalid(id: u64) -> ConnectorError {
    ConnectorError::Missing(format!("checkpoint {id} is missing or not valid"))
}

/// Error for an exhausted, non-reusable checkpoint id space.
pub(crate) fn id_exhausted() -> ConnectorError {
    ConnectorError::Infrastructure("checkpoint id space is exhausted".into())
}

/// Map a hotlap facade error onto the connector error type.
pub(crate) fn hotlap_err(error: hotlap::HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(error.0)
}

/// Map an engine decode error onto the connector error type.
///
/// [`EngineError::Unsupported`] (an unknown engine frame or snapshot version)
/// stays `Unsupported`, so recovery treats an incompatible snapshot as fatal
/// instead of falling back or starting clean. Any other failure at the decode
/// boundary is current-format corruption, which recovery may tolerate.
pub(crate) fn decode_err(error: EngineError) -> ConnectorError {
    match error {
        EngineError::Unsupported(message) => ConnectorError::Unsupported(message),
        error => ConnectorError::Corruption(error.to_string()),
    }
}

/// Prove an engine body is fully reconstructable before selection or promotion.
pub(crate) fn validate_engine_snapshot(snapshot: &EngineSnapshot) -> Result<(), ConnectorError> {
    match EngineCore::new().validate_snapshot(snapshot) {
        Ok(()) => Ok(()),
        Err(EngineError::Unsupported(message)) => Err(ConnectorError::Unsupported(message)),
        Err(error) => Err(ConnectorError::Corruption(error.to_string())),
    }
}

/// Map an engine encode error onto the connector error type.
///
/// Encoding happens while producing a checkpoint from live state, so a failure
/// is an internal or unsupported-configuration error, never persisted
/// corruption; it must not be classified as a decodable body.
pub(crate) fn encode_err(error: EngineError) -> ConnectorError {
    match error {
        EngineError::Unsupported(message) => ConnectorError::Unsupported(message),
        error => ConnectorError::Infrastructure(error.to_string()),
    }
}

/// Map a state backend error onto the connector error type.
pub(crate) fn state_err(error: StateError) -> ConnectorError {
    ConnectorError::Storage(error)
}
