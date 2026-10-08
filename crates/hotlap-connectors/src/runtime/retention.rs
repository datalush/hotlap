//! Checkpoint retention: keep the newest N checkpoints and delete the rest.
//!
//! Deletion only touches `checkpoint/<id>/...` keys, never the `latest`
//! pointer, and it is idempotent, so a partially pruned store can be pruned
//! again safely.

use hotlap::state::{StateBackend, StateError};

/// Namespace holding every checkpoint id.
const PREFIX: &[u8] = b"checkpoint/";

/// Delete every checkpoint except the `retain` newest.
///
/// `retain` is clamped to at least one, so the checkpoint `latest` points at
/// is never removed.
pub(crate) fn prune(
    backend: &mut (dyn StateBackend + Send),
    retain: usize,
) -> Result<(), StateError> {
    let mut ids = checkpoint_ids(backend)?;
    ids.sort_unstable();
    ids.dedup();
    let keep = retain.max(1);
    if ids.len() <= keep {
        return Ok(());
    }
    for id in &ids[..ids.len() - keep] {
        remove(backend, *id)?;
    }
    Ok(())
}

/// Every id that appears under the checkpoint namespace.
fn checkpoint_ids(backend: &dyn StateBackend) -> Result<Vec<u64>, StateError> {
    Ok(backend
        .list(PREFIX)?
        .iter()
        .filter_map(|key| checkpoint_id(key))
        .collect())
}

/// Delete every key belonging to checkpoint `id`.
fn remove(backend: &mut (dyn StateBackend + Send), id: u64) -> Result<(), StateError> {
    let prefix = format!("checkpoint/{id}/");
    for key in backend.list(prefix.as_bytes())? {
        backend.delete(&key)?;
    }
    Ok(())
}

/// Parse the id out of `checkpoint/<id>/...`; `checkpoint/latest` is `None`.
fn checkpoint_id(key: &[u8]) -> Option<u64> {
    let rest = key.strip_prefix(PREFIX)?;
    let segment = rest.split(|byte| *byte == b'/').next()?;
    std::str::from_utf8(segment).ok()?.parse().ok()
}
