//! Periodic checkpoint settings.

use std::time::Duration;

use hotlap::state::StateBackend;

/// How many checkpoints [`CheckpointConfig`] keeps by default.
pub const DEFAULT_RETAIN: usize = 3;

/// Periodic checkpoint settings for a running engine.
pub struct CheckpointConfig {
    /// Minimum time between periodic checkpoints.
    pub interval: Duration,
    /// Destination store, moved into the engine thread.
    pub backend: Box<dyn StateBackend + Send>,
    /// Number of newest checkpoints to keep; older ones are deleted.
    ///
    /// Clamped to at least one, so the `latest` checkpoint always survives.
    pub retain: usize,
}
