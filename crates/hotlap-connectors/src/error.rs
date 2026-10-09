//! Connector error type.

use hotlap::state::StateError;

/// Errors raised by connector sources and sinks.
#[derive(Debug)]
pub enum ConnectorError {
    /// A Fluss client failure.
    Fluss(String),
    /// An Arrow conversion failure.
    Arrow(String),
    /// The caller asked for something outside the supported subset.
    ///
    /// For a persisted checkpoint this is a fatal incompatibility (foreign
    /// magic, unknown version): recovery must not tolerate it or fall back.
    Unsupported(String),
    /// A current-format checkpoint body failed to decode.
    ///
    /// This is the only persisted-state failure that recovery tolerates by
    /// falling back to an older checkpoint, because the bytes are structurally
    /// the current format but damaged.
    Corruption(String),
    /// A checkpoint or one of its parts is absent, or a valid marker is missing.
    ///
    /// `read` reports this instead of a decode failure so recovery can skip an
    /// unfinished checkpoint without treating absence as corruption.
    Missing(String),
    /// An operational store failure while reading or writing state.
    ///
    /// The underlying [`StateError`] (and its I/O source) is retained, so this
    /// is never a decode result and recovery must propagate it, never tolerate
    /// it as corruption or absence.
    Storage(StateError),
    /// The runtime failed (worker/channel stopped, etc.).
    Infrastructure(String),
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fluss(m) => write!(f, "fluss error: {m}"),
            Self::Arrow(m) => write!(f, "arrow error: {m}"),
            Self::Unsupported(m) => write!(f, "unsupported: {m}"),
            Self::Corruption(m) => write!(f, "corrupt checkpoint: {m}"),
            Self::Missing(m) => write!(f, "missing checkpoint: {m}"),
            Self::Storage(e) => write!(f, "storage: {e}"),
            Self::Infrastructure(m) => write!(f, "infrastructure: {m}"),
        }
    }
}

impl std::error::Error for ConnectorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}
