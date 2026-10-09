//! Explicit consistency state of a [`Checkpointer`](super::Checkpointer).
//!
//! The engine and source offsets are not rolled back with the sinks, so an
//! attempt that failed after the sinks were prepared leaves the runtime
//! inconsistent. A restart resolves the durable evidence; until then no new
//! checkpoint may be started on the same runtime.

use crate::runtime::checkpoint::Checkpointer;
use hotlap_connectors::error::ConnectorError;

/// Whether a checkpointer may start a new attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointState {
    /// A new attempt may start.
    Ready,
    /// An attempt failed after the sinks were prepared and, when it reached the
    /// commit path, wrote no durable evidence that could be promoted.
    Failed,
    /// Durable prepare or commit evidence remains unresolved, so a restart must
    /// reject, re-drive, or discard it before continuing.
    CommitUncertain,
}

impl Checkpointer {
    /// Whether this checkpointer may start a new attempt.
    pub fn state(&self) -> CheckpointState {
        self.state
    }

    /// Reject a new attempt once a previous one left the runtime inconsistent.
    pub(crate) fn block_if_failed(&self) -> Result<(), ConnectorError> {
        if self.state == CheckpointState::Ready {
            return Ok(());
        }
        let detail = self.failure.as_deref().unwrap_or("no detail recorded");
        Err(ConnectorError::Infrastructure(format!(
            "checkpoint state is inconsistent after a failed attempt \
             ({detail}); recovery is required"
        )))
    }

    /// Record that an attempt failed in `state`, keeping a diagnostic reason.
    ///
    /// The caller still returns the original typed error; this only blocks
    /// later attempts on the same runtime.
    pub(crate) fn fail(&mut self, state: CheckpointState, error: &ConnectorError) {
        self.state = state;
        self.failure = Some(error.to_string());
    }
}
