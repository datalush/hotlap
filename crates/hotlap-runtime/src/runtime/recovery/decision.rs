//! The outcome of inspecting a store for an interrupted commit.

use crate::runtime::checkpoint::Checkpoint;

/// How startup should recover, including a checkpoint interrupted mid-commit.
///
/// An interrupted commit is one whose durable `commit` marker is present but
/// whose `valid` marker is not: the body is complete and the sinks may or may
/// not have committed before the process stopped.
#[derive(Debug)]
pub enum RecoveryDecision {
    /// No usable checkpoint: start clean.
    Clean,
    /// Resume from this already valid checkpoint.
    Resume(Checkpoint),
    /// Re-drive the interrupted commit for `Checkpoint` (every sink declares its
    /// commit re-drivable), publish it and resume from it without replay.
    Promote(Checkpoint),
    /// The interrupted checkpoint cannot be re-driven: discard `pending` and
    /// replay from `fallback` (the newest valid checkpoint, if any).
    Discard {
        /// Id of the interrupted checkpoint to discard.
        pending: u64,
        /// Newest valid checkpoint to replay from, if one exists.
        fallback: Option<Checkpoint>,
        /// Why the commit cannot be re-driven, for the warning signal.
        reason: &'static str,
    },
    /// The interrupted commit cannot be re-driven and cannot be safely
    /// replayed, because a transactional sink may already have committed.
    /// Recovery must refuse and preserve the marker and body for a manual
    /// decision instead of silently replaying a duplicate.
    Reject {
        /// Id of the interrupted checkpoint to preserve.
        pending: u64,
        /// Why replay is not safe, for the explicit error.
        reason: &'static str,
    },
}
