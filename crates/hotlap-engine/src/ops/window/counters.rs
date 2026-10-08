//! Counter and gauge accessors for [`TumbleCount`](super::TumbleCount).

use super::TumbleCount;

impl TumbleCount {
    /// Number of rows dropped as late since construction.
    pub fn late_dropped(&self) -> u64 {
        self.dropped_late
    }

    /// Number of deltas dropped because their window had already closed.
    pub fn late_closed_dropped(&self) -> u64 {
        self.dropped_closed
    }

    /// Number of tumbling windows still open (not yet emitted).
    pub fn open_windows(&self) -> u64 {
        self.windows.len() as u64
    }
}
