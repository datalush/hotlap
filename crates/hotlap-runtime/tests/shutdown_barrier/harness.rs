//! Backend and source fixtures for the barrier-stall shutdown tests.

#[path = "harness/sinks.rs"]
mod sinks;
#[path = "harness/staged.rs"]
mod staged;

use std::sync::Arc;

use hotlap::state::{StateBackend, StateEntry, StateError};

#[path = "../common/shutdown.rs"]
mod common;
pub use common::*;
pub use sinks::*;
pub use staged::*;

/// A [`SharedBackend`] that signals a test when a watched key is written.
pub struct WatchedBackend {
    inner: SharedBackend,
    reserved: Option<Signal>,
    commit: Option<Signal>,
}

impl WatchedBackend {
    /// Wrap `inner`, firing `reserved`/`commit` when those keys are written.
    pub fn watch(inner: SharedBackend, reserved: Option<Signal>, commit: Option<Signal>) -> Self {
        Self {
            inner,
            reserved,
            commit,
        }
    }
}
impl StateBackend for WatchedBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        self.inner.get(key)
    }
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        if key == b"checkpoint/reserved"
            && let Some(signal) = &self.reserved
        {
            signal.fire();
        }
        if key.ends_with(b"/commit")
            && let Some(signal) = &self.commit
        {
            signal.fire();
        }
        self.inner.put(key, value)
    }
    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        self.inner.scan(prefix)
    }
    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        self.inner.list(prefix)
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        self.inner.delete(key)
    }
}

/// A source of `CHANNEL_CAPACITY + 1` batches that fires `last` on the final one.
///
/// With one batch already parked in the sink, this fills the channel exactly, so
/// a following `Flush` send parks instead of being buffered.
pub fn fill_queue(last: Signal) -> Arc<dyn hotlap_connectors::source::Source> {
    use hotlap_runtime::runtime::sink::CHANNEL_CAPACITY;
    let values: Vec<i64> = (0..CHANNEL_CAPACITY as i64 + 1).collect();
    keys_with(&values, Some(last))
}
