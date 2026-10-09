//! A source wrapper that counts `resume` calls during recovery.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};

/// Delegates to an inner source and records how often `resume` ran, so a test
/// can prove recovery stopped before reopening any split.
pub struct SpySource {
    inner: Arc<dyn Source>,
    resumed: Arc<AtomicU32>,
}

impl SpySource {
    /// Wrap `inner` with a zeroed resume counter.
    pub fn new(inner: Arc<dyn Source>) -> Self {
        Self {
            inner,
            resumed: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Number of `resume` calls seen so far.
    pub fn resumed(&self) -> u32 {
        self.resumed.load(Ordering::SeqCst)
    }
}

impl Source for SpySource {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        self.inner.splits()
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        self.inner.read(split)
    }

    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.inner.commit(split, offset)
    }

    fn state(&self) -> SourceState {
        self.inner.state()
    }

    fn event_time_column(&self) -> Option<usize> {
        self.inner.event_time_column()
    }

    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        self.resumed.fetch_add(1, Ordering::SeqCst);
        self.inner.resume(state)
    }
}
