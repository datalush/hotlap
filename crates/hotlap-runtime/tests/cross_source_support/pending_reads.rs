//! A pending-recovery source that records the `split.start` of every read.
//!
//! It wraps the resumable fixture, so a test can observe the offset recovery
//! actually opens — independently of the committed state the checkpoint seeds —
//! and reject a read whose start is past the end of the dataset.

use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};

use crate::resumable::{Dataset, ResumableSource};

/// `(split id, start offset)` of each read, in call order.
pub type Reads = Arc<Mutex<Vec<(i32, i64)>>>;

/// A resumable source that records read starts and rejects one past EOF.
pub struct ReadStartSource {
    inner: ResumableSource,
    reads: Reads,
    end: usize,
}

impl ReadStartSource {
    /// Wrap `dataset`, returning the source and the shared recording of reads.
    pub fn new(dataset: Dataset) -> (Arc<Self>, Reads) {
        let end = dataset.batches.len();
        let reads: Reads = Arc::new(Mutex::new(Vec::new()));
        let source = Arc::new(Self {
            inner: ResumableSource::new(dataset),
            reads: Arc::clone(&reads),
            end,
        });
        (source, reads)
    }
}

impl Source for ReadStartSource {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        self.inner.splits()
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        self.reads.lock().unwrap().push((split.id, split.start));
        if split.start < 0 || split.start as usize > self.end {
            return Err(ConnectorError::Unsupported(format!(
                "split {} cannot start past its end {}",
                split.id, self.end
            )));
        }
        self.inner.read(split)
    }

    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.inner.commit(split, offset)
    }

    fn state(&self) -> SourceState {
        self.inner.state()
    }

    fn event_time_column(&self) -> Option<usize> {
        None
    }

    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        self.inner.resume(state)
    }

    fn is_unbounded(&self) -> bool {
        self.inner.is_unbounded()
    }
}

/// The recorded read starts, as a plain vector.
pub fn read_starts(reads: &Reads) -> Vec<(i32, i64)> {
    reads.lock().unwrap().clone()
}
