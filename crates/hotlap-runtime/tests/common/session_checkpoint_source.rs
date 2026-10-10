//! Counted resumable source fixture for public session recovery tests.

#[path = "recovery/resumable.rs"]
mod resumable;
#[path = "spy.rs"]
mod spy;
#[path = "watermarked_spy.rs"]
mod watermarked_spy;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};
use hotlap_runtime::SourceFactory;
use hotlap_sql::error::SqlError;
use resumable::{Dataset, ResumableSource};
use spy::SpySource;
use watermarked_spy::WatermarkedSpy;

pub use resumable::Dataset as ProbeDataset;
pub use spy::SpySource as SourceProbe;

pub struct ProbeFactory {
    pub dataset: Dataset,
    pub spies: Arc<Mutex<Vec<Arc<SpySource>>>>,
    pub reads: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl SourceFactory for ProbeFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        let spy = SpySource::new(Arc::new(ResumableSource::new(
            self.dataset.clone().with_retention(0),
        )));
        self.spies.lock().unwrap().push(Arc::new(spy.clone()));
        Ok(Box::new(ReadCounter {
            inner: WatermarkedSpy(spy),
            reads: Arc::clone(&self.reads),
        }))
    }
}

struct ReadCounter<S> {
    inner: WatermarkedSpy<S>,
    reads: Arc<AtomicU32>,
}

impl<S: Source> Source for ReadCounter<S> {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        self.inner.splits()
    }
    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
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
        self.inner.resume(state)
    }
}
