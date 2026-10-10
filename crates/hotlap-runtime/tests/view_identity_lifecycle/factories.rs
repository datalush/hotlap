//! Counting source and sink factories for the late-view lifecycle test.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink};
use hotlap_connectors::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};
use hotlap_runtime::{SinkFactory, SourceFactory};
use hotlap_sql::SqlError;

#[path = "../common/recovery/resumable.rs"]
mod resumable;

use resumable::{Dataset, ResumableSource};

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![2]])
        .with_retention(0)
        .with_physical_identity("test/view-identity-lifecycle/log")
}

/// A resumable source that counts reads and advertises column 0 as event time.
struct CountingSource {
    inner: ResumableSource,
    reads: Arc<AtomicU32>,
}

impl Source for CountingSource {
    fn physical_identity(&self) -> Option<String> {
        self.inner.physical_identity()
    }

    fn schema(&self) -> SchemaRef {
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
        Some(0)
    }
    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        self.inner.resume(state)
    }
}

/// Builds one counting source over the fixed log.
pub struct CountingFactory {
    pub reads: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl SourceFactory for CountingFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        Ok(Box::new(CountingSource {
            inner: ResumableSource::new(log()),
            reads: Arc::clone(&self.reads),
        }))
    }
}

/// A sink that drains its changelog.
struct NullSink;

#[async_trait::async_trait]
impl Sink for NullSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while changes.next().await.is_some() {}
        Ok(())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Builds sinks and counts every create.
pub struct CountingSinkFactory {
    pub creates: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl SinkFactory for CountingSinkFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(NullSink))
    }
}
