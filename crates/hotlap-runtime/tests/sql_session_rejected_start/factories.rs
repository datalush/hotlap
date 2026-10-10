//! Counting source and sink factories for the rejected-start tests.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::{StreamExt, stream};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_runtime::runtime::pipeline::SinkDescription;
use hotlap_runtime::{SinkFactory, SourceFactory};
use hotlap_sql::SqlError;

fn session_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// Counts reads so a refused start can prove it never opened the source.
struct CountedSource {
    reads: Arc<AtomicU32>,
    starts: Arc<Mutex<Vec<i64>>>,
}

impl Source for CountedSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/sql-session-rejected-start/source-store".into())
    }

    fn schema(&self) -> SchemaRef {
        session_schema()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.starts.lock().unwrap().push(split.start);
        Ok(Box::pin(stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

/// Builds a read-counting source for every name.
pub struct CountedFactory {
    pub reads: Arc<AtomicU32>,
    pub starts: Arc<Mutex<Vec<i64>>>,
}

#[async_trait::async_trait]
impl SourceFactory for CountedFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        Ok(Box::new(CountedSource {
            reads: Arc::clone(&self.reads),
            starts: Arc::clone(&self.starts),
        }))
    }
}

/// A sink whose retraction capability can be flipped between attempts.
struct ToggleSink {
    accepts: Arc<AtomicBool>,
    writes: Arc<AtomicU32>,
    physical_identity: String,
}

#[async_trait::async_trait]
impl Sink for ToggleSink {
    fn physical_identity(&self) -> Option<String> {
        Some(self.physical_identity.clone())
    }

    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        while changes.next().await.is_some() {}
        Ok(())
    }
    fn accepts_retractions(&self) -> bool {
        self.accepts.load(Ordering::SeqCst)
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Builds sinks and counts every `create`.
pub struct ToggleFactory {
    pub creates: Arc<AtomicU32>,
    pub accepts: Arc<AtomicBool>,
    pub writes: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl SinkFactory for ToggleFactory {
    async fn describe(
        &self,
        binding_name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
        view: &str,
    ) -> Result<Option<SinkDescription>, SqlError> {
        Ok(Some(SinkDescription {
            binding_name: binding_name.to_owned(),
            view: view.to_owned(),
            physical_identity: "test/sql-session-rejected-start/output-store".into(),
            capabilities: SinkCapabilities::AtLeastOnce,
            accepts_retractions: self.accepts.load(Ordering::SeqCst),
            commit_redriable: false,
        }))
    }

    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(ToggleSink {
            accepts: Arc::clone(&self.accepts),
            writes: Arc::clone(&self.writes),
            physical_identity: "test/sql-session-rejected-start/output-store".into(),
        }))
    }

    fn accepts_retractions(&self, _options: &BTreeMap<String, String>) -> bool {
        self.accepts.load(Ordering::SeqCst)
    }
}
