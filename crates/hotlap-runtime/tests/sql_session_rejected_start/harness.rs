//! Fixtures for the rejected-start durability test.

#[path = "../common/backend.rs"]
mod backend;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::{StreamExt, stream};
use hotlap::{Hotlap, InputId};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::sources::{InputSource, Sources};
use hotlap_runtime::{SinkFactory, SourceFactory};
use hotlap_sql::SqlError;

pub use backend::SharedBackend;

fn session_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// Counts reads so a refused start can prove it never opened the source.
struct CountedSource {
    reads: Arc<AtomicU32>,
}

impl Source for CountedSource {
    fn schema(&self) -> SchemaRef {
        session_schema()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

pub struct CountedFactory {
    pub reads: Arc<AtomicU32>,
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
        }))
    }
}

/// A sink whose retraction capability can be flipped between attempts.
struct ToggleSink {
    accepts: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Sink for ToggleSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
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

pub struct ToggleFactory {
    pub creates: Arc<AtomicU32>,
    pub accepts: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl SinkFactory for ToggleFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(ToggleSink {
            accepts: Arc::clone(&self.accepts),
        }))
    }

    fn accepts_retractions(&self, _options: &BTreeMap<String, String>) -> bool {
        self.accepts.load(Ordering::SeqCst)
    }
}

/// A checkpoint whose saved schema cannot match the session source, so a
/// durable start fails after consuming the config and must latch.
pub async fn mismatched_backend() -> SharedBackend {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "src".into(),
        source: Arc::new(SeedSource { schema }),
        watermark: None,
    }])
    .unwrap();
    let mut engine = Hotlap::open_with(Box::new(EngineCore::new()));
    engine.register_input_with_id("src", InputId(0)).unwrap();
    checkpointer.take(&engine, &sources).await.unwrap();
    backend
}

/// Single-column source used only to seed a mismatched checkpoint.
struct SeedSource {
    schema: SchemaRef,
}

impl Source for SeedSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}
