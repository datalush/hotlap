//! Fixtures for the rejected-start durability test.

#[path = "../common/backend.rs"]
mod backend;
#[path = "factories.rs"]
mod factories;
#[path = "identity_factory.rs"]
mod identity_factory;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::stream;
use hotlap::{Hotlap, InputId, Plan};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Watermark;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

pub use backend::SharedBackend;
pub use factories::{CountedFactory, ToggleFactory};
pub use identity_factory::IdentityFactory;

/// A valid checkpoint with the session source, watermark and named view.
pub async fn compatible_backend(plan: Plan) -> SharedBackend {
    let backend = SharedBackend::default();
    let sink = Arc::new(SeedSink);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(SharedSink::new(sink), "out".into(), "mv".into()),
        ]);
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "src".into(),
        source: Arc::new(SeedSource {
            schema: Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("_event_time", DataType::Int64, false),
            ])),
        }),
        watermark: Some(Watermark { lag: 1_000 }),
    }])
    .unwrap();
    let mut engine = Hotlap::open_with(Box::new(EngineCore::new()));
    engine.register_input_with_id("src", InputId(0)).unwrap();
    engine.declare_watermark("src", 1, 1_000).unwrap();
    engine.declare_splits("src", &[0]).unwrap();
    engine.create_view("mv", plan).unwrap();
    engine.tap_view("mv").unwrap();
    checkpointer.take(&engine, &sources).await.unwrap();
    backend
}

struct SeedSink;

#[async_trait::async_trait]
impl Sink for SeedSink {
    fn physical_identity(&self) -> Option<String> {
        Some("test/sql-session-rejected-start/output-store".into())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
    }
    fn accepts_retractions(&self) -> bool {
        false
    }
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Empty source used only to seed a compatible checkpoint.
struct SeedSource {
    schema: SchemaRef,
}

impl Source for SeedSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/sql-session-rejected-start/source-store".into())
    }

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
        SourceState {
            offsets: [(0, 7)].into(),
        }
    }
    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}
