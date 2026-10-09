//! Fixtures for the rejected-start durability test.

#[path = "../common/backend.rs"]
mod backend;
#[path = "factories.rs"]
mod factories;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::stream;
use hotlap::{Hotlap, InputId};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

pub use backend::SharedBackend;
pub use factories::{CountedFactory, ToggleFactory};

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
