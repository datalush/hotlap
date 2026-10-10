//! Watermark presence and value checks in checkpoint validation.
//!
//! A declared watermark must appear in the engine snapshot with the same lag
//! and event-time column; a missing or unexpected `WatermarkSpec` is rejected.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap::Hotlap;
use hotlap::InputId;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split, SplitId};
use hotlap_engine::{EngineCore, EngineSnapshot};
use hotlap_runtime::runtime::pipeline::Watermark;
use hotlap_runtime::runtime::source_checkpoint::SourcesCheckpoint;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

/// A minimal bounded source advertising an event-time column.
struct EventTimeSource {
    schema: SchemaRef,
    event_time_column: Option<usize>,
}

impl Source for EventTimeSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/source-checkpoint-watermark/orders".into())
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }

    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let stream = futures::stream::empty::<Result<SourceBatch, ConnectorError>>();
        Ok(Box::pin(stream))
    }

    fn state(&self) -> SourceState {
        SourceState::default()
    }

    fn event_time_column(&self) -> Option<usize> {
        self.event_time_column
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

fn input(source: Arc<EventTimeSource>, watermark: Option<Watermark>) -> InputSource {
    InputSource {
        id: InputId(0),
        name: "orders".to_string(),
        source,
        watermark,
    }
}

fn sources(watermark: Option<Watermark>) -> Sources {
    let source = Arc::new(EventTimeSource {
        schema: schema(),
        event_time_column: Some(1),
    });
    Sources::new(vec![input(source, watermark)]).unwrap()
}

/// A real engine snapshot, with a declared watermark when `spec` is given.
fn engine(spec: Option<(usize, i64)>) -> EngineSnapshot {
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    hotlap.register_input_with_id("orders", InputId(0)).unwrap();
    if let Some((time_col, lag)) = spec {
        hotlap.declare_watermark("orders", time_col, lag).unwrap();
    }
    hotlap.declare_splits("orders", &[0 as SplitId]).unwrap();
    hotlap.checkpoint().unwrap()
}

#[test]
fn matching_watermark_validates() {
    let sources = sources(Some(Watermark { lag: 5 }));
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    checkpoint
        .validate(&sources, &engine(Some((1, 5))))
        .unwrap();
}

#[test]
fn missing_engine_watermark_does_not_validate() {
    let sources = sources(Some(Watermark { lag: 5 }));
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    assert!(checkpoint.validate(&sources, &engine(None)).is_err());
}

#[test]
fn unexpected_engine_watermark_does_not_validate() {
    let sources = sources(None);
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    assert!(
        checkpoint
            .validate(&sources, &engine(Some((1, 5))))
            .is_err()
    );
}

#[test]
fn changed_watermark_values_do_not_validate() {
    let sources = sources(Some(Watermark { lag: 5 }));
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    assert!(
        checkpoint
            .validate(&sources, &engine(Some((1, 7))))
            .is_err()
    );
    assert!(
        checkpoint
            .validate(&sources, &engine(Some((0, 5))))
            .is_err()
    );
}
