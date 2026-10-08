use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap::Hotlap;
use hotlap_engine::EngineCore;

use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_runtime::runtime::pipeline::{self, Pipeline, Watermark};

struct FakeSource {
    schema: SchemaRef,
    event_time: Option<usize>,
}

impl Source for FakeSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, hotlap_connectors::ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, hotlap_connectors::ConnectorError> {
        Ok(Box::pin(futures::stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        self.event_time
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

#[tokio::test]
async fn watermark_uses_the_source_event_time_column() {
    let pipeline = Pipeline {
        input: "in".into(),
        source: Box::new(FakeSource {
            schema: schema(),
            event_time: Some(1),
        }),
        watermark: Some(Watermark { lag: 0 }),
        views: vec![],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
}

#[tokio::test]
async fn watermark_without_source_event_time_errors() {
    let pipeline = Pipeline {
        input: "in".into(),
        source: Box::new(FakeSource {
            schema: schema(),
            event_time: None,
        }),
        watermark: Some(Watermark { lag: 0 }),
        views: vec![],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    let error = pipeline::setup(&mut hotlap, &pipeline).unwrap_err();
    assert!(matches!(
        error,
        hotlap_connectors::ConnectorError::Unsupported(_)
    ));
}
