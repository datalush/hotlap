use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::stream;
use hotlap_connectors::runtime::handle::EngineHandle;
use hotlap_connectors::runtime::pipeline::Pipeline;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};

struct PendingSource {
    schema: SchemaRef,
}

impl Source for PendingSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, hotlap_connectors::ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, hotlap_connectors::ConnectorError> {
        // Never yields: keeps the engine alive while we exercise the handle.
        Ok(Box::pin(stream::pending()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

#[test]
fn start_snapshot_shutdown() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let handle = EngineHandle::start(Pipeline {
        input: "in".into(),
        source: Box::new(PendingSource { schema }),
        watermark: None,
        views: vec![],
    })
    .unwrap();
    // No view registered: snapshot of an unknown view errors, but the handle must respond.
    assert!(handle.snapshot("nope").is_err());
    handle.shutdown().unwrap();
}
