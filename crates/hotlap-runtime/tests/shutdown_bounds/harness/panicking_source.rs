//! A source that panics on its first poll.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};

use super::{Signal, schema};

pub struct PanickingSource {
    schema: arrow::datatypes::SchemaRef,
    signal: Signal,
}

impl PanickingSource {
    pub fn new(signal: Signal) -> Arc<Self> {
        Arc::new(Self {
            schema: schema(),
            signal,
        })
    }
}

struct PanicOnPoll {
    signal: Signal,
}

impl Stream for PanicOnPoll {
    type Item = Result<SourceBatch, ConnectorError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.signal.fire();
        panic!("engine source panicked");
    }
}

impl Source for PanickingSource {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(PanicOnPoll {
            signal: self.signal.clone(),
        }))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}
