//! Minimal channel-driven [`Source`] for the SQL cross-source session tests.
//!
//! Unlike the runtime harness, a SQL session reads one split per source and the
//! declared watermark needs the source to advertise its event-time column, so
//! this fixture carries an event-time index and records its acks.

use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

/// The producer half a test uses to feed the source's single split.
pub type BatchSender = UnboundedSender<Result<SourceBatch, ConnectorError>>;

/// A one-split source fed by an in-memory channel.
pub struct SqlSource {
    schema: SchemaRef,
    event_time: usize,
    receiver: Mutex<Option<UnboundedReceiver<Result<SourceBatch, ConnectorError>>>>,
    commits: Arc<Mutex<Vec<(SplitId, Offset)>>>,
}

impl SqlSource {
    /// Build the source and the sender for its split 0.
    pub fn new(schema: SchemaRef, event_time: usize) -> (Arc<Self>, Vec<BatchSender>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let source = Arc::new(Self {
            schema,
            event_time,
            receiver: Mutex::new(Some(receiver)),
            commits: Arc::new(Mutex::new(Vec::new())),
        });
        (source, vec![sender])
    }

    /// Applied `(split, next_offset)` acks, in commit order.
    pub fn commits(&self) -> Vec<(SplitId, Offset)> {
        self.commits.lock().unwrap().clone()
    }
}

impl Source for SqlSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        if split.id != 0 {
            return Err(ConnectorError::Unsupported(format!(
                "unknown split {}",
                split.id
            )));
        }
        let receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| ConnectorError::Infrastructure("split 0 already opened".into()))?;
        let stream = futures::stream::unfold(receiver, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(Box::pin(stream))
    }

    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.commits.lock().unwrap().push((split, offset));
        Ok(())
    }

    fn state(&self) -> SourceState {
        SourceState::default()
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(self.event_time)
    }
}
