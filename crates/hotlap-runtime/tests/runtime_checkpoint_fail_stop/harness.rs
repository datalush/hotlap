//! A channel-driven source for the checkpoint fail-stop tests.
//!
//! Local to `runtime_checkpoint_fail_stop`, so no shared fixture is included
//! and left unused.

use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

/// One item a controlled source may yield.
type BatchItem = Result<SourceBatch, ConnectorError>;

/// The producer half the test uses to feed the controlled source.
pub type BatchSender = UnboundedSender<BatchItem>;

/// A minimal channel-driven source recording its acks.
pub struct ControlledB {
    schema: SchemaRef,
    splits: Vec<Split>,
    receivers: Mutex<Vec<Option<UnboundedReceiver<BatchItem>>>>,
    commits: Arc<Mutex<Vec<(SplitId, Offset)>>>,
}

impl ControlledB {
    pub fn new(schema: SchemaRef, splits: Vec<Split>) -> (Arc<Self>, Vec<BatchSender>) {
        let mut senders = Vec::with_capacity(splits.len());
        let mut receivers = Vec::with_capacity(splits.len());
        for _ in &splits {
            let (sender, receiver) = mpsc::unbounded_channel();
            senders.push(sender);
            receivers.push(Some(receiver));
        }
        let source = Arc::new(Self {
            schema,
            splits,
            receivers: Mutex::new(receivers),
            commits: Arc::new(Mutex::new(Vec::new())),
        });
        (source, senders)
    }

    pub fn commits(&self) -> Vec<(SplitId, Offset)> {
        self.commits.lock().unwrap().clone()
    }
}

impl Source for ControlledB {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(self.splits.clone())
    }
    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        let index = self
            .splits
            .iter()
            .position(|candidate| candidate.id == split.id)
            .ok_or_else(|| ConnectorError::Unsupported(format!("unknown split {}", split.id)))?;
        let receiver = self.receivers.lock().unwrap()[index]
            .take()
            .ok_or_else(|| {
                ConnectorError::Infrastructure(format!("split {} already opened", split.id))
            })?;
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
        None
    }
}

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}
