//! Controlled [`Source`] fixture driven by an external batch channel.
//!
//! The test pushes batches (or closes the channel) to sequence a stream without
//! sleeping: until a batch arrives, [`Source::read`] stays pending.

use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

/// The producer half the test uses to feed one controlled source.
pub type BatchSender = UnboundedSender<Result<SourceBatch, ConnectorError>>;

/// A source whose batches are injected through a channel.
pub struct ControlledSource {
    schema: SchemaRef,
    splits: Vec<Split>,
    receiver: Mutex<Option<UnboundedReceiver<Result<SourceBatch, ConnectorError>>>>,
    fail_read: bool,
    commits: Arc<Mutex<Vec<(SplitId, Offset)>>>,
    applied: Arc<Mutex<SourceState>>,
}

impl ControlledSource {
    /// Build a source over `splits` and return it with its batch sender.
    pub fn new(schema: SchemaRef, splits: Vec<Split>) -> (Arc<Self>, BatchSender) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let source = Arc::new(Self {
            schema,
            splits,
            receiver: Mutex::new(Some(receiver)),
            fail_read: false,
            commits: Arc::new(Mutex::new(Vec::new())),
            applied: Arc::new(Mutex::new(SourceState::default())),
        });
        (source, sender)
    }

    /// Build a source whose `read` always fails, for open-failure tests.
    pub fn failing_read(schema: SchemaRef) -> Arc<Self> {
        Arc::new(Self {
            schema,
            splits: vec![Split { id: 0, start: 0 }],
            receiver: Mutex::new(None),
            fail_read: true,
            commits: Arc::new(Mutex::new(Vec::new())),
            applied: Arc::new(Mutex::new(SourceState::default())),
        })
    }

    /// Applied `(split, next_offset)` acks, in commit order.
    pub fn commits(&self) -> Vec<(SplitId, Offset)> {
        self.commits.lock().unwrap().clone()
    }

    /// The applied state the runtime would persist.
    pub fn applied(&self) -> SourceState {
        self.applied.lock().unwrap().clone()
    }
}

impl Source for ControlledSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(self.splits.clone())
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        if self.fail_read {
            return Err(ConnectorError::Infrastructure(format!(
                "read failed for split {}",
                split.id
            )));
        }
        let receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| ConnectorError::Infrastructure("split already opened".into()))?;
        let stream = futures::stream::unfold(receiver, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(Box::pin(stream))
    }

    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.commits.lock().unwrap().push((split, offset));
        self.applied.lock().unwrap().offsets.insert(split, offset);
        Ok(())
    }

    fn state(&self) -> SourceState {
        self.applied.lock().unwrap().clone()
    }

    fn event_time_column(&self) -> Option<usize> {
        None
    }
}
