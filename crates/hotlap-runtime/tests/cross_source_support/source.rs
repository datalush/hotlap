//! Controlled [`Source`] fixture driven by per-split batch channels.
//!
//! The test pushes batches (or closes a channel) to sequence a stream without
//! sleeping: a split stays pending until its channel produces an item.

use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

type BatchItem = Result<SourceBatch, ConnectorError>;

/// The producer half the test uses to feed one split of a controlled source.
pub type BatchSender = UnboundedSender<BatchItem>;

/// A source whose batches are injected through one channel per split.
pub struct ControlledSource {
    schema: SchemaRef,
    splits: Vec<Split>,
    receivers: Mutex<Vec<Option<UnboundedReceiver<BatchItem>>>>,
    fail_read: bool,
    commits: Arc<Mutex<Vec<(SplitId, Offset)>>>,
    applied: Arc<Mutex<SourceState>>,
}

impl ControlledSource {
    /// Build a source over `splits`; returns it with one sender per split.
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
            fail_read: false,
            commits: Arc::new(Mutex::new(Vec::new())),
            applied: Arc::new(Mutex::new(SourceState::default())),
        });
        (source, senders)
    }

    /// Build a source whose `read` always fails, for open-failure tests.
    pub fn failing_read(schema: SchemaRef) -> Arc<Self> {
        Arc::new(Self {
            schema,
            splits: vec![Split { id: 0, start: 0 }],
            receivers: Mutex::new(vec![None]),
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
