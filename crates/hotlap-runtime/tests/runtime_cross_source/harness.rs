//! Controlled two-source fixtures for the cross-source runtime tests.
//!
//! This module is local to `runtime_cross_source`: it carries the EOF observer
//! the normal-end test needs, without adding shared-fixture API that other
//! test crates include but do not use.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap::InputId;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use hotlap_runtime::runtime::sources::{InputSource, Sources};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

type BatchItem = Result<SourceBatch, ConnectorError>;

/// The producer half the test uses to feed one split of a controlled source.
pub type BatchSender = UnboundedSender<BatchItem>;

/// A source whose batches are injected through one channel per split.
pub struct ControlledSource {
    physical_identity: String,
    schema: SchemaRef,
    splits: Vec<Split>,
    receivers: Mutex<Vec<Option<UnboundedReceiver<BatchItem>>>>,
    fail_read: bool,
    exhausted: Arc<AtomicBool>,
    commits: Arc<Mutex<Vec<(SplitId, Offset)>>>,
    applied: Arc<Mutex<SourceState>>,
}

impl ControlledSource {
    /// Build a source over `splits`; returns it with one sender per split.
    pub fn new(
        schema: SchemaRef,
        splits: Vec<Split>,
        physical_identity: impl Into<String>,
    ) -> (Arc<Self>, Vec<BatchSender>) {
        let mut senders = Vec::with_capacity(splits.len());
        let mut receivers = Vec::with_capacity(splits.len());
        for _ in &splits {
            let (sender, receiver) = mpsc::unbounded_channel();
            senders.push(sender);
            receivers.push(Some(receiver));
        }
        let source = Arc::new(Self {
            physical_identity: physical_identity.into(),
            schema,
            splits,
            receivers: Mutex::new(receivers),
            fail_read: false,
            exhausted: Arc::new(AtomicBool::new(false)),
            commits: Arc::new(Mutex::new(Vec::new())),
            applied: Arc::new(Mutex::new(SourceState::default())),
        });
        (source, senders)
    }

    /// Build a source whose `read` always fails, for open-failure tests.
    pub fn failing_read(schema: SchemaRef, physical_identity: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            physical_identity: physical_identity.into(),
            schema,
            splits: vec![Split { id: 0, start: 0 }],
            receivers: Mutex::new(vec![None]),
            fail_read: true,
            exhausted: Arc::new(AtomicBool::new(false)),
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

    /// Whether the read stream has ended (all senders dropped and drained).
    pub fn is_exhausted(&self) -> bool {
        self.exhausted.load(Ordering::SeqCst)
    }
}

impl Source for ControlledSource {
    fn physical_identity(&self) -> Option<String> {
        Some(self.physical_identity.clone())
    }

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
        let exhausted = Arc::clone(&self.exhausted);
        let stream = futures::stream::unfold(receiver, move |mut rx| {
            let exhausted = Arc::clone(&exhausted);
            async move {
                match rx.recv().await {
                    Some(item) => Some((item, rx)),
                    None => {
                        exhausted.store(true, Ordering::SeqCst);
                        None
                    }
                }
            }
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

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

pub fn split(id: i32) -> Split {
    Split { id, start: 0 }
}

pub fn batch_on(split: i32, key: i64) -> SourceBatch {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![key]));
    let batch = RecordBatch::try_new(schema(), vec![array]).unwrap();
    SourceBatch {
        batch,
        base_offset: 0,
        next_offset: 1,
        split,
    }
}

/// Runs `case` on a worker thread and panics if it does not finish in time, so
/// a deadlocked engine command fails the test instead of hanging the suite.
pub fn bounded(timeout: Duration, case: impl FnOnce() + Send + 'static) {
    let (done, wait) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(case));
        let _ = done.send(result);
    });
    match wait.recv_timeout(timeout) {
        Ok(Ok(())) => {}
        Ok(Err(payload)) => std::panic::resume_unwind(payload),
        Err(_) => panic!("case did not finish within {timeout:?} (possible deadlock)"),
    }
}

/// Both sources as inputs a (id 0) and b (id 1).
pub fn sources(a: Arc<ControlledSource>, b: Arc<ControlledSource>) -> Sources {
    Sources::new(vec![
        InputSource {
            id: InputId(0),
            name: "a".into(),
            source: a,
            watermark: None,
        },
        InputSource {
            id: InputId(1),
            name: "b".into(),
            source: b,
            watermark: None,
        },
    ])
    .unwrap()
}
