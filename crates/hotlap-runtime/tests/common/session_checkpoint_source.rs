//! Counted resumable source fixture for public session recovery tests.

#[path = "recovery/resumable.rs"]
mod resumable;
#[path = "spy.rs"]
mod spy;
#[path = "watermarked_spy.rs"]
mod watermarked_spy;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};
use hotlap_runtime::SourceFactory;
use hotlap_sql::error::SqlError;
use resumable::{Dataset, ResumableSource};
use spy::SpySource;
use watermarked_spy::WatermarkedSpy;

pub use resumable::Dataset as ProbeDataset;
pub use spy::SpySource as SourceProbe;

pub struct ProbeFactory {
    pub dataset: Dataset,
    pub spies: Arc<Mutex<Vec<Arc<SpySource>>>>,
    pub reads: Arc<AtomicU32>,
    pub source_gate: Option<Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>>,
    pub source_ack: Option<std::sync::mpsc::Sender<()>>,
    pub source_read_ack: Option<std::sync::mpsc::Sender<()>>,
}

#[async_trait::async_trait]
impl SourceFactory for ProbeFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        let spy = SpySource::new(Arc::new(ResumableSource::new(
            self.dataset.clone().with_retention(0),
        )));
        self.spies.lock().unwrap().push(Arc::new(spy.clone()));
        let source: Box<dyn Source> = Box::new(ReadCounter {
            inner: WatermarkedSpy(spy),
            reads: Arc::clone(&self.reads),
        });
        if self.source_gate.is_some() || self.source_ack.is_some() || self.source_read_ack.is_some()
        {
            Ok(Box::new(GatedSource {
                inner: source,
                gate: self.source_gate.clone(),
                source_ack: self.source_ack.clone(),
                source_read_ack: self.source_read_ack.clone(),
            }))
        } else {
            Ok(source)
        }
    }
}

struct GatedSource {
    inner: Box<dyn Source>,
    gate: Option<Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>>,
    source_ack: Option<std::sync::mpsc::Sender<()>>,
    source_read_ack: Option<std::sync::mpsc::Sender<()>>,
}

impl Source for GatedSource {
    fn physical_identity(&self) -> Option<String> {
        self.inner.physical_identity()
    }
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        self.inner.splits()
    }
    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        let source = self.inner.read(split)?;
        if let Some(signal) = &self.source_read_ack {
            let _ = signal.send(());
        }
        let receiver = self
            .gate
            .as_ref()
            .and_then(|gate| gate.lock().unwrap().take());
        match receiver {
            Some(receiver) => Ok(Box::pin(
                futures::stream::once(async move {
                    let _ = receiver.await;
                    source
                })
                .flatten(),
            )),
            None => Ok(source),
        }
    }
    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.inner.commit(split, offset)?;
        if let Some(signal) = &self.source_ack {
            let _ = signal.send(());
        }
        Ok(())
    }
    fn state(&self) -> SourceState {
        self.inner.state()
    }
    fn event_time_column(&self) -> Option<usize> {
        self.inner.event_time_column()
    }
    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        self.inner.resume(state)
    }
}

struct ReadCounter<S> {
    inner: WatermarkedSpy<S>,
    reads: Arc<AtomicU32>,
}

impl<S: Source> Source for ReadCounter<S> {
    fn physical_identity(&self) -> Option<String> {
        self.inner.physical_identity()
    }
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        self.inner.splits()
    }
    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read(split)
    }
    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.inner.commit(split, offset)
    }
    fn state(&self) -> SourceState {
        self.inner.state()
    }
    fn event_time_column(&self) -> Option<usize> {
        self.inner.event_time_column()
    }
    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        self.inner.resume(state)
    }
}
