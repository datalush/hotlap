use std::sync::{Arc, Mutex, mpsc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Offset, Source, SourceBatch, SourceState, SourceStream, Split};
use tokio::sync::mpsc::{self as tokio_mpsc, UnboundedReceiver, UnboundedSender};

pub type BatchSender = UnboundedSender<Result<SourceBatch, ConnectorError>>;

pub struct GatedSource {
    schema: SchemaRef,
    receiver: Mutex<Option<UnboundedReceiver<Result<SourceBatch, ConnectorError>>>>,
    commits: Arc<Mutex<Vec<(i32, Offset)>>>,
    commit_signal: mpsc::Sender<(i32, Offset)>,
}

impl GatedSource {
    pub fn new(schema: SchemaRef) -> (Arc<Self>, BatchSender, mpsc::Receiver<(i32, Offset)>) {
        let (sender, receiver) = tokio_mpsc::unbounded_channel();
        let (commit_signal, commit_receiver) = mpsc::channel();
        (
            Arc::new(Self {
                schema,
                receiver: Mutex::new(Some(receiver)),
                commits: Arc::new(Mutex::new(Vec::new())),
                commit_signal,
            }),
            sender,
            commit_receiver,
        )
    }

    pub fn commits(&self) -> Vec<(i32, Offset)> {
        self.commits.lock().unwrap().clone()
    }
}

impl Source for GatedSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }

    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| ConnectorError::Infrastructure("source already opened".into()))?;
        Ok(Box::pin(futures::stream::unfold(
            receiver,
            |mut receiver| async { receiver.recv().await.map(|item| (item, receiver)) },
        )))
    }

    fn commit(&self, split: i32, offset: Offset) -> Result<(), ConnectorError> {
        self.commits.lock().unwrap().push((split, offset));
        let _ = self.commit_signal.send((split, offset));
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
