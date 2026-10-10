//! A source failure revokes the EOF commit after a real sink write.

use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::{InputId, Plan};
use hotlap_connectors::ChangeStream;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

struct FailingSource(SchemaRef);

impl Source for FailingSource {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let batch = RecordBatch::try_new(
            self.0.clone(),
            vec![Arc::new(Int64Array::from(vec![7])) as ArrayRef],
        )
        .unwrap();
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(SourceBatch {
                batch,
                base_offset: 0,
                next_offset: 1,
                split: 0,
            }),
            Err(ConnectorError::Infrastructure("source exploded".into())),
        ])))
    }
    fn commit(&self, _split: SplitId, _offset: Offset) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

#[derive(Default)]
struct Remote {
    staged: Mutex<Vec<i64>>,
    committed: Mutex<Vec<i64>>,
}

struct StagingSink {
    remote: Arc<Remote>,
    written: mpsc::Sender<()>,
}

#[async_trait::async_trait]
impl Sink for StagingSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            let batch = item?;
            let keys = batch
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            self.remote
                .staged
                .lock()
                .unwrap()
                .extend((0..keys.len()).map(|row| keys.value(row)));
            let _ = self.written.send(());
        }
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }
    fn accepts_retractions(&self) -> bool {
        true
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        let staged = std::mem::take(&mut *self.remote.staged.lock().unwrap());
        self.remote.committed.lock().unwrap().extend(staged);
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        self.remote.staged.lock().unwrap().clear();
        Ok(())
    }
}

#[test]
fn source_failure_after_a_real_write_skips_eof_commit_and_fails_shutdown() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let source: Arc<dyn Source> = Arc::new(FailingSource(schema));
    let (written, received) = mpsc::channel();
    let remote = Arc::new(Remote::default());
    let handle = EngineHandle::start(Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source,
            watermark: None,
        }])
        .unwrap(),
        views: vec![("v".into(), Plan::Source(InputId(0)))],
        sinks: vec![SinkSpec {
            view: "v".into(),
            sink: Arc::new(StagingSink {
                remote: remote.clone(),
                written,
            }),
        }],
        checkpoint: None,
        retention: None,
    })
    .unwrap();

    received
        .recv_timeout(Duration::from_secs(5))
        .expect("writer did not receive the real output");
    let error = handle
        .shutdown()
        .expect_err("source failure must survive shutdown");
    assert!(error.to_string().contains("source exploded"), "got {error}");
    assert_eq!(*remote.staged.lock().unwrap(), vec![7]);
    assert!(remote.committed.lock().unwrap().is_empty());
}
