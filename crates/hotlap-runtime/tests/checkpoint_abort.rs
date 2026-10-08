//! A failed checkpoint commit must clear the durable marker before it aborts
//! the sinks, so recovery can never promote a rolled-back checkpoint.

#[path = "common/backend.rs"]
mod backend;

use std::sync::{Arc, Mutex};

use arrow::datatypes::{Field, Schema, SchemaRef};
use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::Checkpointer;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

use backend::SharedBackend;

/// Records whether the commit marker was still present when `abort` ran.
struct AbortProbe {
    backend: SharedBackend,
    id: u64,
    marker_at_abort: Arc<Mutex<bool>>,
}

#[async_trait::async_trait]
impl Sink for AbortProbe {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Err(ConnectorError::Infrastructure("commit failed".into()))
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        let marker = self
            .backend
            .get(format!("checkpoint/{}/commit", self.id).as_bytes())
            .unwrap()
            .is_some();
        *self.marker_at_abort.lock().unwrap() = marker;
        Ok(())
    }
}

/// A source with no splits, enough for the barrier.
struct EmptySource;

impl Source for EmptySource {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(Vec::<Field>::new()))
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(Vec::new())
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(futures::stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

#[tokio::test]
async fn a_failed_commit_clears_the_marker_before_aborting() {
    let backend = SharedBackend::default();
    let marker_at_abort = Arc::new(Mutex::new(true));
    let probe = Arc::new(AbortProbe {
        backend: backend.clone(),
        id: 1,
        marker_at_abort: Arc::clone(&marker_at_abort),
    });
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), 3)
        .with_sinks(vec![SinkSync::sink_only(SharedSink::new(probe))]);
    let engine = hotlap::Hotlap::open_with(Box::new(EngineCore::new()));

    let result = checkpointer.take(&engine, &EmptySource).await;

    assert!(result.is_err(), "the failed commit must surface the error");
    assert!(
        !*marker_at_abort.lock().unwrap(),
        "the marker must already be gone when the sink is aborted"
    );
    assert_eq!(backend.get(b"checkpoint/1/commit").unwrap(), None);
    assert_eq!(backend.get(b"checkpoint/1/valid").unwrap(), None);
}
