//! A failed writer must revoke EOF commits for every sink in the pump.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::Int64Array;
use futures::StreamExt;
use hotlap::{InputId, Plan};
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_connectors::{ChangeStream, ConnectorError};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

#[path = "common/shutdown.rs"]
mod common;

use common::{Signal, keys_with};

#[derive(Default)]
struct Remote {
    staged: Mutex<Vec<i64>>,
    committed: Mutex<Vec<i64>>,
}

struct FailsLastWrite(Signal);

#[async_trait::async_trait]
impl Sink for FailsLastWrite {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            item?;
        }
        self.0.fire();
        Err(ConnectorError::Infrastructure(
            "last sink write failed".into(),
        ))
    }

    fn accepts_retractions(&self) -> bool {
        true
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Err(ConnectorError::Unsupported("writer abort failed".into()))
    }
}

struct StagesUntilCommit {
    remote: Arc<Remote>,
    written: Signal,
}

#[async_trait::async_trait]
impl Sink for StagesUntilCommit {
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
        }
        self.written.fire();
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
fn a_writer_failure_prevents_peer_eof_commit_and_fails_shutdown() {
    let _backend = common::SharedBackend::default();
    let _start = common::start;
    let (failed, failed_rx) = Signal::new();
    let (written, written_rx) = Signal::new();
    let remote = Arc::new(Remote::default());
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: keys_with(&[7], None),
        watermark: None,
    }])
    .unwrap();
    let plan = || Plan::Source(InputId(0));
    let handle = EngineHandle::start(Pipeline {
        sources,
        views: vec![("broken".into(), plan()), ("peer".into(), plan())],
        sinks: vec![
            SinkSpec {
                view: "broken".into(),
                sink: Arc::new(FailsLastWrite(failed)),
            },
            SinkSpec {
                view: "peer".into(),
                sink: Arc::new(StagesUntilCommit {
                    remote: remote.clone(),
                    written,
                }),
            },
        ],
        checkpoint: None,
        retention: None,
    })
    .unwrap();
    failed_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("failing writer never received the final batch");
    written_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("peer writer never staged the final batch");

    let error = handle.shutdown().expect_err("writer failure must surface");
    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert!(
        remote.committed.lock().unwrap().is_empty(),
        "the healthy peer must not commit after another writer fails"
    );
    assert_eq!(*remote.staged.lock().unwrap(), vec![7]);
}
