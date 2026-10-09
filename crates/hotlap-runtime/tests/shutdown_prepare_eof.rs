//! A cancelled prepare must not be converted into a final commit by EOF.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::Int64Array;
use futures::StreamExt;
use hotlap::{InputId, state::StateBackend};
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_connectors::{ChangeStream, ConnectorError};
use hotlap_runtime::runtime::checkpoint::CheckpointConfig;
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

#[path = "common/shutdown.rs"]
mod common;

use common::{SharedBackend, Signal, keys_with, start};

#[derive(Default)]
struct Remote {
    staged: Mutex<Vec<i64>>,
    committed: Mutex<Vec<i64>>,
}

struct PrepareStalls {
    remote: Arc<Remote>,
    pending: Mutex<Vec<i64>>,
    written: Signal,
    entered: Signal,
}

#[async_trait::async_trait]
impl Sink for PrepareStalls {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            let batch = item?;
            let values = batch
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            self.pending
                .lock()
                .unwrap()
                .extend((0..values.len()).map(|row| values.value(row)));
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

    async fn prepare(&self) -> Result<(), ConnectorError> {
        *self.remote.staged.lock().unwrap() = std::mem::take(&mut *self.pending.lock().unwrap());
        self.entered.fire();
        futures::future::pending().await
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
fn shutdown_does_not_commit_payload_staged_by_cancelled_prepare() {
    let (written, written_rx) = Signal::new();
    let (entered, entered_rx) = Signal::new();
    let remote = Arc::new(Remote::default());
    let backend = SharedBackend::default();
    let sink = Arc::new(PrepareStalls {
        remote: remote.clone(),
        pending: Mutex::new(Vec::new()),
        written,
        entered,
    });
    let handle = start(
        keys_with(&[7], None),
        sink.clone(),
        Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain: 3,
        }),
    );
    written_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("source payload never reached the sink");

    let snapshot = handle.snapshot_handle();
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = reply_tx.send(snapshot.checkpoint());
    });
    entered_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("prepare never staged the payload");
    assert_eq!(*remote.staged.lock().unwrap(), vec![7]);
    assert_shutdown_keeps_prepare_unpublished(handle, reply_rx, remote, backend, sink);
}

fn assert_shutdown_keeps_prepare_unpublished(
    handle: hotlap_runtime::runtime::handle::EngineHandle,
    reply_rx: std::sync::mpsc::Receiver<Result<u64, ConnectorError>>,
    remote: Arc<Remote>,
    backend: SharedBackend,
    sink: Arc<PrepareStalls>,
) {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done_tx.send(handle.shutdown());
    });
    let result = done_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("shutdown blocked past its OS watchdog");

    assert!(result.is_err(), "cancelled prepare must fail shutdown");
    assert!(
        reply_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("checkpoint reply remained pending")
            .is_err()
    );
    assert_eq!(
        *remote.committed.lock().unwrap(),
        Vec::<i64>::new(),
        "EOF must not publish payload from a cancelled prepare"
    );
    assert!(backend.get(b"checkpoint/1/commit").unwrap().is_some());
    assert!(backend.get(b"checkpoint/1/engine").unwrap().is_none());
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_none());
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: keys_with(&[7], None),
        watermark: None,
    }])
    .unwrap();
    let checkpointer = hotlap_runtime::runtime::checkpoint::Checkpointer::new(Box::new(backend), 3)
        .with_sinks(vec![SinkSync::sink_only(SharedSink::new(sink))]);
    assert!(matches!(
        Recovery::inspect(&checkpointer, &sources).unwrap(),
        RecoveryDecision::Reject { pending: 1, .. }
    ));
}
