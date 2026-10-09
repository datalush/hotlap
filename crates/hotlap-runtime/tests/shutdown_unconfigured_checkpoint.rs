//! A configuration error must not suppress healthy EOF delivery.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::Int64Array;
use futures::StreamExt;
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_connectors::{ChangeStream, ConnectorError};

#[path = "common/shutdown.rs"]
mod common;

use common::{Signal, keys_with, start};

#[derive(Default)]
struct Remote {
    staged: Mutex<Vec<i64>>,
    committed: Mutex<Vec<i64>>,
}

struct RecordingSink {
    remote: Arc<Remote>,
    written: Signal,
}

#[async_trait::async_trait]
impl Sink for RecordingSink {
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
fn unconfigured_checkpoint_error_preserves_healthy_eof_delivery() {
    let _backend = common::SharedBackend::default();
    let (written, written_rx) = Signal::new();
    let remote = Arc::new(Remote::default());
    let handle = start(
        keys_with(&[7], None),
        Arc::new(RecordingSink {
            remote: remote.clone(),
            written,
        }),
        None,
    );
    written_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("source payload never reached the sink");

    assert!(handle.checkpoint().is_err());
    handle
        .shutdown()
        .expect("configuration error is not a runtime failure");
    assert_eq!(*remote.committed.lock().unwrap(), vec![7]);
}
