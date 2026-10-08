//! A sink shared by its write task and the checkpoint barrier.

use std::sync::Arc;

use hotlap::ZSetBatch;
use tokio::sync::Mutex;

use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_engine::MetricsRegistry;

/// A sink shared by its write task and the checkpoint barrier.
///
/// Every call takes the same async mutex, so a `prepare`/`commit`/`abort` from
/// the barrier can never overlap a `write` in the task even though both hold the
/// same `Arc`. This is what makes the two-phase-commit contract sound for a real
/// [`SinkCapabilities::Transactional`] sink. The write task drives one batch per
/// call, so control between batches is never blocked behind a live stream.
pub struct SharedSink {
    inner: Arc<dyn Sink>,
    lock: Mutex<()>,
    /// Shared counter registry; `None` for a sink built outside the runtime.
    metrics: Option<Arc<MetricsRegistry>>,
}

impl SharedSink {
    /// Wrap `inner` so writes and barrier control calls are serialized.
    pub fn new(inner: Arc<dyn Sink>) -> Arc<Self> {
        Self::build(inner, None)
    }

    /// Like [`Self::new`], but reports successful commits into `metrics`.
    pub fn with_metrics(inner: Arc<dyn Sink>, metrics: Arc<MetricsRegistry>) -> Arc<Self> {
        Self::build(inner, Some(metrics))
    }

    fn build(inner: Arc<dyn Sink>, metrics: Option<Arc<MetricsRegistry>>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            lock: Mutex::new(()),
            metrics,
        })
    }

    /// Delivery guarantee of the wrapped sink.
    pub fn capabilities(&self) -> SinkCapabilities {
        self.inner.capabilities()
    }

    /// Write one batch, serialized against the barrier's control calls.
    pub async fn write_batch(&self, batch: ZSetBatch) -> Result<(), ConnectorError> {
        let _guard = self.lock.lock().await;
        let stream = Box::pin(futures::stream::iter(vec![Ok(batch)]));
        self.inner.write(stream).await
    }

    /// First 2PC phase, serialized against writes.
    pub async fn prepare(&self) -> Result<(), ConnectorError> {
        let _guard = self.lock.lock().await;
        self.inner.prepare().await
    }

    /// Commit, serialized against writes.
    pub async fn commit(&self) -> Result<(), ConnectorError> {
        let _guard = self.lock.lock().await;
        self.inner.commit().await?;
        if let Some(metrics) = &self.metrics {
            metrics.inc("sinks_committed");
        }
        Ok(())
    }

    /// Abort, serialized against writes.
    pub async fn abort(&self) -> Result<(), ConnectorError> {
        let _guard = self.lock.lock().await;
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use tokio::sync::oneshot;

    use super::*;
    use hotlap_connectors::sink::ChangeStream;

    /// A transactional sink whose `write` blocks until released.
    struct BlockingSink {
        entered: Arc<AtomicBool>,
        release: StdMutex<Option<oneshot::Receiver<()>>>,
        overlap: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Sink for BlockingSink {
        async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
            self.entered.store(true, Ordering::SeqCst);
            let receiver = self.release.lock().unwrap().take();
            if let Some(rx) = receiver {
                let _ = rx.await;
            }
            self.entered.store(false, Ordering::SeqCst);
            Ok(())
        }

        fn capabilities(&self) -> SinkCapabilities {
            SinkCapabilities::Transactional
        }

        async fn prepare(&self) -> Result<(), ConnectorError> {
            if self.entered.load(Ordering::SeqCst) {
                self.overlap.store(true, Ordering::SeqCst);
            }
            Ok(())
        }

        async fn commit(&self) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn abort(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    fn batch() -> ZSetBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let columns: Vec<ArrowRef> = vec![Arc::new(Int64Array::from(vec![1]))];
        let record = RecordBatch::try_new(schema, columns).unwrap();
        ZSetBatch::new(record, Arc::new(Int64Array::from(vec![1]))).unwrap()
    }

    type ArrowRef = Arc<dyn arrow::array::Array>;

    #[tokio::test]
    async fn prepare_waits_for_an_in_flight_write() {
        let (release, held) = oneshot::channel();
        let sink = Arc::new(BlockingSink {
            entered: Arc::new(AtomicBool::new(false)),
            release: StdMutex::new(Some(held)),
            overlap: Arc::new(AtomicBool::new(false)),
        });
        let shared = SharedSink::new(sink.clone());

        let writer = {
            let shared = Arc::clone(&shared);
            tokio::spawn(async move { shared.write_batch(batch()).await })
        };
        while !sink.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        // A write owns the lock, so prepare cannot run concurrently.
        let blocked = tokio::time::timeout(Duration::from_millis(50), shared.prepare()).await;
        assert!(
            blocked.is_err(),
            "prepare must wait for the in-flight write"
        );

        release.send(()).unwrap();
        writer.await.unwrap().unwrap();
        shared.prepare().await.unwrap();
        assert!(
            !sink.overlap.load(Ordering::SeqCst),
            "control must never observe an in-flight write"
        );
    }
}
