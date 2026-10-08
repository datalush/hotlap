//! Two-phase-commit coordination of a pipeline's sinks.
//!
//! The checkpoint barrier runs [`SinkBarrier::around`]: prepare every
//! transactional sink, capture and persist the checkpoint body, then commit.
//! Any failure before the commit completes aborts the prepared sinks, so the
//! checkpoint is discarded and the engine keeps its last valid one.
//!
//! Non-transactional sinks adapt the protocol: `Idempotent` sinks are only
//! flushed on commit (safe to replay after a crash), `AtLeastOnce` sinks are
//! not coordinated at all (their writes are already visible).
//!
//! The barrier calls `prepare`/`commit`/`abort` directly on the sink objects,
//! so a `Transactional` sink must serialize those calls against a concurrent
//! [`Sink::write`](crate::sink::Sink::write).

use std::future::Future;
use std::sync::Arc;

use crate::error::ConnectorError;
use crate::sink::{Sink, SinkCapabilities};

/// The sinks a checkpoint barrier coordinates.
pub struct SinkBarrier {
    sinks: Vec<Arc<dyn Sink>>,
}

/// Indices of the transactional sinks that reached the prepared state.
pub struct Prepared {
    indices: Vec<usize>,
}

impl SinkBarrier {
    /// Build a barrier over `sinks`; an empty list is a no-op.
    pub fn new(sinks: Vec<Arc<dyn Sink>>) -> Self {
        Self { sinks }
    }

    /// Run the 2PC order around `capture`: prepare, `capture`, commit.
    ///
    /// `capture` snapshots the engine and writes the checkpoint body. When it
    /// fails, or when the later commit fails, the prepared sinks are aborted
    /// and the error is returned, so no checkpoint can become valid.
    pub async fn around<F, Fut, T>(&self, capture: F) -> Result<T, ConnectorError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, ConnectorError>>,
    {
        let prepared = self.prepare().await?;
        match capture().await {
            Ok(value) => self.commit(&prepared).await.map(|()| value),
            Err(error) => {
                self.abort(&prepared).await;
                Err(error)
            }
        }
    }

    /// Phase one: prepare every transactional sink.
    ///
    /// A failure aborts the sinks that already prepared, so no sink keeps a
    /// half-open transaction.
    async fn prepare(&self) -> Result<Prepared, ConnectorError> {
        let mut indices = Vec::new();
        for (index, sink) in self.sinks.iter().enumerate() {
            if sink.capabilities() != SinkCapabilities::Transactional {
                continue;
            }
            if let Err(error) = sink.prepare().await {
                self.abort_indices(&indices).await;
                return Err(error);
            }
            indices.push(index);
        }
        Ok(Prepared { indices })
    }

    /// Phase two: commit prepared transactional sinks and flush idempotent
    /// ones. At-least-once sinks are already visible, so they are skipped.
    ///
    /// On failure, the prepared sinks that have not committed yet are aborted.
    async fn commit(&self, prepared: &Prepared) -> Result<(), ConnectorError> {
        for (position, &index) in prepared.indices.iter().enumerate() {
            if let Err(error) = self.sinks[index].commit().await {
                self.abort_indices(&prepared.indices[position + 1..]).await;
                return Err(error);
            }
        }
        for sink in &self.sinks {
            if sink.capabilities() == SinkCapabilities::Idempotent {
                // No prepare to abort: an idempotent replay is always safe.
                sink.commit().await?;
            }
        }
        Ok(())
    }

    /// Abort every prepared transactional sink.
    async fn abort(&self, prepared: &Prepared) {
        self.abort_indices(&prepared.indices).await;
    }

    /// Best-effort abort of the given sink indices.
    async fn abort_indices(&self, indices: &[usize]) {
        for &index in indices {
            let _ = self.sinks[index].abort().await;
        }
    }
}
