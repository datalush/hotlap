//! Two-phase-commit coordination of a pipeline's sinks.
//!
//! The checkpoint barrier runs [`SinkBarrier::around`]: drain every sink's
//! channel so all queued deltas are applied, prepare every transactional sink,
//! capture and persist the checkpoint body, then commit. Draining first closes
//! the gap between the engine-side pump and the sink task, so a checkpoint can
//! never be marked valid while output deltas are still queued in its channel.
//!
//! Any failure before the commit completes aborts the prepared sinks, so the
//! checkpoint is discarded and the engine keeps its last valid one.
//!
//! Non-transactional sinks adapt the protocol: `Idempotent` sinks are only
//! flushed on commit (safe to replay after a crash), `AtLeastOnce` sinks are
//! not coordinated at all (their writes are already visible).
//!
//! Each sink is a [`SharedSink`](crate::runtime::sink::SharedSink), whose mutex
//! serializes these control calls against the concurrent `write` in the sink
//! task, so the 2PC contract holds.

use std::future::Future;

use tokio::sync::oneshot;

use crate::runtime::sink::{SinkMessage, SinkSync};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::SinkCapabilities;

/// The sinks a checkpoint barrier coordinates.
pub struct SinkBarrier {
    sinks: Vec<SinkSync>,
}

/// Indices of the transactional sinks that reached the prepared state.
pub struct Prepared {
    indices: Vec<usize>,
}

impl SinkBarrier {
    /// Build a barrier over `sinks`; an empty list is a no-op.
    pub fn new(sinks: Vec<SinkSync>) -> Self {
        Self { sinks }
    }

    /// Run the barrier order around `capture`: drain, prepare, `capture`, commit.
    ///
    /// `capture` snapshots the engine and writes the checkpoint body. When it
    /// fails, or when the later commit fails, the prepared sinks are aborted
    /// and the error is returned, so no checkpoint can become valid.
    pub async fn around<F, Fut, T>(&self, capture: F) -> Result<T, ConnectorError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, ConnectorError>>,
    {
        self.drain().await?;
        let prepared = self.prepare().await?;
        match capture().await {
            Ok(value) => self.commit(&prepared).await.map(|()| value),
            Err(error) => {
                self.abort(&prepared).await;
                Err(error)
            }
        }
    }

    /// Whether every coordinated sink declares its `commit` re-drivable.
    ///
    /// The barrier trusts each sink's
    /// [`commit_redriable`](hotlap_connectors::sink::Sink::commit_redriable)
    /// declaration: `Transactional` and `Idempotent` sinks qualify by default,
    /// but a sink may opt out when its effects are not actually repeatable, and
    /// an `AtLeastOnce` sink must be discarded and replayed.
    pub fn redriable(&self) -> bool {
        self.sinks.iter().all(|sync| sync.sink().commit_redriable())
    }

    /// Re-drive `commit` for every sink after an interrupted commit.
    ///
    /// Only valid when [`Self::redriable`] holds: `Sink::commit` must tolerate
    /// running more than once, which the sink contract already requires.
    pub async fn redrive_commit(&self) -> Result<(), ConnectorError> {
        for sync in &self.sinks {
            sync.sink().commit().await?;
        }
        Ok(())
    }

    /// Drain every channel so all queued deltas have reached their sink.
    ///
    /// A `Flush` is sent behind the queued batches and awaited; the sink task
    /// replies only after writing them, so its state covers the checkpoint.
    async fn drain(&self) -> Result<(), ConnectorError> {
        let mut replies = Vec::new();
        for sync in &self.sinks {
            let Some(sender) = sync.sender() else {
                continue;
            };
            let (reply, rx) = oneshot::channel();
            sender
                .send(SinkMessage::Flush(reply))
                .await
                .map_err(|_| stopped())?;
            replies.push(rx);
        }
        for rx in replies {
            rx.await.map_err(|_| stopped())?;
        }
        Ok(())
    }

    /// Phase one: prepare every transactional sink.
    ///
    /// A failure aborts the sinks that already prepared, so no sink keeps a
    /// half-open transaction.
    async fn prepare(&self) -> Result<Prepared, ConnectorError> {
        let mut indices = Vec::new();
        for (index, sync) in self.sinks.iter().enumerate() {
            let sink = sync.sink();
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

    /// Phase two: flush idempotent sinks first, then commit the prepared
    /// transactional ones. At-least-once sinks are already visible and skipped.
    ///
    /// Flushing first means an idempotent failure cannot strand a checkpoint
    /// whose transactional sinks already committed (which would replay and
    /// duplicate). On any commit or flush error every still-uncommitted
    /// prepared sink is aborted, matching the documented contract.
    async fn commit(&self, prepared: &Prepared) -> Result<(), ConnectorError> {
        for sync in &self.sinks {
            let sink = sync.sink();
            if sink.capabilities() == SinkCapabilities::Idempotent
                && let Err(error) = sink.commit().await
            {
                self.abort(prepared).await;
                return Err(error);
            }
        }
        for (position, &index) in prepared.indices.iter().enumerate() {
            if let Err(error) = self.sinks[index].sink().commit().await {
                // The failed sink may not have committed, so abort it too.
                self.abort_indices(&prepared.indices[position..]).await;
                return Err(error);
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
            let _ = self.sinks[index].sink().abort().await;
        }
    }
}

/// Error for a sink task that stopped before the barrier could drain it.
fn stopped() -> ConnectorError {
    ConnectorError::Infrastructure("sink task stopped before drain".into())
}
