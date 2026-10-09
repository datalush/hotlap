//! Two-phase-commit coordination of a pipeline's sinks.
//!
//! The checkpoint barrier runs the phases in order: drain every sink's channel
//! so all queued deltas are applied, prepare every transactional sink, capture
//! and persist the checkpoint body, then commit. Draining first closes the gap
//! between the engine-side pump and the sink task, so a checkpoint can never be
//! marked valid while output deltas are still queued in its channel.
//!
//! A failure in `prepare` aborts the sinks that already prepared, so no sink
//! keeps a half-open transaction. A failure during `commit` aborts nothing: a
//! participant may have confirmed, so the caller keeps the durable commit
//! intent and the body for recovery to re-drive or discard and replay.
//!
//! Non-transactional sinks adapt the protocol: `Idempotent` sinks are only
//! flushed on commit (safe to replay after a crash), and `AtLeastOnce` sinks
//! must still confirm their flush before the checkpoint is published: appends
//! may be visible but unacknowledged, so an unconfirmed delivery cannot certify
//! the offsets. Neither can be rolled back, so both are flushed before the
//! prepared transactional sinks commit.
//!
//! Each sink is a [`SharedSink`](crate::runtime::sink::SharedSink), whose mutex
//! serializes these control calls against the concurrent `write` in the sink
//! task, so the 2PC contract holds.

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

    /// Drain every channel so all queued deltas have reached their sink.
    ///
    /// A `Flush` is sent behind the queued batches and awaited; the sink task
    /// replies only after writing them, so its state covers the checkpoint.
    pub(crate) async fn drain(&self) -> Result<(), ConnectorError> {
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
    pub(crate) async fn prepare(&self) -> Result<Prepared, ConnectorError> {
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

    /// Phase two: flush the sinks that cannot be rolled back first, then commit
    /// the prepared transactional ones.
    ///
    /// `Idempotent` and `AtLeastOnce` sinks confirm delivery with `commit`;
    /// neither can be rolled back, so flushing them first means their failure
    /// cannot strand a checkpoint whose transactional sinks already committed
    /// (which would replay and duplicate).
    ///
    /// A failure does **not** abort anything: a participant may already have
    /// confirmed, and rolling a confirmed commit back would be wrong. The
    /// caller keeps the durable marker and body so recovery can re-drive or
    /// discard and replay.
    pub(crate) async fn commit(&self, prepared: &Prepared) -> Result<(), ConnectorError> {
        for sync in &self.sinks {
            let sink = sync.sink();
            let wait_for_delivery = matches!(
                sink.capabilities(),
                SinkCapabilities::Idempotent | SinkCapabilities::AtLeastOnce
            );
            if wait_for_delivery && let Err(error) = sink.commit().await {
                return Err(error);
            }
        }
        for &index in &prepared.indices {
            self.sinks[index].sink().commit().await?;
        }
        Ok(())
    }

    /// Abort every prepared transactional sink.
    pub(crate) async fn abort(&self, prepared: &Prepared) {
        self.abort_indices(&prepared.indices).await;
    }

    /// Whether every coordinated sink declares its `commit` re-drivable.
    ///
    /// The barrier trusts each sink's
    /// [`commit_redriable`](hotlap_connectors::sink::Sink::commit_redriable)
    /// declaration. No capability is re-drivable by default: a sink opts in
    /// explicitly only when it holds durable staged state or its re-driven
    /// commit is a true no-op, and otherwise is discarded and replayed.
    pub fn redriable(&self) -> bool {
        self.sinks.iter().all(|sync| sync.sink().commit_redriable())
    }

    /// Whether every coordinated sink tolerates replay after an interrupted
    /// commit.
    ///
    /// A transactional sink that cannot be re-driven must not be discarded and
    /// replayed: its commit may already be visible, so replay would duplicate
    /// it. Idempotent and at-least-once sinks declare their own replay contract
    /// (deduplication or documented duplication).
    pub fn replay_safe(&self) -> bool {
        self.sinks
            .iter()
            .all(|sync| sync.sink().capabilities() != SinkCapabilities::Transactional)
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
