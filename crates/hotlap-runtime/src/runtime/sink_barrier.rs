//! Two-phase-commit coordination of a pipeline's sinks.
//!
//! The checkpoint barrier runs the phases in order: drain every sink's channel
//! so all queued deltas are applied, prepare every transactional sink, capture
//! and persist the checkpoint body, then commit. Draining first closes the gap
//! between the engine-side pump and the sink task, so a checkpoint can never be
//! marked valid while output deltas are still queued in its channel.
//!
//! Any failure before the commit completes aborts the prepared sinks, so the
//! checkpoint is discarded and the engine keeps its last valid one. The caller
//! clears the durable commit intent before it aborts, so recovery can never
//! promote sinks that were rolled back.
//!
//! Non-transactional sinks adapt the protocol: `Idempotent` sinks are only
//! flushed on commit (safe to replay after a crash), `AtLeastOnce` sinks are
//! not coordinated at all (their writes are already visible).
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

impl Prepared {
    /// The full set of prepared sinks, used when no participant committed.
    fn all(prepared: &Self) -> Self {
        Self {
            indices: prepared.indices.clone(),
        }
    }

    /// The prepared sinks from `slice` onward, used to abort the failed and
    /// still-uncommitted participants.
    fn from(indices: &[usize]) -> Self {
        Self {
            indices: indices.to_vec(),
        }
    }
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

    /// Phase two: flush idempotent sinks first, then commit the prepared
    /// transactional ones. At-least-once sinks are already visible and skipped.
    ///
    /// Flushing first means an idempotent failure cannot strand a checkpoint
    /// whose transactional sinks already committed (which would replay and
    /// duplicate). On failure the error carries the prepared sinks still to
    /// abort: all of them when an idempotent flush fails, otherwise the failed
    /// transactional sink and those after it. The caller aborts them after
    /// clearing the durable commit intent, so no sink keeps a half-open
    /// transaction and no rolled-back commit stays promotable.
    pub(crate) async fn commit(
        &self,
        prepared: &Prepared,
    ) -> Result<(), (ConnectorError, Prepared)> {
        for sync in &self.sinks {
            let sink = sync.sink();
            if sink.capabilities() == SinkCapabilities::Idempotent
                && let Err(error) = sink.commit().await
            {
                return Err((error, Prepared::all(prepared)));
            }
        }
        for (position, &index) in prepared.indices.iter().enumerate() {
            if let Err(error) = self.sinks[index].sink().commit().await {
                // The failed sink may not have committed, so abort it too.
                return Err((error, Prepared::from(&prepared.indices[position..])));
            }
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
    /// declaration: `Idempotent` sinks qualify by default, but any sink may opt
    /// in or out when its effects are not repeatable, and an `AtLeastOnce` sink
    /// must be discarded and replayed. A `Transactional` sink is not re-drivable
    /// unless it opts in explicitly.
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
