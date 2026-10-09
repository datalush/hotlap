//! Two-phase-commit coordination of a pipeline's sinks.
//!
//! The checkpoint barrier runs the phases in order: drain every sink's channel
//! so all queued deltas are applied, prepare every transactional sink, capture
//! and persist the checkpoint body, then commit. Draining first closes the gap
//! between the engine-side pump and the sink task, so a checkpoint can never be
//! marked valid while output deltas are still queued in its channel.
//!
//! A failure in `prepare` aborts every participant that might have staged data;
//! the prepare marker stays unless each abort is confirmed. A failure during
//! `commit` aborts nothing because a participant may already have confirmed.
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

use crate::runtime::cancel::Cancel;
use crate::runtime::sink::{SinkMessage, SinkSync};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::SinkCapabilities;

#[path = "sink_barrier/abort.rs"]
mod abort;
#[path = "sink_barrier/recovery.rs"]
mod recovery;

/// The sinks a checkpoint barrier coordinates.
pub struct SinkBarrier {
    sinks: Vec<SinkSync>,
}

/// Indices of the transactional sinks that reached the prepared state.
pub struct Prepared {
    indices: Vec<usize>,
}

pub(crate) struct PrepareFailure {
    pub(crate) error: ConnectorError,
    pub(crate) aborted: bool,
}

impl SinkBarrier {
    /// Build a barrier over `sinks`; an empty list is a no-op.
    pub fn new(sinks: Vec<SinkSync>) -> Self {
        Self { sinks }
    }

    /// Drain every channel so all queued deltas have reached their sink.
    ///
    /// A `Flush` is sent behind the queued batches and awaited; the sink task
    /// replies only after writing them, so its state covers the checkpoint. A
    /// cancel abandons a flush parked on a stalled sink, so shutdown is not held
    /// by the drain.
    pub(crate) async fn drain(&self, cancel: &Cancel) -> Result<(), ConnectorError> {
        let mut replies = Vec::new();
        for sync in &self.sinks {
            let Some(sender) = sync.sender() else {
                continue;
            };
            let (reply, rx) = oneshot::channel();
            match cancel.race(sender.send(SinkMessage::Flush(reply))).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(stopped()),
                Err(error) => return Err(error),
            }
            replies.push(rx);
        }
        for rx in replies {
            match cancel.race(rx).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(stopped()),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Phase one: prepare every transactional sink.
    ///
    /// A failure aborts each sink whose `prepare` may have changed remote state,
    /// including the participant that returned the error. Cancellation skips
    /// abort because the operation may be parked; its durable marker remains.
    pub(crate) async fn prepare(&self, cancel: &Cancel) -> Result<Prepared, PrepareFailure> {
        let mut indices = Vec::new();
        for (index, sync) in self.sinks.iter().enumerate() {
            let sink = sync.sink();
            if sink.capabilities() != SinkCapabilities::Transactional {
                continue;
            }
            match cancel.race(sink.prepare()).await {
                Ok(Ok(())) => indices.push(index),
                Ok(Err(error)) => {
                    indices.push(index);
                    return Err(match self.abort_indices(&indices).await {
                        Ok(()) => PrepareFailure {
                            error,
                            aborted: true,
                        },
                        Err(error) => PrepareFailure {
                            error,
                            aborted: false,
                        },
                    });
                }
                Err(error) => {
                    return Err(PrepareFailure {
                        error,
                        aborted: false,
                    });
                }
            }
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
    /// discard and replay. A cancellation is reported the same way.
    pub(crate) async fn commit(
        &self,
        prepared: &Prepared,
        cancel: &Cancel,
    ) -> Result<(), ConnectorError> {
        for sync in &self.sinks {
            let sink = sync.sink();
            let wait_for_delivery = matches!(
                sink.capabilities(),
                SinkCapabilities::Idempotent | SinkCapabilities::AtLeastOnce
            );
            if wait_for_delivery {
                match cancel.race(sink.commit()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) | Err(error) => return Err(error),
                }
            }
        }
        for &index in &prepared.indices {
            match cancel.race(self.sinks[index].sink().commit()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) | Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Abort every prepared transactional sink, failing if rollback is unconfirmed.
    pub(crate) async fn abort(&self, prepared: &Prepared) -> Result<(), ConnectorError> {
        self.abort_indices(&prepared.indices).await
    }
}

/// Error for a sink task that stopped before the barrier could drain it.
fn stopped() -> ConnectorError {
    ConnectorError::Infrastructure("sink task stopped before drain".into())
}
