//! Sink task: bounded changelog channel, serialized control and engine-side pump.

mod writer;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use hotlap::{Hotlap, HotlapError};

use crate::runtime::cancel::Cancel;
use crate::runtime::pipeline::SinkSpec;
pub use crate::runtime::shared_sink::SharedSink;
pub use crate::runtime::sink_sync::{ChangelogSender, SinkMessage, SinkSync};
use hotlap_connectors::error::ConnectorError;
use hotlap_engine::MetricsRegistry;
use writer::SinkEntry;
pub use writer::spawn_sink;

/// Bound on how far a sink may lag the engine before backpressure bites.
pub const CHANNEL_CAPACITY: usize = 64;

/// How long `SinkPump::close` waits for one sink task before giving up.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Bound on reaping an aborted task before the close gives up on it.
const REAP_TIMEOUT: Duration = Duration::from_secs(1);

/// Engine-side owner of every sink channel and task.
pub struct SinkPump {
    entries: Vec<SinkEntry>,
    cancel: Cancel,
    /// Whether shutdown abandoned a send parked on a full channel.
    interrupted: AtomicBool,
}

impl SinkPump {
    /// Spawn one sink task per spec; the views were tapped during setup.
    pub fn start(specs: &[SinkSpec]) -> Self {
        Self::start_with_metrics(specs, None, Cancel::new())
    }

    /// Like [`Self::start`], but counts successful commits into `metrics` and
    /// carries the engine's cancellation flag for a parked pump.
    pub(crate) fn start_with_metrics(
        specs: &[SinkSpec],
        metrics: Option<Arc<MetricsRegistry>>,
        cancel: Cancel,
    ) -> Self {
        let entries = specs
            .iter()
            .map(|spec| {
                let shared = match &metrics {
                    Some(metrics) => {
                        SharedSink::with_metrics(Arc::clone(&spec.sink), Arc::clone(metrics))
                    }
                    None => SharedSink::new(Arc::clone(&spec.sink)),
                };
                let (tx, handle) = spawn_sink(Arc::clone(&shared), CHANNEL_CAPACITY);
                SinkEntry {
                    view: spec.view.clone(),
                    tx,
                    shared,
                    handle,
                }
            })
            .collect();
        Self {
            entries,
            cancel,
            interrupted: AtomicBool::new(false),
        }
    }

    /// The sinks the checkpoint barrier coordinates, sharing this pump's state.
    ///
    /// Each entry pairs the sink with the sender of its channel, so the barrier
    /// can drain queued deltas before it prepares and commits.
    pub fn coordinated(&self) -> Vec<SinkSync> {
        self.entries
            .iter()
            .map(|entry| SinkSync::new(entry.tx.clone(), Arc::clone(&entry.shared)))
            .collect()
    }

    /// Drain each tapped view's deltas and push them to its sink.
    ///
    /// Awaiting `send` blocks while a channel is full, which propagates
    /// backpressure to the source loop instead of buffering without bound. A
    /// ready send wins so a clean shutdown never drops a batch, but once the
    /// engine cancels, a send parked on a full channel is abandoned and the
    /// pump reports that delivery could not be guaranteed.
    pub async fn pump(&self, hotlap: &mut Hotlap) -> Result<(), ConnectorError> {
        for entry in &self.entries {
            let changes = hotlap.take_changes(&entry.view).map_err(hotlap_err)?;
            if changes.is_empty() {
                continue;
            }
            let message = SinkMessage::Batch(Ok(changes));
            tokio::select! {
                biased;
                sent = entry.tx.send(message) => sent.map_err(|_| stopped())?,
                () = self.cancel.cancelled() => {
                    self.interrupted.store(true, Ordering::SeqCst);
                    return Err(cancelled());
                }
            }
        }
        Ok(())
    }

    /// Close every channel and wait for its task, surfacing the first failure.
    ///
    /// Each join is bounded by [`CLOSE_TIMEOUT`]. A timed-out task is aborted
    /// and reaped: abort drops the task at its next await, so a sink parked in
    /// an await cannot outlive the close. A task stuck in work that never
    /// yields cannot be interrupted by abort, so the close still reports the
    /// timeout instead of claiming delivery. If cancellation abandoned a send,
    /// delivery is likewise unproven and the close fails even when every task
    /// happened to finish.
    pub async fn close(self) -> Result<(), ConnectorError> {
        let interrupted = self.interrupted.load(Ordering::SeqCst);
        let mut failure = None;
        for mut entry in self.entries {
            drop(entry.tx);
            match tokio::time::timeout(CLOSE_TIMEOUT, &mut entry.handle).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => record(&mut failure, error),
                Ok(Err(join)) => record(&mut failure, join_error(join)),
                Err(_) => {
                    // The join handle is still ours; abort the stalled task and
                    // try to reap it. The reap is bounded: an aborted task is
                    // dropped at its next await, but a task stuck in work that
                    // never yields cannot be interrupted, so the close must not
                    // await it without a bound.
                    entry.handle.abort();
                    let _ = tokio::time::timeout(REAP_TIMEOUT, &mut entry.handle).await;
                    record(&mut failure, timed_out());
                }
            }
        }
        if failure.is_none() && interrupted {
            return Err(cancelled());
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn record(slot: &mut Option<ConnectorError>, error: ConnectorError) {
    slot.get_or_insert(error);
}

fn stopped() -> ConnectorError {
    ConnectorError::Infrastructure("sink task stopped".into())
}

fn timed_out() -> ConnectorError {
    ConnectorError::Infrastructure("sink task timed out during close".into())
}

fn cancelled() -> ConnectorError {
    ConnectorError::Infrastructure(
        "shutdown cancelled a sink send before the changelog drained".into(),
    )
}

fn join_error(error: tokio::task::JoinError) -> ConnectorError {
    if error.is_panic() {
        ConnectorError::Infrastructure(format!("sink task panicked: {error}"))
    } else {
        stopped()
    }
}

fn hotlap_err(error: HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(error.0)
}
