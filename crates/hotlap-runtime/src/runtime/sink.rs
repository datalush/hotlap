//! Sink task: bounded changelog channel, serialized control and engine-side pump.

mod close;
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
    close_clean: Arc<AtomicBool>,
    /// Whether shutdown abandoned a send parked on a full channel.
    interrupted: AtomicBool,
}

impl SinkPump {
    /// Spawn one sink task per spec; the views were tapped during setup.
    pub fn start(specs: &[SinkSpec]) -> Self {
        Self::start_with_metrics(specs, None, Cancel::new(), Arc::new(AtomicBool::new(true)))
    }

    /// Like [`Self::start`], with shared metrics, cancellation and close state.
    pub(crate) fn start_with_metrics(
        specs: &[SinkSpec],
        metrics: Option<Arc<MetricsRegistry>>,
        cancel: Cancel,
        close_clean: Arc<AtomicBool>,
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
                let (tx, handle) = writer::spawn_sink_for_pump(
                    Arc::clone(&shared),
                    CHANNEL_CAPACITY,
                    Arc::clone(&close_clean),
                );
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
            close_clean,
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
}

fn stopped() -> ConnectorError {
    ConnectorError::Infrastructure("sink task stopped".into())
}

fn cancelled() -> ConnectorError {
    ConnectorError::Infrastructure(
        "shutdown cancelled a sink send before the changelog drained".into(),
    )
}

fn hotlap_err(error: HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(error.0)
}
