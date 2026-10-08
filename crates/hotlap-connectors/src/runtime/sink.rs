//! Sink task: bounded changelog channel, serialized control and engine-side pump.

use std::sync::Arc;
use std::time::Duration;

use hotlap::{Hotlap, HotlapError};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::ConnectorError;
use crate::runtime::pipeline::SinkSpec;
pub use crate::runtime::shared_sink::SharedSink;
pub use crate::runtime::sink_sync::{ChangelogSender, SinkMessage, SinkSync};

/// Bound on how far a sink may lag the engine before backpressure bites.
const CHANNEL_CAPACITY: usize = 64;

/// How long `SinkPump::close` waits for one sink task before giving up.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Spawn a sink task that writes every received batch through `shared`.
///
/// Dropping the returned sender ends the changelog and lets the task finish.
/// The task ends with a final `commit` (delivery for the last batch), or an
/// `abort` on write failure. A coordinated sink may also be committed by the
/// checkpoint barrier, so `Sink::commit` must tolerate running more than once.
pub fn spawn_sink(
    shared: Arc<SharedSink>,
    capacity: usize,
) -> (ChangelogSender, JoinHandle<Result<(), ConnectorError>>) {
    let (tx, mut rx) = mpsc::channel(capacity);
    let handle = tokio::spawn(async move {
        match drive(&shared, &mut rx).await {
            Ok(()) => shared.commit().await,
            Err(error) => {
                let _ = shared.abort().await;
                Err(error)
            }
        }
    });
    (tx, handle)
}

/// Write every batch until the channel closes.
async fn drive(
    shared: &SharedSink,
    rx: &mut mpsc::Receiver<SinkMessage>,
) -> Result<(), ConnectorError> {
    while let Some(message) = rx.recv().await {
        match message {
            SinkMessage::Batch(item) => shared.write_batch(item?).await?,
            // In-order processing means every earlier batch is already written.
            SinkMessage::Flush(reply) => {
                let _ = reply.send(());
            }
        }
    }
    Ok(())
}

/// One running sink plus the view it is fed from.
struct SinkEntry {
    view: String,
    tx: ChangelogSender,
    shared: Arc<SharedSink>,
    handle: JoinHandle<Result<(), ConnectorError>>,
}

/// Engine-side owner of every sink channel and task.
pub struct SinkPump {
    entries: Vec<SinkEntry>,
}

impl SinkPump {
    /// Spawn one sink task per spec; the views were tapped during setup.
    pub fn start(specs: &[SinkSpec]) -> Self {
        let entries = specs
            .iter()
            .map(|spec| {
                let shared = SharedSink::new(Arc::clone(&spec.sink));
                let (tx, handle) = spawn_sink(Arc::clone(&shared), CHANNEL_CAPACITY);
                SinkEntry {
                    view: spec.view.clone(),
                    tx,
                    shared,
                    handle,
                }
            })
            .collect();
        Self { entries }
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
    /// backpressure to the source loop instead of buffering without bound.
    pub async fn pump(&self, hotlap: &mut Hotlap) -> Result<(), ConnectorError> {
        for entry in &self.entries {
            let changes = hotlap.take_changes(&entry.view).map_err(hotlap_err)?;
            if changes.is_empty() {
                continue;
            }
            entry
                .tx
                .send(SinkMessage::Batch(Ok(changes)))
                .await
                .map_err(|_| stopped())?;
        }
        Ok(())
    }

    /// Close every channel and wait for its task, surfacing the first failure.
    ///
    /// Each join is bounded by [`CLOSE_TIMEOUT`] so a stalled sink cannot hang
    /// shutdown forever; a timeout is reported like any other sink failure. The
    /// task is aborted and reaped so it cannot outlive `shutdown`.
    pub async fn close(self) -> Result<(), ConnectorError> {
        let mut failure = None;
        for mut entry in self.entries {
            drop(entry.tx);
            match tokio::time::timeout(CLOSE_TIMEOUT, &mut entry.handle).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => record(&mut failure, error),
                Ok(Err(_)) => record(&mut failure, stopped()),
                Err(_) => {
                    // The join handle is still ours; abort the stalled task and
                    // wait for it to unwind before reporting the timeout.
                    entry.handle.abort();
                    let _ = entry.handle.await;
                    record(&mut failure, timed_out());
                }
            }
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

fn hotlap_err(error: HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(error.0)
}
