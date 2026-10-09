//! One sink task: drain a bounded changelog channel and write each batch.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::{ChangelogSender, SinkMessage};
use crate::runtime::shared_sink::SharedSink;
use hotlap_connectors::error::ConnectorError;

const ABORT_TIMEOUT: Duration = Duration::from_secs(5);

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
    spawn_sink_task(shared, capacity, Arc::new(AtomicBool::new(true)), true)
}

pub(super) fn spawn_sink_for_pump(
    shared: Arc<SharedSink>,
    capacity: usize,
    close_clean: Arc<AtomicBool>,
) -> (ChangelogSender, JoinHandle<Result<(), ConnectorError>>) {
    spawn_sink_task(shared, capacity, close_clean, false)
}

fn spawn_sink_task(
    shared: Arc<SharedSink>,
    capacity: usize,
    close_clean: Arc<AtomicBool>,
    commit_on_eof: bool,
) -> (ChangelogSender, JoinHandle<Result<(), ConnectorError>>) {
    let (tx, mut rx) = mpsc::channel(capacity);
    let handle = tokio::spawn(async move {
        match drive(&shared, &mut rx).await {
            Ok(()) if commit_on_eof && close_clean.load(Ordering::SeqCst) => shared.commit().await,
            Ok(()) => Ok(()),
            Err(error) => {
                close_clean.store(false, Ordering::SeqCst);
                match tokio::time::timeout(ABORT_TIMEOUT, shared.abort()).await {
                    Ok(Ok(())) => Err(error),
                    Ok(Err(abort_error)) => Err(abort_error),
                    Err(_) => Err(ConnectorError::Infrastructure(
                        "sink abort timed out".into(),
                    )),
                }
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
pub(super) struct SinkEntry {
    pub(super) view: String,
    pub(super) tx: ChangelogSender,
    pub(super) shared: Arc<SharedSink>,
    pub(super) handle: JoinHandle<Result<(), ConnectorError>>,
}
