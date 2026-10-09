//! One sink task: drain a bounded changelog channel and write each batch.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::{ChangelogSender, SinkMessage};
use crate::runtime::shared_sink::SharedSink;
use hotlap_connectors::error::ConnectorError;

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
pub(super) struct SinkEntry {
    pub(super) view: String,
    pub(super) tx: ChangelogSender,
    pub(super) shared: Arc<SharedSink>,
    pub(super) handle: JoinHandle<Result<(), ConnectorError>>,
}
