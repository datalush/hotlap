//! Sink task: bounded changelog channel, stream adapter and engine-side pump.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use hotlap::{ChangeBatch, Hotlap, HotlapError};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::ConnectorError;
use crate::runtime::pipeline::SinkSpec;
use crate::sink::Sink;

/// Bound on how far a sink may lag the engine before backpressure bites.
const CHANNEL_CAPACITY: usize = 64;

/// Receiver end of a sink's bounded changelog channel.
pub struct ChangelogStream {
    rx: mpsc::Receiver<Result<ChangeBatch, ConnectorError>>,
}

impl Stream for ChangelogStream {
    type Item = Result<ChangeBatch, ConnectorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Sender side of a sink's bounded changelog channel.
pub type ChangelogSender = mpsc::Sender<Result<ChangeBatch, ConnectorError>>;

/// Spawn the sink task; returns the bounded sender the engine pumps into.
///
/// Dropping the returned sender ends the changelog and lets the task finish,
/// which is what makes shutdown deliver the last batches.
pub fn spawn_sink(
    sink: Arc<dyn Sink>,
    capacity: usize,
) -> (ChangelogSender, JoinHandle<Result<(), ConnectorError>>) {
    let (tx, rx) = mpsc::channel(capacity);
    let handle = tokio::spawn(async move { sink.write(Box::pin(ChangelogStream { rx })).await });
    (tx, handle)
}

/// One running sink plus the view it is fed from.
struct SinkEntry {
    view: String,
    tx: ChangelogSender,
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
                let (tx, handle) = spawn_sink(Arc::clone(&spec.sink), CHANNEL_CAPACITY);
                SinkEntry {
                    view: spec.view.clone(),
                    tx,
                    handle,
                }
            })
            .collect();
        Self { entries }
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
                .send(Ok(ChangeBatch { rows: changes }))
                .await
                .map_err(|_| stopped())?;
        }
        Ok(())
    }

    /// Close every channel and wait for its task, surfacing the first failure.
    pub async fn close(self) -> Result<(), ConnectorError> {
        let mut failure = None;
        for entry in self.entries {
            drop(entry.tx);
            match entry.handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => record(&mut failure, error),
                Err(_) => record(&mut failure, stopped()),
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

fn hotlap_err(error: HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(error.0)
}
