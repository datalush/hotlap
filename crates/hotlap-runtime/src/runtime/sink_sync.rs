//! Sink channel protocol and the checkpoint barrier's per-sink handle.

use std::sync::Arc;

use hotlap::ZSetBatch;
use tokio::sync::{mpsc, oneshot};

use crate::runtime::shared_sink::SharedSink;
use hotlap_connectors::error::ConnectorError;

/// One message on a sink's bounded changelog channel.
///
/// The checkpoint barrier sends [`SinkMessage::Flush`] behind every queued
/// batch; because the sink task consumes the channel in order, the reply proves
/// that all earlier batches have reached the sink, closing the gap between the
/// engine-side pump and the sink task.
pub enum SinkMessage {
    /// A changelog batch to write.
    Batch(Result<ZSetBatch, ConnectorError>),
    /// Drain barrier: reply once every earlier batch has been written.
    Flush(oneshot::Sender<()>),
}

/// Sender side of a sink's bounded changelog channel.
pub type ChangelogSender = mpsc::Sender<SinkMessage>;

/// A sink paired with the channel the engine feeds it through.
///
/// The checkpoint barrier needs both: it first drains the channel so every
/// queued delta reaches the sink, then runs the 2PC control calls. A sink with
/// no channel (embedded use, tests) coordinates the control calls directly.
pub struct SinkSync {
    sender: Option<ChangelogSender>,
    sink: Arc<SharedSink>,
}

impl SinkSync {
    /// Coordinate `sink`, draining `sender` before each barrier.
    pub fn new(sender: ChangelogSender, sink: Arc<SharedSink>) -> Self {
        Self {
            sender: Some(sender),
            sink,
        }
    }

    /// Coordinate `sink` without a channel to drain.
    pub fn sink_only(sink: Arc<SharedSink>) -> Self {
        Self { sender: None, sink }
    }

    /// The shared sink the barrier prepares and commits.
    pub(crate) fn sink(&self) -> &Arc<SharedSink> {
        &self.sink
    }

    /// The channel to drain, when the sink is fed through one.
    pub(crate) fn sender(&self) -> Option<&ChangelogSender> {
        self.sender.as_ref()
    }
}
