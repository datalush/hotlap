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
    binding_name: Option<String>,
    view: Option<String>,
}

impl SinkSync {
    /// Coordinate `sink`, draining `sender` before each barrier.
    pub fn new(sender: ChangelogSender, sink: Arc<SharedSink>) -> Self {
        Self {
            sender: Some(sender),
            sink,
            binding_name: None,
            view: None,
        }
    }

    /// Coordinate a sink using the same explicit name and view binding.
    pub fn new_bound(sender: ChangelogSender, sink: Arc<SharedSink>, binding: String) -> Self {
        Self::new_named(sender, sink, binding.clone(), binding)
    }

    /// Coordinate a sink with explicit binding name and view.
    pub fn new_named(
        sender: ChangelogSender,
        sink: Arc<SharedSink>,
        binding_name: String,
        view: String,
    ) -> Self {
        Self {
            sender: Some(sender),
            sink,
            binding_name: Some(binding_name),
            view: Some(view),
        }
    }

    /// Coordinate `sink` without a channel to drain.
    pub fn sink_only(sink: Arc<SharedSink>) -> Self {
        Self {
            sender: None,
            sink,
            binding_name: None,
            view: None,
        }
    }

    /// Coordinate an embedded sink with an explicit stable binding name.
    pub fn sink_only_bound(sink: Arc<SharedSink>, binding: String) -> Self {
        Self::sink_only_named(sink, binding.clone(), binding)
    }

    /// Coordinate an embedded sink with explicit binding name and view.
    pub fn sink_only_named(sink: Arc<SharedSink>, binding_name: String, view: String) -> Self {
        Self {
            sender: None,
            sink,
            binding_name: Some(binding_name),
            view: Some(view),
        }
    }

    /// The shared sink the barrier prepares and commits.
    pub(crate) fn sink(&self) -> &Arc<SharedSink> {
        &self.sink
    }

    /// The channel to drain, when the sink is fed through one.
    pub(crate) fn sender(&self) -> Option<&ChangelogSender> {
        self.sender.as_ref()
    }

    pub(crate) fn binding_name(&self) -> Option<&str> {
        self.binding_name.as_deref()
    }

    pub(crate) fn view(&self) -> Option<&str> {
        self.view.as_deref()
    }
}
