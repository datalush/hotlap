//! Commands from the handle to the engine thread and their dispatch.

use std::sync::Mutex;

use hotlap::Hotlap;
use hotlap::{Plan, ZSetBatch};
use tokio::sync::oneshot;

use crate::runtime::checkpoint::Checkpointer;
use crate::runtime::pipeline::{Pipeline, record_error};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::Source;

/// Command sent from the handle to the engine thread.
pub enum Command {
    Snapshot {
        view: String,
        reply: oneshot::Sender<Result<ZSetBatch, ConnectorError>>,
    },
    LateDropped {
        input: String,
        reply: oneshot::Sender<Result<u64, ConnectorError>>,
    },
    Checkpoint {
        reply: oneshot::Sender<Result<u64, ConnectorError>>,
    },
    BuildView {
        view: String,
        plan: Plan,
        reply: oneshot::Sender<Result<(), ConnectorError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Handle one command; returns true when the engine must shut down.
pub(crate) async fn handle(
    cmd: Option<Command>,
    hotlap: &mut Hotlap,
    pipeline: &Pipeline,
    checkpointer: &mut Option<Checkpointer>,
) -> bool {
    match cmd {
        Some(Command::Snapshot { view, reply }) => {
            let _ = reply.send(hotlap.snapshot(&view).map_err(map_err));
            false
        }
        Some(Command::LateDropped { input, reply }) => {
            let _ = reply.send(hotlap.late_dropped(&input).map_err(map_err));
            false
        }
        Some(Command::Checkpoint { reply }) => {
            let _ = reply.send(take(checkpointer, hotlap, pipeline.source.as_ref()).await);
            false
        }
        Some(Command::BuildView { view, plan, reply }) => {
            let _ = reply.send(hotlap.create_view(&view, plan).map_err(map_err));
            false
        }
        Some(Command::Shutdown { reply }) => {
            let _ = reply.send(());
            true
        }
        None => true,
    }
}

/// Run a periodic checkpoint, recording failures on their own error slot.
pub(crate) async fn run_periodic(
    checkpointer: &mut Option<Checkpointer>,
    hotlap: &Hotlap,
    pipeline: &Pipeline,
    checkpoint_error: &Mutex<Option<String>>,
) {
    if let Some(active) = checkpointer
        && let Err(error) = active.take(hotlap, pipeline.source.as_ref()).await
    {
        record_error(checkpoint_error, error);
    }
}

/// Take a checkpoint, or report that none is configured.
async fn take(
    checkpointer: &mut Option<Checkpointer>,
    hotlap: &Hotlap,
    source: &dyn Source,
) -> Result<u64, ConnectorError> {
    match checkpointer {
        Some(active) => active.take(hotlap, source).await,
        None => Err(ConnectorError::Unsupported(
            "checkpointing is not configured".into(),
        )),
    }
}

fn map_err(error: hotlap::HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(error.0)
}
