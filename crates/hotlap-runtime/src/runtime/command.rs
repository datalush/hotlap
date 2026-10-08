//! Commands from the handle to the engine thread and their dispatch.

use std::sync::Mutex;

use hotlap::Hotlap;
use hotlap::{Plan, ZSetBatch};
use tokio::sync::oneshot;

use crate::runtime::checkpoint::Checkpointer;
use crate::runtime::pipeline::record_error;
use crate::runtime::sources::Sources;
use hotlap_connectors::error::ConnectorError;

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
///
/// After a source failure (`failed`), checkpoint and view-build commands are
/// rejected because the engine state may no longer be consistent. Reads and
/// shutdown stay available so the caller can still inspect and stop the engine.
pub(crate) async fn handle(
    cmd: Option<Command>,
    hotlap: &mut Hotlap,
    sources: &Sources,
    checkpointer: &mut Option<Checkpointer>,
    failed: bool,
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
            let result = match reject_if_failed(failed) {
                Ok(()) => take(checkpointer, hotlap, sources).await,
                Err(error) => Err(error),
            };
            let _ = reply.send(result);
            false
        }
        Some(Command::BuildView { view, plan, reply }) => {
            let result = match reject_if_failed(failed) {
                Ok(()) => hotlap.create_view(&view, plan).map_err(map_err),
                Err(error) => Err(error),
            };
            let _ = reply.send(result);
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
    sources: &Sources,
    checkpoint_error: &Mutex<Option<String>>,
) {
    if let Some(active) = checkpointer
        && let Err(error) = active.take(hotlap, sources).await
    {
        record_error(checkpoint_error, error);
    }
}

/// Take a checkpoint, or report that none is configured.
async fn take(
    checkpointer: &mut Option<Checkpointer>,
    hotlap: &Hotlap,
    sources: &Sources,
) -> Result<u64, ConnectorError> {
    match checkpointer {
        Some(active) => active.take(hotlap, sources).await,
        None => Err(ConnectorError::Unsupported(
            "checkpointing is not configured".into(),
        )),
    }
}

fn reject_if_failed(failed: bool) -> Result<(), ConnectorError> {
    if failed {
        return Err(ConnectorError::Infrastructure(
            "engine stopped after a source failure".into(),
        ));
    }
    Ok(())
}

fn map_err(error: hotlap::HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(error.0)
}
