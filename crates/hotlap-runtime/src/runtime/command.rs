//! Commands from the handle to the engine thread and their dispatch.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use hotlap::Hotlap;
use hotlap::{Plan, ZSetBatch};
use tokio::sync::oneshot;

use crate::runtime::checkpoint::{CheckpointState, Checkpointer};
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
/// `failed` marks an engine state that is no longer consistent. Once set,
/// checkpoint and view-build commands are rejected and ingestion stops, because
/// aborting the sinks did not roll the engine or the source offsets back. Reads
/// and shutdown stay available so the caller can still inspect and stop the
/// engine. A failed checkpoint sets `failed` before it replies, so the caller
/// cannot poll a source through the race.
pub(crate) async fn handle(
    cmd: Option<Command>,
    hotlap: &mut Hotlap,
    sources: &Sources,
    checkpointer: &mut Option<Checkpointer>,
    failed: &mut bool,
    close_clean: &AtomicBool,
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
            let result = match reject_if_failed(*failed) {
                Ok(()) => take(checkpointer, hotlap, sources).await,
                Err(error) => Err(error),
            };
            if result.is_err() {
                close_clean.store(false, Ordering::SeqCst);
            }
            if result.is_err() && checkpointer.as_ref().is_some_and(inconsistent) {
                *failed = true;
            }
            let _ = reply.send(result);
            false
        }
        Some(Command::BuildView { view, plan, reply }) => {
            let result = match reject_if_failed(*failed) {
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

/// Run a periodic checkpoint; returns whether it left the state inconsistent.
pub(crate) async fn run_periodic(
    checkpointer: &mut Option<Checkpointer>,
    hotlap: &Hotlap,
    sources: &Sources,
    checkpoint_error: &Mutex<Option<String>>,
    close_clean: &AtomicBool,
) -> bool {
    let Some(active) = checkpointer else {
        return false;
    };
    match active.take(hotlap, sources).await {
        Ok(_) => false,
        Err(error) => {
            close_clean.store(false, Ordering::SeqCst);
            let inconsistent = inconsistent(active);
            record_error(checkpoint_error, error);
            inconsistent
        }
    }
}

/// Whether a checkpointer still refuses new attempts after a failed one.
fn inconsistent(checkpointer: &Checkpointer) -> bool {
    checkpointer.state() != CheckpointState::Ready
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
            "engine stopped after a runtime failure".into(),
        ));
    }
    Ok(())
}

fn map_err(error: hotlap::HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(error.0)
}
