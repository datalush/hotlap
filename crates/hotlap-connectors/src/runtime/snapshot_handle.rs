//! Cloneable handle for reading snapshots and building views concurrently.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use hotlap::{Plan, ZSetBatch};
use tokio::sync::{mpsc, oneshot};

use crate::error::ConnectorError;
use crate::runtime::command::Command;
use crate::runtime::handle::stopped;

/// Cloneable, thread-safe handle to a running engine.
///
/// Shares the command channel and error slot with
/// [`EngineHandle`](super::handle::EngineHandle) but neither owns the engine
/// thread nor stops it on drop, so table providers can read snapshots and build
/// views concurrently with the owner.
#[derive(Clone)]
pub struct SnapshotHandle {
    pub(super) tx: mpsc::UnboundedSender<Command>,
    pub(super) last_error: Arc<Mutex<Option<String>>>,
    pub(super) built: Arc<AtomicBool>,
}

impl SnapshotHandle {
    /// Whether at least one batch has been pushed, i.e. the dataflow has been
    /// built. Before that, a declared view has no output to read.
    pub fn is_built(&self) -> bool {
        self.built.load(Ordering::SeqCst)
    }

    /// First source/ingestion error that stopped the source branch, if any.
    pub fn last_error(&self) -> Result<Option<String>, ConnectorError> {
        self.last_error
            .lock()
            .map(|slot| slot.clone())
            .map_err(|_| ConnectorError::Infrastructure("engine error state poisoned".into()))
    }

    /// Read the consolidated output of a view.
    ///
    /// Blocks on `blocking_recv`, so it must not be called from within an async
    /// runtime; callers in async code should offload it to a blocking thread.
    pub fn snapshot(&self, view: &str) -> Result<ZSetBatch, ConnectorError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Snapshot {
                view: view.to_string(),
                reply,
            })
            .map_err(|_| stopped())?;
        rx.blocking_recv().map_err(|_| stopped())?
    }

    /// Build a view after `START`, replaying the retained inputs.
    ///
    /// Blocks on `blocking_recv`, so it must not be called from within an async
    /// runtime; callers in async code should offload it to a blocking thread.
    pub fn build_view(&self, view: &str, plan: Plan) -> Result<(), ConnectorError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::BuildView {
                view: view.to_string(),
                plan,
                reply,
            })
            .map_err(|_| stopped())?;
        rx.blocking_recv().map_err(|_| stopped())?
    }
}
