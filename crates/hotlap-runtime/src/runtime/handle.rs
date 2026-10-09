//! Thread-safe handle to a running engine, and the command protocol.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use hotlap::ZSetBatch;
use hotlap_engine::MetricsRegistry;
use tokio::sync::{mpsc, oneshot};

use crate::runtime::command::Command;
use crate::runtime::engine::{self, EngineShared};
use crate::runtime::pipeline::Pipeline;
use crate::runtime::snapshot_handle::SnapshotHandle;
use hotlap_connectors::error::ConnectorError;

/// Owns the engine thread and speaks to it over a channel.
pub struct EngineHandle {
    tx: mpsc::UnboundedSender<Command>,
    join: Option<JoinHandle<()>>,
    shared: EngineShared,
    metrics: Arc<MetricsRegistry>,
}

impl EngineHandle {
    /// Spawn the engine thread and start the pipeline.
    ///
    /// Blocks until the engine reports startup success or failure, so setup
    /// errors surface here instead of later as a stopped engine.
    pub fn start(pipeline: Pipeline) -> Result<Self, ConnectorError> {
        // Reject unsupported sink wiring before spawning the engine thread, so
        // no writer, source stream or tap starts for a pipeline that cannot run.
        pipeline.validate()?;
        let (tx, rx) = mpsc::unbounded_channel();
        let shared = EngineShared {
            last_error: Arc::new(Mutex::new(None)),
            checkpoint_error: Arc::new(Mutex::new(None)),
            close_error: Arc::new(Mutex::new(None)),
            built: Arc::new(AtomicBool::new(false)),
        };
        let engine_shared = shared.clone();
        let metrics = Arc::new(MetricsRegistry::new());
        let engine_metrics = Arc::clone(&metrics);
        let (ready_tx, ready_rx) = oneshot::channel();
        let join = std::thread::spawn(move || {
            engine::run(pipeline, rx, engine_shared, engine_metrics, ready_tx)
        });
        match ready_rx.blocking_recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = join.join();
                return Err(error);
            }
            Err(_) => {
                let _ = join.join();
                return Err(stopped());
            }
        }
        Ok(Self {
            tx,
            join: Some(join),
            shared,
            metrics,
        })
    }

    /// The registry shared with the engine core and sink tasks.
    pub fn metrics(&self) -> Arc<MetricsRegistry> {
        Arc::clone(&self.metrics)
    }

    /// First source/ingestion error that stopped the source branch, if any.
    pub fn last_error(&self) -> Result<Option<String>, ConnectorError> {
        self.snapshot_handle().last_error()
    }

    /// First periodic-checkpoint failure, kept separate from source errors.
    pub fn checkpoint_error(&self) -> Result<Option<String>, ConnectorError> {
        self.shared
            .checkpoint_error
            .lock()
            .map(|slot| slot.clone())
            .map_err(|_| ConnectorError::Infrastructure("checkpoint error state poisoned".into()))
    }

    /// Read the consolidated output of a view.
    ///
    /// Blocks on `blocking_recv`, so it must not be called from within an async
    /// runtime (it would panic or stall the executor).
    pub fn snapshot(&self, view: &str) -> Result<ZSetBatch, ConnectorError> {
        self.snapshot_handle().snapshot(view)
    }

    /// Derive a cloneable handle that can read snapshots without owning the
    /// engine thread.
    pub fn snapshot_handle(&self) -> SnapshotHandle {
        SnapshotHandle {
            tx: self.tx.clone(),
            last_error: Arc::clone(&self.shared.last_error),
            built: Arc::clone(&self.shared.built),
        }
    }

    /// Events dropped as late in `input`.
    ///
    /// Blocks on `blocking_recv`, so it must not be called from within an async
    /// runtime (it would panic or stall the executor).
    pub fn late_dropped(&self, input: &str) -> Result<u64, ConnectorError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::LateDropped {
                input: input.to_string(),
                reply,
            })
            .map_err(|_| stopped())?;
        rx.blocking_recv().map_err(|_| stopped())?
    }

    /// Take a checkpoint now and return its id.
    ///
    /// Blocks on `blocking_recv`, so it must not be called from within an async
    /// runtime (it would panic or stall the executor).
    pub fn checkpoint(&self) -> Result<u64, ConnectorError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Checkpoint { reply })
            .map_err(|_| stopped())?;
        rx.blocking_recv().map_err(|_| stopped())?
    }

    /// Build a view after `START`, replaying the retained inputs.
    ///
    /// Blocks on `blocking_recv`, so it must not be called from within an async
    /// runtime (it would panic or stall the executor).
    pub fn build_view(&self, view: &str, plan: hotlap::Plan) -> Result<(), ConnectorError> {
        self.snapshot_handle().build_view(view, plan)
    }

    /// Stop the engine and join its thread, surfacing any close failure.
    pub fn shutdown(mut self) -> Result<(), ConnectorError> {
        let joined = self.stop();
        let closed = self.take_close_error();
        joined.and(closed)
    }

    /// Ask the engine to stop and join its thread, if still running.
    ///
    /// The reply is deliberately not awaited: `blocking_recv` panics inside a
    /// tokio executor, so `Drop` could not use it. Joining the engine thread
    /// still blocks until the engine has processed the command and exited, and
    /// `thread::join` is safe to call from within a runtime.
    ///
    /// A panic in the engine thread surfaces here instead of being swallowed.
    fn stop(&mut self) -> Result<(), ConnectorError> {
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        let (reply, _rx) = oneshot::channel();
        let _ = self.tx.send(Command::Shutdown { reply });
        match join.join() {
            Ok(()) => Ok(()),
            Err(_) => Err(worker_panicked()),
        }
    }

    /// Take the sink/close failure the engine recorded, if any.
    fn take_close_error(&self) -> Result<(), ConnectorError> {
        let mut slot = self.shared.close_error.lock().map_err(|_| {
            ConnectorError::Infrastructure("shutdown error state poisoned".into())
        })?;
        match slot.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for EngineHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub(super) fn stopped() -> ConnectorError {
    ConnectorError::Infrastructure("engine thread stopped".into())
}

fn worker_panicked() -> ConnectorError {
    ConnectorError::Infrastructure("engine thread panicked".into())
}
