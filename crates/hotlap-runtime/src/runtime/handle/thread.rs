//! Engine-thread lifecycle for [`EngineHandle`](super::EngineHandle).

use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use hotlap_engine::MetricsRegistry;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

use crate::runtime::command::Command;
use crate::runtime::engine::{self, EngineShared};
use crate::runtime::pipeline::Pipeline;
use hotlap_connectors::error::ConnectorError;

/// Bound the caller waits for the engine thread to finish after `Shutdown`.
///
/// The worker runs on its own current-thread runtime, so a sink stuck in
/// non-yielding work can stop it from ever polling the shutdown command or a
/// timer. This bound lives on the calling thread, independent of that runtime.
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Signals that the engine thread has returned, even if it panicked.
struct Done(mpsc::Sender<()>);

impl Drop for Done {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// Spawn the engine thread; return its join handle, completion signal and the
/// startup handshake the caller blocks on.
pub(super) fn spawn(
    pipeline: Pipeline,
    rx: tokio_mpsc::UnboundedReceiver<Command>,
    shared: EngineShared,
    metrics: Arc<MetricsRegistry>,
) -> (
    JoinHandle<()>,
    mpsc::Receiver<()>,
    oneshot::Receiver<Result<(), ConnectorError>>,
) {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (done_tx, done) = mpsc::channel();
    let join = std::thread::spawn(move || {
        let _done = Done(done_tx);
        engine::run(pipeline, rx, shared, metrics, ready_tx);
    });
    (join, done, ready_rx)
}

/// Wait for the engine thread to finish, bounded on the caller's side.
///
/// A panic in the thread surfaces as an error. A worker that does not finish
/// within the bound is detached and reported, never treated as success: it may
/// still be running non-cancellable work, so no delivery can be certified.
pub(super) fn wait(join: JoinHandle<()>, done: &mpsc::Receiver<()>) -> Result<(), ConnectorError> {
    match done.recv_timeout(SHUTDOWN_JOIN_TIMEOUT) {
        Ok(()) => match join.join() {
            Ok(()) => Ok(()),
            Err(_) => Err(worker_panicked()),
        },
        Err(_) => Err(join_timed_out()),
    }
}

fn worker_panicked() -> ConnectorError {
    ConnectorError::Infrastructure("engine thread panicked".into())
}

fn join_timed_out() -> ConnectorError {
    ConnectorError::Infrastructure(
        "engine thread did not stop within the shutdown timeout; it was detached".into(),
    )
}
