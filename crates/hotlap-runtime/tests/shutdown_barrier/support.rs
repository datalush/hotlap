//! Helpers shared by the barrier-stall shutdown tests.

use std::time::{Duration, Instant};

use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::CheckpointConfig;
use hotlap_runtime::runtime::handle::EngineHandle;

use super::harness::{GatedCommitSink, SharedBackend, Signal, WatchedBackend, keys_with, start};

pub const WATCHDOG: Duration = Duration::from_secs(30);
pub const SETUP: Duration = Duration::from_secs(10);

/// Run `task` on its own OS thread, failing if it misses the watchdog.
pub fn with_watchdog<T: Send + 'static>(task: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(task());
    });
    rx.recv_timeout(WATCHDOG)
        .unwrap_or_else(|_| panic!("operation blocked past the {WATCHDOG:?} watchdog"))
}

/// A checkpoint config over a watched backend, with no periodic checkpoint.
pub fn config(backend: WatchedBackend) -> CheckpointConfig {
    CheckpointConfig {
        interval: Duration::from_secs(3600),
        backend: Box::new(backend),
        retain: 3,
    }
}

/// Start a manual checkpoint on its own thread and return its reply channel.
pub fn spawn_checkpoint(
    handle: &EngineHandle,
) -> std::sync::mpsc::Receiver<Result<u64, ConnectorError>> {
    let snap = handle.snapshot_handle();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(snap.checkpoint());
    });
    rx
}

/// Assert the durable state after a cancelled attempt.
pub fn assert_evidence(backend: &SharedBackend, committed: bool) {
    use hotlap::state::StateBackend;
    assert!(
        backend.get(b"checkpoint/reserved").unwrap().is_some(),
        "the reservation must be retained"
    );
    assert!(
        backend.get(b"checkpoint/1/valid").unwrap().is_none(),
        "a cancelled attempt must never publish valid"
    );
    assert!(
        backend.get(b"checkpoint/latest").unwrap().is_none(),
        "a cancelled attempt must never become latest"
    );
    assert_eq!(
        backend.get(b"checkpoint/1/engine").unwrap().is_some(),
        committed,
        "body presence"
    );
    assert_eq!(
        backend.get(b"checkpoint/1/commit").unwrap().is_some(),
        committed,
        "commit marker presence"
    );
}

/// Assert the marker written before an unacknowledged prepare, with no body.
pub fn assert_prepare_uncertain(backend: &SharedBackend) {
    use hotlap::state::StateBackend;
    assert!(backend.get(b"checkpoint/1/commit").unwrap().is_some());
    assert!(backend.get(b"checkpoint/1/engine").unwrap().is_none());
    assert!(backend.get(b"checkpoint/1/sources").unwrap().is_none());
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_none());
    assert!(backend.get(b"checkpoint/latest").unwrap().is_none());
}

/// Assert a bounded failing shutdown and a failing pending checkpoint caller.
pub fn assert_cancelled(
    result: Result<(), ConnectorError>,
    elapsed: Duration,
    reply: &std::sync::mpsc::Receiver<Result<u64, ConnectorError>>,
) {
    assert!(result.is_err(), "a cancelled checkpoint must fail shutdown");
    assert!(
        elapsed < Duration::from_secs(8),
        "the barrier await must be cancelled, not left to the caller timeout: {elapsed:?}"
    );
    assert!(
        reply
            .recv_timeout(Duration::from_secs(5))
            .expect("the pending checkpoint must not stay pending")
            .is_err(),
        "the interrupted checkpoint must fail its pending caller"
    );
}

/// Drive a manual checkpoint that parks in `commit`, cancel it, assert the
/// retained evidence and return the backend for a restart check.
pub fn commit_uncertain() -> SharedBackend {
    let (entered, entered_rx) = Signal::new();
    let (sink, _release) = GatedCommitSink::new(entered);
    let backend = SharedBackend::default();
    let watched = WatchedBackend::watch(backend.clone(), None, None);
    let handle = start(keys_with(&[1], None), sink, Some(config(watched)));

    let reply = spawn_checkpoint(&handle);
    entered_rx
        .recv_timeout(SETUP)
        .expect("commit never entered");
    let started = Instant::now();
    let result = with_watchdog(move || handle.shutdown());

    assert_cancelled(result, started.elapsed(), &reply);
    backend
}
