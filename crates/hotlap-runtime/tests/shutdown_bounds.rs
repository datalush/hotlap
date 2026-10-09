//! Bounded, observable engine shutdown under backpressure and failure.
//!
//! Every test that can hang protects itself with an independent OS thread and a
//! blocking receive timeout, never with a timer around the blocking call on the
//! same thread.

use std::sync::Arc;
use std::time::Duration;

#[path = "shutdown_bounds/harness.rs"]
mod harness;

use harness::{FailingCommitSink, PanickingSink, PanickingSource, RecordingSink, Signal};

/// Generous ceiling: the runtime's own close timeout is far smaller.
const WATCHDOG: Duration = Duration::from_secs(30);
/// Ceiling for a fixture to reach its deterministic signal.
const SETUP: Duration = Duration::from_secs(10);

/// Run `task` on its own OS thread and fail the test if it never finishes.
fn with_watchdog<T: Send + 'static>(task: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(task());
    });
    rx.recv_timeout(WATCHDOG)
        .unwrap_or_else(|_| panic!("operation blocked past the {WATCHDOG:?} watchdog"))
}

/// Drain `count` deterministic signals, failing if the fixture never fires.
fn drain(rx: &std::sync::mpsc::Receiver<()>, count: usize) {
    for _ in 0..count {
        rx.recv_timeout(SETUP)
            .unwrap_or_else(|_| panic!("fixture never reached its signal"));
    }
}

#[test]
fn shutdown_with_checkpointing_closes_and_commits() {
    let (written, signal) = Signal::new();
    let sink = RecordingSink::new(written);
    let handle = harness::start(
        harness::keys(&[1, 2, 3]),
        sink.clone(),
        Some(harness::checkpoint()),
    );
    drain(&signal, 3);
    assert_eq!(sink.rows(), 3, "the sink missed a changelog batch");

    let result = with_watchdog(move || handle.shutdown());
    assert!(result.is_ok(), "a clean shutdown must succeed: {result:?}");
    assert!(
        sink.committed(),
        "the final commit must run once the changelog ends"
    );
}

#[test]
fn a_failed_final_commit_fails_shutdown() {
    let (written, signal) = Signal::new();
    let handle = harness::start(
        harness::keys(&[1, 2, 3]),
        FailingCommitSink::new(written),
        Some(harness::checkpoint()),
    );
    drain(&signal, 3);

    let result = with_watchdog(move || handle.shutdown());
    assert!(
        result.is_err(),
        "a failed final commit must not report success, got {result:?}"
    );
}

#[test]
fn a_sink_task_panic_fails_shutdown() {
    let handle = harness::start(harness::keys(&[1]), Arc::new(PanickingSink), None);
    let result = with_watchdog(move || handle.shutdown());
    assert!(
        result.is_err(),
        "a panicked sink task must not report success, got {result:?}"
    );
}

#[test]
fn an_engine_worker_panic_fails_shutdown() {
    let (poisoned, signal) = Signal::new();
    let (written, written_rx) = Signal::new();
    let handle = harness::start(
        PanickingSource::new(poisoned),
        RecordingSink::new(written),
        None,
    );
    signal
        .recv_timeout(SETUP)
        .expect("the engine never polled the panicking source");

    let result = with_watchdog(move || handle.shutdown());
    drop(written_rx);
    assert!(
        result.is_err(),
        "a panicked engine thread must not report success, got {result:?}"
    );
}
