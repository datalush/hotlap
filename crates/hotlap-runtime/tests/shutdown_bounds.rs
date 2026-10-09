//! Bounded, observable engine shutdown under backpressure and failure.
//!
//! Every test that can hang protects itself with an independent OS thread and a
//! blocking receive timeout, never with a timer around the blocking call on the
//! same thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hotlap_connectors::ConnectorError;

#[path = "shutdown_bounds/harness.rs"]
mod harness;

use harness::{
    BlockingSink, FailingCommitSink, PanickingSink, PanickingSource, RecordingSink, Signal,
    StallingCommitSink,
};

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
        harness::keys_with(&[1, 2, 3], None),
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
        harness::keys_with(&[1, 2, 3], None),
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
fn a_stalled_final_commit_is_bounded_and_its_future_is_cancelled() {
    let (written, written_rx) = Signal::new();
    let (entered, entered_rx) = Signal::new();
    let (resumed, resumed_rx) = Signal::new();
    let released = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let aborted = Arc::new(AtomicBool::new(false));
    let sink = StallingCommitSink::new(
        written,
        entered,
        Arc::clone(&released),
        resumed,
        Arc::clone(&dropped),
        Arc::clone(&aborted),
    );
    let handle = harness::start(
        harness::keys_with(&[1], None),
        sink,
        Some(harness::checkpoint()),
    );
    written_rx
        .recv_timeout(SETUP)
        .expect("the sink never finished its EOF write");
    let started = Instant::now();
    let (done, result_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(handle.shutdown());
    });
    entered_rx
        .recv_timeout(SETUP)
        .expect("the final commit never entered its parked state");
    let result = result_rx.recv_timeout(WATCHDOG).unwrap_or_else(|_| {
        released.notify_one();
        panic!("shutdown blocked past the {WATCHDOG:?} watchdog")
    });
    let elapsed = started.elapsed();
    released.notify_one();
    let resumed_after_shutdown = resumed_rx.recv_timeout(Duration::from_secs(1)).is_ok();

    assert!(
        matches!(
            &result,
            Err(ConnectorError::Infrastructure(message))
                if message == "final sink commit timed out during close"
        ),
        "a stalled final commit must return its typed timeout error: {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(9)
            && dropped.load(Ordering::SeqCst)
            && !aborted.load(Ordering::SeqCst)
            && !resumed_after_shutdown,
        "final-commit worker must stop at its own deadline, before the caller's \
         10s bound; elapsed={elapsed:?}, future_dropped={}, aborted={}, \
         resumed_after_shutdown={resumed_after_shutdown}",
        dropped.load(Ordering::SeqCst),
        aborted.load(Ordering::SeqCst),
    );
}

#[test]
fn a_stalled_commit_does_not_abort_or_commit_later_sinks() {
    let (written, written_rx) = Signal::new();
    let (entered, entered_rx) = Signal::new();
    let (resumed, resumed_rx) = Signal::new();
    let released = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let aborted = Arc::new(AtomicBool::new(false));
    let first = StallingCommitSink::new(
        written,
        entered,
        Arc::clone(&released),
        resumed,
        Arc::clone(&dropped),
        Arc::clone(&aborted),
    );
    let second = RecordingSink::new(Signal::new().0);
    let handle = harness::start_pair(harness::keys_with(&[1], None), first, second.clone());
    written_rx
        .recv_timeout(SETUP)
        .expect("the first sink never received the final batch");

    let started = Instant::now();
    let (done, result_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(handle.shutdown());
    });
    entered_rx
        .recv_timeout(SETUP)
        .expect("the first final commit never entered");
    let result = result_rx.recv_timeout(WATCHDOG).unwrap_or_else(|_| {
        released.notify_one();
        panic!("shutdown blocked past the {WATCHDOG:?} watchdog")
    });
    let elapsed = started.elapsed();
    released.notify_one();
    let first_resumed = resumed_rx.recv_timeout(Duration::from_secs(1)).is_ok();

    assert!(
        result.is_err()
            && elapsed < Duration::from_secs(9)
            && !first_resumed
            && !aborted.load(Ordering::SeqCst)
            && !second.committed()
            && !second.aborted(),
        "partial EOF commit must time out without continuing, committing, or aborting \
         later sinks; result={result:?}, elapsed={elapsed:?}, first_resumed={first_resumed}, \
         first_aborted={}, \
         second_committed={}, second_aborted={}",
        aborted.load(Ordering::SeqCst),
        second.committed(),
        second.aborted(),
    );
}

#[test]
fn a_sink_task_panic_fails_shutdown() {
    let handle = harness::start(
        harness::keys_with(&[1], None),
        Arc::new(PanickingSink),
        None,
    );
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

#[test]
fn a_nonyield_sink_bounds_the_caller_join() {
    let (entered, signal) = Signal::new();
    let (sink, release) = BlockingSink::new(entered);
    let handle = harness::start(harness::keys_with(&[1], None), sink, None);
    signal
        .recv_timeout(SETUP)
        .expect("the sink never entered a non-yielding write");

    let started = Instant::now();
    let result = with_watchdog(move || handle.shutdown());
    let elapsed = started.elapsed();
    assert!(
        result.is_err(),
        "a worker wedged in non-yielding work must not report success, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "the caller must return at its own bound, took {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_secs(8),
        "a non-yielding worker must fall back to the caller bound, took {elapsed:?}"
    );
    release
        .send(())
        .expect("release the wedged sink so the worker can exit");
}
