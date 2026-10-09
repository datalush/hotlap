//! Final EOF commit must be bounded inside the engine worker itself.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hotlap_connectors::ConnectorError;

#[path = "shutdown_final_commit/harness.rs"]
mod harness;

use harness::{ObservedSink, Signal, StallingCommitSink};

const WATCHDOG: Duration = Duration::from_secs(30);
const SETUP: Duration = Duration::from_secs(10);

fn wait_shutdown(
    handle: hotlap_runtime::runtime::handle::EngineHandle,
    release: &tokio::sync::Notify,
) -> (Result<(), ConnectorError>, Duration) {
    let started = Instant::now();
    let (done, result_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(handle.shutdown());
    });
    let result = result_rx.recv_timeout(WATCHDOG).unwrap_or_else(|_| {
        release.notify_one();
        panic!("shutdown blocked past the {WATCHDOG:?} OS watchdog")
    });
    (result, started.elapsed())
}

#[test]
fn stalled_eof_commit_times_out_and_drops_its_worker_future() {
    let (written, written_rx) = Signal::new();
    let (entered, entered_rx) = Signal::new();
    let (resumed, resumed_rx) = Signal::new();
    let release = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let aborted = Arc::new(AtomicBool::new(false));
    let sink = StallingCommitSink::new(
        written,
        entered,
        Arc::clone(&release),
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
        .expect("sink never finished its EOF write");

    let (result, elapsed) = wait_shutdown(handle, &release);
    entered_rx
        .recv_timeout(SETUP)
        .expect("final commit never entered its parked state");
    release.notify_one();
    let resumed_after_shutdown = resumed_rx.recv_timeout(Duration::from_secs(1)).is_ok();

    assert!(matches!(
        &result,
        Err(ConnectorError::Infrastructure(message))
            if message == "final sink commit timed out during close"
    ));
    assert!(
        elapsed < Duration::from_secs(9)
            && dropped.load(Ordering::SeqCst)
            && !aborted.load(Ordering::SeqCst)
            && !resumed_after_shutdown,
        "final commit must stop at its own deadline, not the caller's 10s bound; \
         elapsed={elapsed:?}, dropped={}, aborted={}, resumed={resumed_after_shutdown}",
        dropped.load(Ordering::SeqCst),
        aborted.load(Ordering::SeqCst),
    );
}

#[test]
fn partial_eof_commit_does_not_continue_or_abort_later_sinks() {
    let (written, written_rx) = Signal::new();
    let (entered, entered_rx) = Signal::new();
    let (resumed, resumed_rx) = Signal::new();
    let release = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let aborted = Arc::new(AtomicBool::new(false));
    let first = StallingCommitSink::new(
        written,
        entered,
        Arc::clone(&release),
        resumed,
        Arc::clone(&dropped),
        Arc::clone(&aborted),
    );
    let second = ObservedSink::new();
    let handle = harness::start_pair(harness::keys_with(&[1], None), first, second.clone());
    written_rx
        .recv_timeout(SETUP)
        .expect("first sink never received the final batch");

    let (result, elapsed) = wait_shutdown(handle, &release);
    entered_rx
        .recv_timeout(SETUP)
        .expect("first final commit never entered");
    release.notify_one();
    let first_resumed = resumed_rx.recv_timeout(Duration::from_secs(1)).is_ok();

    assert!(
        result.is_err()
            && elapsed < Duration::from_secs(9)
            && !first_resumed
            && !aborted.load(Ordering::SeqCst)
            && !second.committed()
            && !second.aborted(),
        "partial EOF commit must stop under one deadline without aborting or \
         committing later sinks; result={result:?}, elapsed={elapsed:?}, \
         first_resumed={first_resumed}, first_aborted={}, second_committed={}, \
         second_aborted={}",
        aborted.load(Ordering::SeqCst),
        second.committed(),
        second.aborted(),
    );
}
