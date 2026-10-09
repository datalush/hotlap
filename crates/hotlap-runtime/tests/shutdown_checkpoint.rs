//! Shutdown must cancel a checkpoint parked on a stalled sink.
//!
//! Both the periodic tick and a manual `checkpoint()` call run the same barrier
//! awaits; a cancellation must abandon them, mark the attempt inconsistent, keep
//! the durable evidence and never report success.

use std::time::{Duration, Instant};

use hotlap::state::StateBackend;
use hotlap_runtime::runtime::checkpoint::Checkpointer;

#[path = "shutdown_checkpoint/harness.rs"]
mod harness;

use harness::{
    GatedCommitSink, SharedBackend, Signal, checkpoint, checkpoint_with, keys_with, start,
};

const WATCHDOG: Duration = Duration::from_secs(30);
const SETUP: Duration = Duration::from_secs(10);

fn with_watchdog<T: Send + 'static>(task: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(task());
    });
    rx.recv_timeout(WATCHDOG)
        .unwrap_or_else(|_| panic!("operation blocked past the {WATCHDOG:?} watchdog"))
}

#[test]
fn shutdown_cancels_a_periodic_checkpoint_and_keeps_markers() {
    let backend = SharedBackend::default();
    let interval = Duration::from_millis(20);
    let (entered, entered_rx) = Signal::new();
    let (sink, _release) = GatedCommitSink::new(entered);
    let handle = start(
        keys_with(&[1], None),
        sink,
        Some(checkpoint_with(backend.clone(), interval)),
    );
    entered_rx
        .recv_timeout(SETUP)
        .expect("the periodic checkpoint never reached commit");

    let started = Instant::now();
    let result = with_watchdog(move || handle.shutdown());
    let elapsed = started.elapsed();
    assert!(
        result.is_err(),
        "an interrupted checkpoint must fail shutdown, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(8),
        "the checkpoint await must be cancelled, not left to the caller timeout: took {elapsed:?}"
    );
    assert!(
        backend.get(b"checkpoint/1/commit").unwrap().is_some(),
        "the commit marker must be retained for recovery"
    );
    assert!(
        backend.get(b"checkpoint/1/valid").unwrap().is_none(),
        "an interrupted checkpoint must not be published"
    );

    let fresh = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(
        fresh.latest().unwrap(),
        None,
        "an interrupted checkpoint must not become latest"
    );
    assert!(
        fresh.ids_descending().unwrap().contains(&1),
        "the reserved id must survive for a restart to resolve"
    );
}

#[test]
fn shutdown_cancels_a_manual_checkpoint_and_fails_the_caller() {
    let (entered, entered_rx) = Signal::new();
    let (sink, _release) = GatedCommitSink::new(entered);
    let handle = start(keys_with(&[1], None), sink, Some(checkpoint()));
    let snap = handle.snapshot_handle();
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = reply_tx.send(snap.checkpoint());
    });
    entered_rx
        .recv_timeout(SETUP)
        .expect("the manual checkpoint never reached commit");

    let started = Instant::now();
    let result = with_watchdog(move || handle.shutdown());
    let elapsed = started.elapsed();
    assert!(
        result.is_err(),
        "shutdown must fail after interrupting a checkpoint, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(8),
        "the checkpoint await must be cancelled, not left to the caller timeout: took {elapsed:?}"
    );
    let reply = reply_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the interrupted checkpoint must not stay pending");
    assert!(
        reply.is_err(),
        "the interrupted checkpoint must fail its pending caller"
    );
}
