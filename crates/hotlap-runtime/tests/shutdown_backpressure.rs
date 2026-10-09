//! Shutdown must interrupt a sink pump blocked by backpressure.
//!
//! The engine is stopped while the pump is parked on a full channel. The test
//! waits for two deterministic signals, not a timer: the sink parked in `write`,
//! and the source reaching its final batch, which the engine can only do after
//! it filled the channel and parked its next send. The shutdown call runs on its
//! own OS thread, bounded by an independent watchdog.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[path = "shutdown_backpressure/harness.rs"]
mod harness;

use harness::{Signal, StallingSink};

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
fn shutdown_cancels_a_pump_blocked_by_backpressure() {
    let (entered, entered_rx) = Signal::new();
    let (last, last_rx) = Signal::new();
    let sink = StallingSink::new(entered);
    let handle = harness::start(
        harness::fill(last),
        sink.clone(),
        Some(harness::checkpoint()),
    );
    entered_rx
        .recv_timeout(SETUP)
        .expect("the sink never parked in write");
    last_rx
        .recv_timeout(SETUP)
        .expect("the pump never filled the channel and blocked");

    let started = Instant::now();
    let result = with_watchdog(move || handle.shutdown());
    let elapsed = started.elapsed();
    assert!(
        result.is_err(),
        "a sink stalled behind a full channel must fail shutdown, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(8),
        "the pump must be cancelled, not left to the caller timeout: took {elapsed:?}"
    );
    assert_eq!(
        sink.writes.load(Ordering::SeqCst),
        1,
        "the aborted sink must not keep writing past shutdown"
    );
}
