//! Shutdown must interrupt a sink pump blocked by backpressure.
//!
//! The engine is stopped while the pump is parked on a full channel. The
//! shutdown call runs on its own OS thread and is bounded by an independent
//! watchdog, never by a timer around the blocking call on the same thread.

use std::sync::atomic::Ordering;
use std::time::Duration;

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
    let (entered, signal) = Signal::new();
    let sink = StallingSink::new(entered);
    let handle = harness::start(
        harness::many(300),
        sink.clone(),
        Some(harness::checkpoint()),
    );
    signal
        .recv_timeout(SETUP)
        .expect("the sink never parked in write");

    let result = with_watchdog(move || handle.shutdown());
    assert!(
        result.is_err(),
        "a sink stalled behind a full channel must fail shutdown, got {result:?}"
    );
    assert_eq!(
        sink.writes.load(Ordering::SeqCst),
        1,
        "the aborted sink must not keep writing past shutdown"
    );
}
