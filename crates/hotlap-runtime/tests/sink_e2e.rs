use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[path = "sink_e2e/harness.rs"]
mod harness;

use harness::{FakeSink, batch, sentinel, start, zset_rows};

/// Wait until both the engine and the sink have observed the sentinel row.
fn wait_converged(
    handle: &hotlap_runtime::runtime::handle::EngineHandle,
    sink: &FakeSink,
) -> Vec<Vec<i64>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rows = handle
            .snapshot("c")
            .map(|z| zset_rows(&z))
            .unwrap_or_default();
        if rows.contains(&sentinel()) && sink.consolidated().contains_key(&sentinel()) {
            return rows;
        }
        assert!(Instant::now() < deadline, "sink/engine never converged");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn sink_consolidated_state_matches_snapshot() {
    let batches = vec![
        batch(&[1, 1, 2], &[10, 10, 10]),
        batch(&[2, 3], &[20, 20]),
        batch(&[999], &[99]),
    ];
    let (handle, sink) = start(batches, FakeSink::new(None));
    let rows = wait_converged(&handle, &sink);
    let expected: BTreeMap<Vec<i64>, i64> = rows.into_iter().map(|r| (r, 1)).collect();
    assert_eq!(sink.consolidated(), expected);
    handle.shutdown().unwrap();
}

#[test]
fn backpressure_loses_no_batch() {
    let mut batches: Vec<_> = (0..100).map(|i| batch(&[i as i64], &[i as i64])).collect();
    batches.push(batch(&[999], &[999]));
    let (handle, sink) = start(batches, FakeSink::new(Some(Duration::from_millis(1))));
    wait_converged(&handle, &sink);
    handle.shutdown().unwrap();
    assert_eq!(sink.len(), 101, "batch lost");
}

#[test]
fn shutdown_delivers_the_last_changelog() {
    let batches = vec![
        batch(&[1, 2], &[10, 10]),
        batch(&[3], &[20]),
        batch(&[999], &[99]),
    ];
    let (handle, sink) = start(batches, FakeSink::new(None));
    wait_converged(&handle, &sink);
    handle.shutdown().unwrap();
    assert_eq!(sink.len(), 3, "the sink missed a changelog batch");
}

#[test]
fn commit_runs_after_the_stream_ends() {
    let batches = vec![batch(&[1], &[10]), batch(&[999], &[99])];
    let (handle, sink) = start(batches, FakeSink::new(None));
    wait_converged(&handle, &sink);
    assert!(!sink.committed(), "commit must wait for the stream to end");
    assert!(!sink.aborted(), "a healthy run must not abort");
    handle.shutdown().unwrap();
    assert!(sink.committed(), "commit must run once the stream ends");
    assert!(!sink.aborted(), "a healthy run must not abort");
}

#[test]
fn metrics_count_sink_commits() {
    let batches = vec![batch(&[1, 1], &[10, 10]), batch(&[999], &[99])];
    let (handle, sink) = start(batches, FakeSink::new(None));
    wait_converged(&handle, &sink);

    let metrics = handle.metrics();
    handle.shutdown().unwrap();
    assert!(
        metrics
            .snapshot()
            .get("sinks_committed")
            .copied()
            .unwrap_or(0)
            >= 1,
        "a completed sink must be counted as committed"
    );
}
