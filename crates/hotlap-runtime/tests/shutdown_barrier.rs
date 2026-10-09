//! Shutdown must cancel a checkpoint at each stall point of the real barrier.
//!
//! Each test drives the public runtime: a real engine, pump and checkpoint
//! barrier, an in-memory backend that reports its durable keys, and a sink that
//! parks at one specific SPI call. The state after cancellation is asserted
//! precisely, and every hang test is bounded by an independent OS watchdog.

use std::time::Instant;

use hotlap::InputId;
use hotlap_runtime::runtime::checkpoint::Checkpointer;
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

#[path = "shutdown_barrier/harness.rs"]
mod harness;
#[path = "shutdown_barrier/support.rs"]
mod support;

use harness::{
    GatedPrepareSink, ParkedWriteSink, SharedBackend, Signal, StagedSink, StagedStore,
    WatchedBackend, fill_queue, keys_with, start,
};
use support::{
    assert_cancelled, assert_evidence, commit_uncertain, config, spawn_checkpoint, with_watchdog,
};

#[test]
fn shutdown_cancels_a_checkpoint_blocked_in_the_flush_send() {
    let (entered, entered_rx) = Signal::new();
    let (last, last_rx) = Signal::new();
    let (reserved, reserved_rx) = Signal::new();
    let backend = SharedBackend::default();
    let watched = WatchedBackend::watch(backend.clone(), Some(reserved), None);
    let handle = start(
        fill_queue(last),
        ParkedWriteSink::new(entered),
        Some(config(watched)),
    );
    entered_rx
        .recv_timeout(support::SETUP)
        .expect("sink never parked");
    last_rx
        .recv_timeout(support::SETUP)
        .expect("queue never filled");

    let reply = spawn_checkpoint(&handle);
    reserved_rx
        .recv_timeout(support::SETUP)
        .expect("checkpoint never reserved");
    let started = Instant::now();
    let result = with_watchdog(move || handle.shutdown());

    assert_cancelled(result, started.elapsed(), &reply);
    assert_evidence(&backend, false);
}

#[test]
fn shutdown_cancels_a_checkpoint_blocked_in_the_flush_reply() {
    let (entered, entered_rx) = Signal::new();
    let (reserved, reserved_rx) = Signal::new();
    let backend = SharedBackend::default();
    let watched = WatchedBackend::watch(backend.clone(), Some(reserved), None);
    let handle = start(
        keys_with(&[1], None),
        ParkedWriteSink::new(entered),
        Some(config(watched)),
    );
    entered_rx
        .recv_timeout(support::SETUP)
        .expect("sink never parked");

    let reply = spawn_checkpoint(&handle);
    reserved_rx
        .recv_timeout(support::SETUP)
        .expect("checkpoint never reserved");
    let started = Instant::now();
    let result = with_watchdog(move || handle.shutdown());

    assert_cancelled(result, started.elapsed(), &reply);
    assert_evidence(&backend, false);
}

#[test]
fn shutdown_cancels_a_checkpoint_blocked_in_prepare() {
    let (entered, entered_rx) = Signal::new();
    let (sink, _release) = GatedPrepareSink::new(entered);
    let backend = SharedBackend::default();
    let watched = WatchedBackend::watch(backend.clone(), None, None);
    let handle = start(keys_with(&[1], None), sink, Some(config(watched)));

    let reply = spawn_checkpoint(&handle);
    entered_rx
        .recv_timeout(support::SETUP)
        .expect("prepare never entered");
    let started = Instant::now();
    let result = with_watchdog(move || handle.shutdown());

    assert_cancelled(result, started.elapsed(), &reply);
    assert_evidence(&backend, false);
}

#[test]
fn shutdown_cancels_a_checkpoint_blocked_in_commit() {
    let backend = commit_uncertain();
    assert_evidence(&backend, true);
}

#[test]
fn a_staged_transactional_commit_cancel_is_rejected_on_restart() {
    let store = StagedStore::new();
    let (entered, entered_rx) = Signal::new();
    let (written, written_rx) = Signal::new();
    let (sink, _release) = StagedSink::new(store.clone(), entered, written);
    let backend = SharedBackend::default();
    let watched = WatchedBackend::watch(backend.clone(), None, None);
    let handle = start(
        keys_with(&[7, 8], None),
        sink.clone(),
        Some(config(watched)),
    );
    for _ in 0..2 {
        written_rx
            .recv_timeout(support::SETUP)
            .expect("the sink never wrote a batch");
    }

    let reply = spawn_checkpoint(&handle);
    entered_rx
        .recv_timeout(support::SETUP)
        .expect("commit never entered");
    assert_eq!(
        store.staged(),
        vec![7, 8],
        "prepare must persist the real written payload"
    );
    let started = Instant::now();
    let result = with_watchdog(move || handle.shutdown());

    assert_cancelled(result, started.elapsed(), &reply);
    assert_evidence(&backend, true);
    assert!(
        store.published().is_empty(),
        "a cancelled commit must not publish the staged payload"
    );
    assert_eq!(
        store.staged(),
        vec![7, 8],
        "the staged payload must be retained"
    );

    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: keys_with(&[7, 8], None),
        watermark: None,
    }])
    .unwrap();
    let restarted = Checkpointer::new(Box::new(backend), 3)
        .with_sinks(vec![SinkSync::sink_only(SharedSink::new(sink))]);
    let decision = Recovery::inspect(&restarted, &sources).expect("inspect");
    assert!(
        matches!(decision, RecoveryDecision::Reject { .. }),
        "a non-re-drivable transactional sink must be explicitly rejected, got {decision:?}"
    );
    assert!(
        store.published().is_empty(),
        "a rejected commit must not have published"
    );
}
