//! Pending recovery opens each source at the offset saved in the checkpoint.
//!
//! The sources here record the `split.start` of every `read`, independently of
//! the committed state the checkpoint seeds, so a promoted pending at EOF and a
//! discarded pending both prove the actual restart offsets. A saved offset past
//! the end of the dataset must fail recovery rather than silently read nothing.

#[path = "common/backend.rs"]
mod backend;
#[path = "cross_source_support/pending.rs"]
mod pending;
#[path = "cross_source_support/pending_reads.rs"]
mod pending_reads;
#[path = "common/recovery/resumable.rs"]
mod resumable;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use hotlap::InputId;
use hotlap_connectors::error::ConnectorError;
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::source_checkpoint::{decode_sources, encode_sources};
use hotlap_runtime::runtime::sources::{InputSource, InputStream, Sources};

use backend::SharedBackend;
use pending::{
    counting, engine_with, join_pipeline, log_a, log_b, put_sources, seed_pair_with_redriable,
    sources_bytes,
};
use pending_reads::{ReadStartSource, Reads, read_starts};

/// Two recording sources `a` (id 0) and `b` (id 1) joined on `k`.
fn recording_pipeline() -> (Pipeline, Reads, Reads) {
    let (a, a_reads) = ReadStartSource::new(log_a());
    let (b, b_reads) = ReadStartSource::new(log_b());
    let sources = Sources::new(vec![
        InputSource {
            id: InputId(0),
            name: "a".into(),
            source: a,
            watermark: None,
        },
        InputSource {
            id: InputId(1),
            name: "b".into(),
            source: b,
            watermark: None,
        },
    ])
    .unwrap();
    (join_pipeline(sources), a_reads, b_reads)
}

/// The offsets saved for `a` and `b` in checkpoint `id`.
fn saved_offsets(backend: &SharedBackend, id: u64) -> (i64, i64) {
    let sources = decode_sources(&sources_bytes(backend, id)).unwrap();
    let a = sources.entry(InputId(0)).unwrap().state.offsets[&0];
    let b = sources.entry(InputId(1)).unwrap().state.offsets[&0];
    (a, b)
}

/// Start recovery over recording sources; returns commits and the warning.
fn start(backend: &SharedBackend, pipe: &Pipeline, redriable: bool) -> (Arc<AtomicU32>, String) {
    let (sink, commits) = counting(redriable);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![sink]);
    let mut hotlap = engine_with(pipe);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();
    let _stream: InputStream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .unwrap();
    let warning = signal.lock().unwrap().clone().unwrap_or_default();
    (commits, warning)
}

#[test]
fn a_promoted_pending_reads_each_source_at_its_saved_offset() {
    let backend = SharedBackend::default();
    let (_valid, pending) = seed_pair_with_redriable(&backend, true);
    let (pipe, a_reads, b_reads) = recording_pipeline();

    let (commits, warning) = start(&backend, &pipe, true);
    assert_eq!(commits.load(Ordering::SeqCst), 1, "commit re-driven");
    assert!(warning.is_empty(), "a promotion is not a discard");

    // The pending is at EOF, so the read must still start exactly at the saved
    // offsets rather than at the committed state or at zero.
    let (a, b) = saved_offsets(&backend, pending);
    assert_eq!(read_starts(&a_reads), vec![(0, a)]);
    assert_eq!(read_starts(&b_reads), vec![(0, b)]);
}

#[test]
fn a_discarded_pending_reads_both_sources_at_the_previous_offsets() {
    let backend = SharedBackend::default();
    let (valid, _pending) = seed_pair_with_redriable(&backend, false);
    let (pipe, a_reads, b_reads) = recording_pipeline();

    let (commits, warning) = start(&backend, &pipe, false);
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not re-drive");
    assert!(warning.contains("not re-drivable"), "reason: {warning}");

    let (a, b) = saved_offsets(&backend, valid);
    assert_eq!(read_starts(&a_reads), vec![(0, a)]);
    assert_eq!(read_starts(&b_reads), vec![(0, b)]);
}

#[test]
fn a_saved_offset_past_the_end_fails_recovery() {
    let backend = SharedBackend::default();
    let (valid, pending) = seed_pair_with_redriable(&backend, true);
    // Drop the pending so recovery resumes the valid body directly (no promote),
    // then corrupt its saved `a` offset past the end of the dataset.
    let (sink, _) = counting(true);
    Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![sink])
        .discard_commit(pending)
        .unwrap();
    let mut saved = decode_sources(&sources_bytes(&backend, valid)).unwrap();
    saved.entries[0].state.offsets.insert(0, 99);
    put_sources(&backend, valid, encode_sources(&saved).unwrap());

    let (pipe, a_reads, _b_reads) = recording_pipeline();
    let (sink, commits) = counting(true);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![sink]);
    let mut hotlap = engine_with(&pipe);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();
    let result: Result<InputStream, ConnectorError> = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ));

    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_eq!(read_starts(&a_reads), vec![(0, 99)]);
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not commit");
}
