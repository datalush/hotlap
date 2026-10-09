//! An unknown inner engine-frame version is a fatal `Unsupported` at the
//! recovery boundary, never a tolerated corruption that falls back to an older
//! checkpoint or starts clean. The outer `HLSR` container stays valid and only
//! the inner frame version changes, so the decode must classify it as an
//! incompatible format rather than undecodable corruption.

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
use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sources::{InputSource, InputStream, Sources};

use backend::SharedBackend;
use pending::{
    counting, engine_with, fresh_pipeline, join_pipeline, log_a, log_b, put_sources, seed_pair,
    sources_bytes,
};
use pending_reads::{ReadStartSource, Reads, read_starts};

/// Overwrite a source envelope's inner engine-frame version, keeping `HLSR`.
fn corrupt_sources_version(bytes: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out[12..16].copy_from_slice(&999u32.to_le_bytes());
    out
}

/// Overwrite a bare engine snapshot frame's version (no `HLSR` container).
fn corrupt_engine_version(bytes: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out[4..8].copy_from_slice(&999u32.to_le_bytes());
    out
}

fn storage_key(id: u64, part: &str) -> Vec<u8> {
    format!("checkpoint/{id}/{part}").into_bytes()
}

fn put_engine(backend: &SharedBackend, id: u64, bytes: Vec<u8>) {
    backend
        .clone()
        .put(&storage_key(id, "engine"), bytes)
        .unwrap();
}

fn engine_bytes(backend: &SharedBackend, id: u64) -> Vec<u8> {
    backend
        .clone()
        .get(&storage_key(id, "engine"))
        .unwrap()
        .unwrap()
}

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

/// Publish `tip` as a newer valid checkpoint without a commit marker.
fn publish_as_valid(backend: &SharedBackend, tip: u64) {
    let mut writer = backend.clone();
    writer
        .put(&storage_key(tip, "valid"), b"1".to_vec())
        .unwrap();
    writer.delete(&storage_key(tip, "commit")).unwrap();
    writer
        .put(b"checkpoint/latest", tip.to_le_bytes().to_vec())
        .unwrap();
}

#[test]
fn an_unknown_inner_version_in_a_valid_tip_does_not_fall_back() {
    let backend = SharedBackend::default();
    let (older, tip) = seed_pair(&backend);
    publish_as_valid(&backend, tip);
    put_sources(
        &backend,
        tip,
        corrupt_sources_version(&sources_bytes(&backend, tip)),
    );

    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let result = Recovery::load(&checkpointer, &fresh_pipeline().sources);
    assert!(
        matches!(result, Err(ConnectorError::Unsupported(_))),
        "must not skip the incompatible tip to checkpoint {older}"
    );
}

#[test]
fn an_unknown_engine_frame_version_in_a_valid_tip_is_fatal() {
    let backend = SharedBackend::default();
    let (valid, _pending) = seed_pair(&backend);
    put_engine(
        &backend,
        valid,
        corrupt_engine_version(&engine_bytes(&backend, valid)),
    );

    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let error = Recovery::load(&checkpointer, &fresh_pipeline().sources).unwrap_err();
    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "an unknown engine frame version must not start clean: {error}"
    );
}

#[test]
fn an_unknown_inner_version_in_a_pending_body_is_fatal_before_any_read_or_commit() {
    let backend = SharedBackend::default();
    let (_valid, pending) = seed_pair(&backend);
    put_sources(
        &backend,
        pending,
        corrupt_sources_version(&sources_bytes(&backend, pending)),
    );

    let (pipe, a_reads, b_reads) = recording_pipeline();
    let (sink, commits): (_, Arc<AtomicU32>) = counting(true);
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
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not commit a sink");
    assert!(read_starts(&a_reads).is_empty(), "must not read source a");
    assert!(read_starts(&b_reads).is_empty(), "must not read source b");
    assert!(
        signal.lock().unwrap().is_none(),
        "must not discard or replay"
    );
    assert!(
        backend
            .get(&storage_key(pending, "valid"))
            .unwrap()
            .is_none(),
        "an incompatible pending must never be promoted"
    );
}
