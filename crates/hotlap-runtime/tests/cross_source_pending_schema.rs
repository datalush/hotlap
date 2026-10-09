//! A pending commit whose registry is incompatible must fail before any sink
//! commit, for a changed source schema and for a foreign (previous) format.

#[path = "common/backend.rs"]
mod backend;
#[path = "cross_source_support/pending.rs"]
mod pending;
#[path = "common/recovery/resumable.rs"]
mod resumable;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema};
use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::SourceState;
use hotlap_engine::{MetricsRegistry, encode_framed, encode_schema};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::source_checkpoint::{decode_sources, encode_sources};
use hotlap_runtime::runtime::sources::InputStream;

use backend::SharedBackend;
use pending::{counting, engine_with, fresh_pipeline, put_sources, seed_pair, sources_bytes};

/// Run recovery against an incompatible pending and count sink commits.
fn run_incompatible(
    backend: &SharedBackend,
) -> (Result<InputStream, ConnectorError>, Arc<AtomicU32>) {
    let (sink, commits) = counting(true);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![sink]);
    let pipe = fresh_pipeline();
    let mut hotlap = engine_with(&pipe);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();
    let result = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ));
    (result, commits)
}

/// Assert the pending `pending` was never published and no sink committed.
fn assert_not_promoted(backend: &SharedBackend, pending: u64, commits: &Arc<AtomicU32>) {
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not commit");
    assert!(
        backend
            .get(format!("checkpoint/{pending}/valid").as_bytes())
            .unwrap()
            .is_none()
    );
}

#[test]
fn a_pending_commit_with_a_changed_schema_errors_before_any_commit() {
    let backend = SharedBackend::default();
    let (_valid, pending) = seed_pair(&backend);
    let mut saved = decode_sources(&sources_bytes(&backend, pending)).unwrap();
    let changed = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    saved.entries[0].schema = encode_schema(&changed).unwrap();
    put_sources(&backend, pending, encode_sources(&saved).unwrap());

    let (result, commits) = run_incompatible(&backend);
    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_not_promoted(&backend, pending, &commits);
}

#[test]
fn a_pending_commit_in_a_foreign_format_is_not_decoded_or_promoted() {
    let backend = SharedBackend::default();
    let (_valid, pending) = seed_pair(&backend);
    put_sources(
        &backend,
        pending,
        encode_framed(&SourceState::default()).unwrap(),
    );

    let (result, commits) = run_incompatible(&backend);
    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_not_promoted(&backend, pending, &commits);
}
