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
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::source_checkpoint::{decode_sources, encode_sources};
use hotlap_runtime::runtime::sources::InputStream;

use backend::SharedBackend;
use pending::{copy_to_pending, counting, engine_with, seed};

/// Run recovery against an incompatible pending and count sink commits.
fn run_incompatible(
    backend: &SharedBackend,
    pipe: &Pipeline,
) -> (Result<InputStream, ConnectorError>, Arc<AtomicU32>) {
    let (sink, commits) = counting(true);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![sink]);
    let mut hotlap = engine_with(pipe);
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

/// Rewrite the pending `sources` body with `mutate` applied.
fn rewrite_pending(backend: &SharedBackend, pending: u64, mutate: impl FnOnce(&mut Vec<u8>)) {
    let mut writer = backend.clone();
    let key = format!("checkpoint/{pending}/sources");
    let mut raw = writer.get(key.as_bytes()).unwrap().unwrap();
    mutate(&mut raw);
    writer.put(key.as_bytes(), raw).unwrap();
}

#[test]
fn a_pending_commit_with_a_changed_schema_errors_before_any_commit() {
    let backend = SharedBackend::default();
    let (pipe, valid) = seed(&backend, 3);
    let pending = copy_to_pending(&backend, valid);
    rewrite_pending(&backend, pending, |raw| {
        let mut saved = decode_sources(raw).unwrap();
        let changed = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        saved.entries[0].schema = encode_schema(&changed).unwrap();
        *raw = encode_sources(&saved).unwrap();
    });

    let (result, commits) = run_incompatible(&backend, &pipe);
    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not commit");
    assert!(
        backend
            .get(format!("checkpoint/{pending}/valid").as_bytes())
            .unwrap()
            .is_none()
    );
}

#[test]
fn a_pending_commit_in_a_foreign_format_is_not_decoded_or_promoted() {
    let backend = SharedBackend::default();
    let (pipe, valid) = seed(&backend, 3);
    let pending = copy_to_pending(&backend, valid);
    rewrite_pending(&backend, pending, |raw| {
        *raw = encode_framed(&SourceState::default()).unwrap();
    });

    let (result, commits) = run_incompatible(&backend, &pipe);
    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not re-drive");
    assert!(
        backend
            .get(format!("checkpoint/{pending}/valid").as_bytes())
            .unwrap()
            .is_none()
    );
}
