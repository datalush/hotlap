//! Recovery around a pending commit body: tolerating corruption, incomplete
//! bodies and stale markers without aborting startup.

#[path = "common/recovery.rs"]
mod recovery;

use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline as runtime;
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, take};

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

/// A sink that only declares its capability, exercising the built-in default.
struct CapabilitySink(SinkCapabilities);

#[async_trait::async_trait]
impl Sink for CapabilitySink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        self.0
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn capability(capabilities: SinkCapabilities) -> Arc<SharedSink> {
    SharedSink::new(Arc::new(CapabilitySink(capabilities)))
}

/// Persist a valid checkpoint 1 over a fresh shared backend.
fn seed_valid_one() -> SharedBackend {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = runtime::merged_stream(pipe.source.as_ref()).unwrap();
    drain(&mut engine, pipe.source.as_ref(), &mut stream, 3);
    take(&mut checkpointer, &engine, pipe.source.as_ref());
    backend
}

/// Copy the valid body to `pending` and add the durable commit marker.
fn seed_pending(backend: &SharedBackend, valid: u64, pending: u64) {
    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let value = writer
            .get(format!("checkpoint/{valid}/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/{pending}/{part}").as_bytes(), value)
            .unwrap();
    }
    writer
        .put(
            format!("checkpoint/{pending}/commit").as_bytes(),
            b"1".to_vec(),
        )
        .unwrap();
}

/// Delete every key under `checkpoint/<id>/` and the `latest` pointer.
fn delete_checkpoint(backend: &SharedBackend, id: u64) {
    let mut writer = backend.clone();
    for key in writer.list(format!("checkpoint/{id}/").as_bytes()).unwrap() {
        writer.delete(&key).unwrap();
    }
}

fn fallback_id(backend: &SharedBackend) -> u64 {
    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    Recovery::load(&checkpointer)
        .unwrap()
        .expect("checkpoint")
        .id
}

#[test]
fn transactional_sink_is_not_redrivable_by_default() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only(capability(SinkCapabilities::Transactional)),
        ]);

    match Recovery::inspect(&checkpointer).unwrap() {
        RecoveryDecision::Discard { pending, fallback } => {
            assert_eq!(pending, 2);
            assert_eq!(fallback.expect("fallback").id, 1);
        }
        other => panic!("expected Discard, got {other:?}"),
    }
}

#[test]
fn corrupt_pending_body_falls_back_to_a_valid_predecessor() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let mut writer = backend.clone();
    writer
        .put(b"checkpoint/2/engine", b"not-a-snapshot".to_vec())
        .unwrap();

    let (mut hotlap, _pipe) = engine_with(ResumableSource::new(log()));
    let source = ResumableSource::new(log());
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only(capability(SinkCapabilities::AtLeastOnce)),
        ]);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();

    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &source,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .unwrap();

    assert_eq!(
        rows(&hotlap.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]]
    );
    assert!(writer.get(b"checkpoint/2/engine").unwrap().is_none());
    let warning = signal.lock().unwrap().clone().expect("explicit signal");
    assert!(
        warning.contains("checkpoint 1"),
        "must name the fallback: {warning}"
    );
}

#[test]
fn a_commit_marker_over_an_incomplete_body_is_discarded() {
    let backend = seed_valid_one();
    let mut writer = backend.clone();
    let engine = writer.get(b"checkpoint/1/engine").unwrap().unwrap();
    writer.put(b"checkpoint/2/engine", engine).unwrap();
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();

    let checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only(capability(SinkCapabilities::Idempotent)),
        ]);
    match Recovery::inspect(&checkpointer).unwrap() {
        RecoveryDecision::Discard { pending, fallback } => {
            assert_eq!(pending, 2);
            assert_eq!(fallback.expect("fallback").id, 1);
        }
        other => panic!("expected Discard, got {other:?}"),
    }
}

#[test]
fn a_body_without_a_marker_is_not_a_pending_commit() {
    let backend = seed_valid_one();
    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let value = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), value)
            .unwrap();
    }

    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    match Recovery::inspect(&checkpointer).unwrap() {
        RecoveryDecision::Resume(checkpoint) => assert_eq!(checkpoint.id, 1),
        other => panic!("expected Resume, got {other:?}"),
    }
}

#[test]
fn both_valid_and_commit_resumes_and_sweeps_the_marker() {
    let backend = seed_valid_one();
    let mut writer = backend.clone();
    writer.put(b"checkpoint/1/commit", b"1".to_vec()).unwrap();

    let (mut hotlap, _pipe) = engine_with(ResumableSource::new(log()));
    let source = ResumableSource::new(log());
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();

    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &source,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .unwrap();

    assert_eq!(checkpointer.latest().unwrap(), Some(1));
    assert_eq!(
        writer.get(b"checkpoint/1/commit").unwrap(),
        None,
        "the stale marker must be swept"
    );
    assert!(
        signal.lock().unwrap().is_none(),
        "a resume is not a discard"
    );
}

#[test]
fn discard_commit_is_idempotent() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);

    checkpointer.discard_commit(2).unwrap();
    checkpointer.discard_commit(2).unwrap();
    assert!(backend.list(b"checkpoint/2/").unwrap().is_empty());
    assert_eq!(fallback_id(&backend), 1);
}

#[test]
fn discarding_without_a_fallback_starts_clean() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    delete_checkpoint(&backend, 1);

    let (mut hotlap, _pipe) = engine_with(ResumableSource::new(log()));
    let source = ResumableSource::new(log());
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only(capability(SinkCapabilities::AtLeastOnce)),
        ]);
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();

    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &source,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .unwrap();

    assert!(rows(&hotlap.snapshot("c").unwrap()).is_empty());
    let warning = signal.lock().unwrap().clone().expect("explicit signal");
    assert!(
        warning.contains("starting clean"),
        "not a replay: {warning}"
    );
}
