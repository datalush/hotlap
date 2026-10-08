//! Recovery decisions around the durable commit-intent marker.
//!
//! `checkpoint/<id>/commit` is written after the body, before `Sink::commit`,
//! and removed after `mark_valid`, so a crash leaves a marker without `valid`.

#[path = "common/recovery.rs"]
mod recovery;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline as runtime;
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, take};

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

/// A sink that counts commits and, when probing, reports whether the commit
/// marker was visible (and `valid` absent) while `commit` ran.
struct FakeSink {
    capabilities: SinkCapabilities,
    commits: Arc<AtomicU32>,
    probe: Option<(SharedBackend, u64, Arc<Mutex<bool>>)>,
}

#[async_trait::async_trait]
impl Sink for FakeSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        self.capabilities
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        if let Some((backend, id, observed)) = &self.probe {
            let marker = backend
                .get(format!("checkpoint/{id}/commit").as_bytes())
                .unwrap()
                .is_some();
            let valid = backend
                .get(format!("checkpoint/{id}/valid").as_bytes())
                .unwrap()
                .is_some();
            *observed.lock().unwrap() = marker && !valid;
        }
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A shared fake sink with the given capability plus its commit counter.
fn sink(capabilities: SinkCapabilities) -> (Arc<SharedSink>, Arc<AtomicU32>) {
    let commits = Arc::new(AtomicU32::new(0));
    let sink = Arc::new(FakeSink {
        capabilities,
        commits: Arc::clone(&commits),
        probe: None,
    });
    (SharedSink::new(sink), commits)
}

/// Persist a valid checkpoint 1 over the shared backend.
fn seed_valid_one() -> SharedBackend {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = runtime::merged_stream(pipe.source.as_ref()).unwrap();
    drain(&mut engine, pipe.source.as_ref(), &mut stream, 3);
    take(&mut checkpointer, &engine, pipe.source.as_ref());
    assert_eq!(
        rows(&engine.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]]
    );
    backend
}

/// Simulate a crash mid-commit: copy the valid body to `pending` and add the
/// durable commit marker, leaving `valid` absent.
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

#[test]
fn commit_marker_is_durable_before_commit_and_removed_after_valid() {
    let backend = SharedBackend::default();
    let commits = Arc::new(AtomicU32::new(0));
    let observed = Arc::new(Mutex::new(false));
    let probe = Arc::new(FakeSink {
        capabilities: SinkCapabilities::Transactional,
        commits: Arc::clone(&commits),
        probe: Some((backend.clone(), 1, Arc::clone(&observed))),
    });
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(SharedSink::new(probe))]);

    let (engine, pipe) = engine_with(ResumableSource::new(log()));
    let id = futures::executor::block_on(checkpointer.take(&engine, pipe.source.as_ref())).unwrap();

    assert_eq!(id, 1);
    assert_eq!(commits.load(Ordering::SeqCst), 1);
    let durable = *observed.lock().unwrap();
    assert!(durable, "the marker must be durable before Sink::commit");
    assert_eq!(backend.get(b"checkpoint/1/commit").unwrap(), None);
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
}

#[test]
fn recovery_promotes_a_redrivable_interrupted_commit() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let (sink, commits) = sink(SinkCapabilities::Transactional);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(sink)]);

    let checkpoint = match Recovery::inspect(&checkpointer).unwrap() {
        RecoveryDecision::Promote(checkpoint) => checkpoint,
        other => panic!("expected Promote, got {other:?}"),
    };
    assert_eq!(checkpoint.id, 2);
    assert_eq!(commits.load(Ordering::SeqCst), 0, "inspect must not commit");

    futures::executor::block_on(checkpointer.promote(2)).unwrap();

    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "commit must be re-driven"
    );
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), None);
    assert!(matches!(
        Recovery::inspect(&checkpointer).unwrap(),
        RecoveryDecision::Resume(c) if c.id == 2
    ));
}

#[test]
fn recovery_discards_a_non_redrivable_interrupted_commit() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let (sink, commits) = sink(SinkCapabilities::AtLeastOnce);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(sink)]);

    match Recovery::inspect(&checkpointer).unwrap() {
        RecoveryDecision::Discard { pending, fallback } => {
            assert_eq!(pending, 2);
            assert_eq!(fallback.expect("fallback").id, 1);
        }
        other => panic!("expected Discard, got {other:?}"),
    }
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not commit");

    checkpointer.discard_commit(2).unwrap();
    assert!(
        backend.list(b"checkpoint/2/").unwrap().is_empty(),
        "the discarded checkpoint must be removed"
    );
    assert_eq!(Recovery::load(&checkpointer).unwrap().unwrap().id, 1);
}

#[test]
fn recovery_without_a_marker_matches_the_newest_valid() {
    let backend = seed_valid_one();
    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);

    match Recovery::inspect(&checkpointer).unwrap() {
        RecoveryDecision::Resume(checkpoint) => assert_eq!(checkpoint.id, 1),
        other => panic!("expected Resume, got {other:?}"),
    }
}
