//! A sink declares whether its `commit` may be re-driven after a restart.
//!
//! Recovery uses `Sink::commit_redriable` (defaulting to the delivery
//! capability) to promote or discard an interrupted commit.

#[path = "common/recovery.rs"]
mod recovery;

use std::sync::atomic::{AtomicU32, Ordering};
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

/// A sink that overrides the default re-drivability declaration.
struct DeclaringSink {
    capabilities: SinkCapabilities,
    redriable: bool,
    commits: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Sink for DeclaringSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        self.capabilities
    }

    fn commit_redriable(&self) -> bool {
        self.redriable
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

/// A shared declaring sink plus its commit counter.
fn declaring(capabilities: SinkCapabilities, redriable: bool) -> (Arc<SharedSink>, Arc<AtomicU32>) {
    let commits = Arc::new(AtomicU32::new(0));
    let sink = Arc::new(DeclaringSink {
        capabilities,
        redriable,
        commits: Arc::clone(&commits),
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
fn a_sink_can_declare_commit_non_redrivable_despite_its_capability() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let (sink, commits) = declaring(SinkCapabilities::Idempotent, false);
    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(sink)]);

    match Recovery::inspect(&checkpointer).unwrap() {
        RecoveryDecision::Discard { pending, fallback } => {
            assert_eq!(pending, 2);
            assert_eq!(fallback.expect("fallback").id, 1);
        }
        other => panic!("expected Discard, got {other:?}"),
    }
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not commit");
}

#[test]
fn a_sink_can_declare_commit_redrivable_despite_at_least_once() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let (sink, commits) = declaring(SinkCapabilities::AtLeastOnce, true);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(sink)]);

    match Recovery::inspect(&checkpointer).unwrap() {
        RecoveryDecision::Promote(checkpoint) => assert_eq!(checkpoint.id, 2),
        other => panic!("expected Promote, got {other:?}"),
    }
    futures::executor::block_on(checkpointer.promote(2)).unwrap();
    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "commit must be re-driven"
    );
}

#[test]
fn discarding_emits_a_warning_signal_and_metric() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let (sink, _) = declaring(SinkCapabilities::AtLeastOnce, false);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(sink)]);
    let (mut hotlap, _pipe) = engine_with(ResumableSource::new(log()));
    let source = ResumableSource::new(log());
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
    let warning = signal.lock().unwrap().clone().expect("explicit signal");
    assert!(
        warning.contains("discarded interrupted checkpoint 2"),
        "signal must name the discarded checkpoint: {warning}"
    );
    assert_eq!(metrics.snapshot().get("checkpoints_discarded"), Some(&1));
}
