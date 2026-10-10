//! A sink declares whether its `commit` may be re-driven after a restart.
//!
//! Recovery uses `Sink::commit_redriable` (only `Idempotent` by default; other
//! capabilities must opt in) to promote or discard an interrupted commit.

#[path = "common/recovery.rs"]
mod recovery;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, sources, take};

/// A sink that overrides the default re-drivability declaration.
struct DeclaringSink {
    physical_identity: String,
    capabilities: SinkCapabilities,
    redriable: bool,
    commits: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Sink for DeclaringSink {
    fn physical_identity(&self) -> Option<String> {
        Some(self.physical_identity.clone())
    }

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
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]])
        .with_retention(0)
        .with_physical_identity("test/recovery-redrivable/log")
}

/// A shared declaring sink plus its commit counter.
fn declaring(
    capabilities: SinkCapabilities,
    redriable: bool,
    physical_identity: &str,
) -> (Arc<SharedSink>, Arc<AtomicU32>) {
    let commits = Arc::new(AtomicU32::new(0));
    let sink = Arc::new(DeclaringSink {
        physical_identity: physical_identity.to_owned(),
        capabilities,
        redriable,
        commits: Arc::clone(&commits),
    });
    (SharedSink::new(sink), commits)
}

/// Persist a valid checkpoint 1 over the shared backend.
fn seed_valid_one(
    capabilities: SinkCapabilities,
    redriable: bool,
    physical_identity: &str,
) -> SharedBackend {
    let backend = SharedBackend::default();
    let (sink, _) = declaring(capabilities, redriable, physical_identity);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(sink, "output".into(), "c".into()),
        ]);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    engine.tap_view("c").unwrap();
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    take(&mut checkpointer, &engine, &pipe.sources);
    backend
}

/// Simulate a crash mid-commit: copy the valid body to `pending` and add the
/// durable commit marker, leaving `valid` absent.
fn seed_pending(backend: &SharedBackend, valid: u64, pending: u64) {
    let mut writer = backend.clone();
    for part in ["engine", "sources", "participants"] {
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
    let target = "test/recovery-redrivable/non-redrivable";
    let backend = seed_valid_one(SinkCapabilities::Idempotent, false, target);
    seed_pending(&backend, 1, 2);
    let (sink, commits) = declaring(SinkCapabilities::Idempotent, false, target);
    let checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(sink, "output".into(), "c".into()),
        ]);

    match Recovery::inspect(&checkpointer, &sources(ResumableSource::new(log()))).unwrap() {
        RecoveryDecision::Discard {
            pending, fallback, ..
        } => {
            assert_eq!(pending, 2);
            assert_eq!(fallback.expect("fallback").id, 1);
        }
        other => panic!("expected Discard, got {other:?}"),
    }
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not commit");
}

#[test]
fn a_sink_can_declare_commit_redrivable_despite_at_least_once() {
    let target = "test/recovery-redrivable/redrivable";
    let backend = seed_valid_one(SinkCapabilities::AtLeastOnce, true, target);
    seed_pending(&backend, 1, 2);
    let (sink, commits) = declaring(SinkCapabilities::AtLeastOnce, true, target);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(sink, "output".into(), "c".into()),
        ]);

    match Recovery::inspect(&checkpointer, &sources(ResumableSource::new(log()))).unwrap() {
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
    let target = "test/recovery-redrivable/discard";
    let backend = seed_valid_one(SinkCapabilities::AtLeastOnce, false, target);
    seed_pending(&backend, 1, 2);
    let (sink, _) = declaring(SinkCapabilities::AtLeastOnce, false, target);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(sink, "output".into(), "c".into()),
        ]);
    let (mut hotlap, pipe) = engine_with(ResumableSource::new(log()));
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();

    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
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
    assert!(
        warning.contains("not re-drivable"),
        "signal must name the un-redrivable sink: {warning}"
    );
    assert_eq!(metrics.snapshot().get("checkpoints_discarded"), Some(&1));
}
