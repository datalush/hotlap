//! The `Recovery::start` promote path: re-drive the sinks, publish or discard.

#[path = "common/recovery.rs"]
mod recovery;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, take};

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]])
        .with_retention(0)
        .with_physical_identity("test/recovery-promote/log")
}

/// A sink that counts commits, optionally declares re-drivability and can fail.
struct CountingSink {
    physical_identity: String,
    capabilities: SinkCapabilities,
    redriable: bool,
    fail_commit: bool,
    commits: Arc<AtomicU32>,
    prepares: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Sink for CountingSink {
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
    async fn prepare(&self) -> Result<(), ConnectorError> {
        self.prepares.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        if self.fail_commit {
            return Err(ConnectorError::Infrastructure("commit failed".into()));
        }
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn counting(
    physical_identity: &str,
    redriable: bool,
    fail_commit: bool,
    commits: u32,
) -> (Arc<SharedSink>, Arc<AtomicU32>) {
    let (sink, commits, _) =
        counting_with_prepare(physical_identity, redriable, fail_commit, commits);
    (sink, commits)
}

fn counting_with_prepare(
    physical_identity: &str,
    redriable: bool,
    fail_commit: bool,
    initial_commits: u32,
) -> (Arc<SharedSink>, Arc<AtomicU32>, Arc<AtomicU32>) {
    let commits = Arc::new(AtomicU32::new(initial_commits));
    let prepares = Arc::new(AtomicU32::new(0));
    let sink = Arc::new(CountingSink {
        physical_identity: physical_identity.to_owned(),
        capabilities: SinkCapabilities::Idempotent,
        redriable,
        fail_commit,
        commits: Arc::clone(&commits),
        prepares: Arc::clone(&prepares),
    });
    (SharedSink::new(sink), commits, prepares)
}

fn seed_valid_one(sinks: Vec<SinkSync>) -> SharedBackend {
    let backend = SharedBackend::default();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(sinks);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    engine.tap_view("c").unwrap();
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    take(&mut checkpointer, &engine, &pipe.sources);
    backend
}

#[test]
fn checkpoint_rejects_sink_bound_untapped_view_before_prepare_or_capture() {
    let backend = SharedBackend::default();
    let (engine, pipe) = engine_with(ResumableSource::new(log()));
    let (sink, commits, prepares) =
        counting_with_prepare("test/recovery-promote/untapped", true, false, 0);
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(sink, "output".into(), "c".into()),
        ]);

    let result = futures::executor::block_on(checkpointer.take(&engine, &pipe.sources));

    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_eq!(prepares.load(Ordering::SeqCst), 0);
    assert_eq!(commits.load(Ordering::SeqCst), 0);
    for key in [
        b"checkpoint/reserved".as_slice(),
        b"checkpoint/1/participants",
        b"checkpoint/1/prepare",
        b"checkpoint/1/engine",
        b"checkpoint/1/sources",
        b"checkpoint/1/commit",
        b"checkpoint/1/valid",
    ] {
        assert!(
            backend.get(key).unwrap().is_none(),
            "unexpected write: {key:?}"
        );
    }
}

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

/// Run `Recovery::start` against a seeded backend and return the warning.
fn start(
    backend: &SharedBackend,
    sinks: Vec<SinkSync>,
) -> (hotlap::Hotlap, String, MetricsRegistry) {
    let (mut hotlap, pipe) = engine_with(ResumableSource::new(log()));
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(sinks);
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
    let warning = signal.lock().unwrap().clone().unwrap_or_default();
    (hotlap, warning, metrics)
}

#[test]
fn start_promotes_a_redrivable_commit_and_resumes() {
    let target = "test/recovery-promote/output";
    let (sink, _) = counting(target, true, false, 0);
    let binding = SinkSync::sink_only_named(sink, "output".into(), "c".into());
    let backend = seed_valid_one(vec![binding]);
    seed_pending(&backend, 1, 2);
    let (sink, commits) = counting(target, true, false, 0);

    let (mut hotlap, warning, _) = start(
        &backend,
        vec![SinkSync::sink_only_named(sink, "output".into(), "c".into())],
    );

    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "commit must be re-driven"
    );
    assert!(warning.is_empty(), "a promotion is not a discard");
    assert_eq!(
        rows(&hotlap.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]]
    );
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), None);
}

#[test]
fn promote_redrives_commit_for_every_sink() {
    let first_target = "test/recovery-promote/first";
    let second_target = "test/recovery-promote/second";
    let (seed_first, _) = counting(first_target, true, false, 0);
    let (seed_second, _) = counting(second_target, true, false, 0);
    let backend = seed_valid_one(vec![
        SinkSync::sink_only_named(seed_first, "first".into(), "c".into()),
        SinkSync::sink_only_named(seed_second, "second".into(), "c".into()),
    ]);
    seed_pending(&backend, 1, 2);
    // Model a partial commit: the first sink committed before the crash, the
    // second did not. Both must be re-driven, so the first runs twice.
    let (first, first_commits) = counting(first_target, true, false, 1);
    let (second, second_commits) = counting(second_target, true, false, 0);

    let (_hotlap, _warning, _) = start(
        &backend,
        vec![
            SinkSync::sink_only_named(first, "first".into(), "c".into()),
            SinkSync::sink_only_named(second, "second".into(), "c".into()),
        ],
    );

    assert_eq!(first_commits.load(Ordering::SeqCst), 2);
    assert_eq!(second_commits.load(Ordering::SeqCst), 1);
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
}

#[test]
fn redrive_failure_is_discarded_and_replayed() {
    let target = "test/recovery-promote/output-failure";
    let (seed_sink, _) = counting(target, true, false, 0);
    let backend = seed_valid_one(vec![SinkSync::sink_only_named(
        seed_sink,
        "output".into(),
        "c".into(),
    )]);
    seed_pending(&backend, 1, 2);
    let (sink, commits) = counting(target, true, true, 0);

    let (mut hotlap, warning, metrics) = start(
        &backend,
        vec![SinkSync::sink_only_named(sink, "output".into(), "c".into())],
    );

    assert!(
        commits.load(Ordering::SeqCst) >= 1,
        "the re-drive must have been attempted"
    );
    assert!(
        warning.contains("commit re-drive failed"),
        "must signal the failure: {warning}"
    );
    assert!(
        warning.contains("checkpoint 1"),
        "must replay the fallback: {warning}"
    );
    assert_eq!(metrics.snapshot().get("checkpoints_discarded"), Some(&1));
    assert_eq!(
        rows(&hotlap.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]]
    );
    assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), None);
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_none());
}
