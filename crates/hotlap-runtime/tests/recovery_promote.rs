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
use hotlap_runtime::runtime::pipeline as runtime;
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, take};

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

/// A sink that counts commits, optionally declares re-drivability and can fail.
struct CountingSink {
    capabilities: SinkCapabilities,
    redriable: bool,
    fail_commit: bool,
    commits: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Sink for CountingSink {
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
        if self.fail_commit {
            return Err(ConnectorError::Infrastructure("commit failed".into()));
        }
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn counting(redriable: bool, fail_commit: bool, commits: u32) -> (Arc<SharedSink>, Arc<AtomicU32>) {
    let commits = Arc::new(AtomicU32::new(commits));
    let sink = Arc::new(CountingSink {
        capabilities: SinkCapabilities::Idempotent,
        redriable,
        fail_commit,
        commits: Arc::clone(&commits),
    });
    (SharedSink::new(sink), commits)
}

fn seed_valid_one() -> SharedBackend {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = runtime::merged_stream(pipe.source.as_ref()).unwrap();
    drain(&mut engine, pipe.source.as_ref(), &mut stream, 3);
    take(&mut checkpointer, &engine, pipe.source.as_ref());
    backend
}

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

/// Run `Recovery::start` against a seeded backend and return the warning.
fn start(
    backend: &SharedBackend,
    sinks: Vec<SinkSync>,
) -> (hotlap::Hotlap, String, MetricsRegistry) {
    let (mut hotlap, _pipe) = engine_with(ResumableSource::new(log()));
    let source = ResumableSource::new(log());
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(sinks);
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
    let warning = signal.lock().unwrap().clone().unwrap_or_default();
    (hotlap, warning, metrics)
}

#[test]
fn start_promotes_a_redrivable_commit_and_resumes() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let (sink, commits) = counting(true, false, 0);

    let (mut hotlap, warning, _) = start(&backend, vec![SinkSync::sink_only(sink)]);

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
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    // Model a partial commit: the first sink committed before the crash, the
    // second did not. Both must be re-driven, so the first runs twice.
    let (first, first_commits) = counting(true, false, 1);
    let (second, second_commits) = counting(true, false, 0);

    let (_hotlap, _warning, _) = start(
        &backend,
        vec![SinkSync::sink_only(first), SinkSync::sink_only(second)],
    );

    assert_eq!(first_commits.load(Ordering::SeqCst), 2);
    assert_eq!(second_commits.load(Ordering::SeqCst), 1);
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
}

#[test]
fn redrive_failure_is_discarded_and_replayed() {
    let backend = seed_valid_one();
    seed_pending(&backend, 1, 2);
    let (sink, commits) = counting(true, true, 0);

    let (mut hotlap, warning, metrics) = start(&backend, vec![SinkSync::sink_only(sink)]);

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
