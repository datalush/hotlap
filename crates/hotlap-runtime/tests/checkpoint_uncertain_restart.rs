//! Restart resolution of a checkpoint interrupted after the sinks were touched.
//!
//! A re-drivable pending commit is promoted and its staged values are delivered
//! exactly once. A non-re-drivable one is discarded and replayed, so its remote
//! output may be duplicated: that guarantee is not exactly-once.

#[path = "checkpoint_uncertain/harness.rs"]
mod harness;
#[path = "common/spy.rs"]
mod spy;
#[path = "checkpoint_uncertain/support.rs"]
mod support;

use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap::{Hotlap, InputId};
use hotlap_connectors::ConnectorError;
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use harness::{DurableSink, SharedBackend};
use spy::SpySource;
use support::{Dataset, ResumableSource, drain, engine_with, rows};

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

fn take(
    checkpointer: &mut Checkpointer,
    engine: &Hotlap,
    sources: &Sources,
) -> Result<u64, ConnectorError> {
    futures::executor::block_on(checkpointer.take(engine, sources))
}

/// A resume-counting source set over `log`, named like the captured source.
fn spy_sources() -> (Sources, Arc<SpySource>) {
    let inner: Arc<dyn hotlap_connectors::source::Source> = Arc::new(ResumableSource::new(log()));
    let source = Arc::new(SpySource::new(inner));
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: source.clone(),
        watermark: None,
    }])
    .unwrap();
    (sources, source)
}

/// Run one interrupted commit: the first sink delivers, the second fails.
fn interrupt(
    backend: &SharedBackend,
    stage: &SharedBackend,
    remote: &SharedBackend,
    redriable: bool,
) {
    let first = Arc::new(DurableSink::new(
        "a",
        stage.clone(),
        remote.clone(),
        redriable,
    ));
    let second = Arc::new(DurableSink::failing(
        "b",
        stage.clone(),
        remote.clone(),
        redriable,
        1,
    ));
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![harness::coord(first), harness::coord(second)]);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    assert!(take(&mut checkpointer, &engine, &pipe.sources).is_err());
}

/// Restart over `backend` with fresh sinks sharing `stage`/`remote`.
fn restart(
    backend: &SharedBackend,
    stage: &SharedBackend,
    remote: &SharedBackend,
    redriable: bool,
) -> (
    MetricsRegistry,
    Mutex<Option<String>>,
    Arc<SpySource>,
    Arc<DurableSink>,
    Hotlap,
) {
    let first = Arc::new(DurableSink::new(
        "a",
        stage.clone(),
        remote.clone(),
        redriable,
    ));
    let second = Arc::new(DurableSink::new(
        "b",
        stage.clone(),
        remote.clone(),
        redriable,
    ));
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![harness::coord(first.clone()), harness::coord(second)]);
    let (mut hotlap, _pipe) = engine_with(ResumableSource::new(log()));
    let (sources, spy) = spy_sources();
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();
    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ))
    .expect("recovery must resolve the pending commit");
    (metrics, signal, spy, first, hotlap)
}

fn remote_value(remote: &SharedBackend, token: &str) -> Option<Vec<u8>> {
    remote.get(format!("remote/{token}").as_bytes()).unwrap()
}

#[test]
fn a_restart_promotes_the_redrivable_pending_without_duplicating() {
    let backend = SharedBackend::default();
    let stage = SharedBackend::default();
    let remote = SharedBackend::default();
    interrupt(&backend, &stage, &remote, true);

    let (metrics, signal, spy, first, mut hotlap) = restart(&backend, &stage, &remote, true);

    assert_eq!(
        rows(&hotlap.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]],
        "promotion resumes the pending state instead of replaying"
    );
    assert_eq!(remote_value(&remote, "a").as_deref(), Some(b"a".as_ref()));
    assert_eq!(remote_value(&remote, "b").as_deref(), Some(b"b".as_ref()));
    assert_eq!(first.delivered().as_deref(), Some(b"a".as_ref()));
    assert!(
        first.events().contains(&harness::Event::Commit),
        "the fresh instance re-drove its commit"
    );
    assert!(
        first.staged().is_none(),
        "the re-driven commit cleared the stage"
    );
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/1/commit").unwrap(), None);
    assert_eq!(metrics.snapshot().get("checkpoints_discarded"), None);
    assert!(
        signal.lock().unwrap().is_none(),
        "a promotion is not a discard"
    );
    assert_eq!(spy.resumed(), 1, "promotion resumes instead of replaying");
    assert_eq!(spy.offset(), Some(3));
}

#[test]
fn a_non_redrivable_pending_is_discarded_and_replayed() {
    let backend = SharedBackend::default();
    let stage = SharedBackend::default();
    let remote = SharedBackend::default();
    interrupt(&backend, &stage, &remote, false);

    let (metrics, signal, spy, _first, _hotlap) = restart(&backend, &stage, &remote, false);

    assert_eq!(metrics.snapshot().get("checkpoints_discarded"), Some(&1));
    assert!(
        signal.lock().unwrap().is_some(),
        "a discard must signal the replay"
    );
    assert_eq!(backend.get(b"checkpoint/1/commit").unwrap(), None);
    assert_eq!(backend.get(b"checkpoint/1/valid").unwrap(), None);
    assert_eq!(spy.resumed(), 0, "a discard replays from the fallback");
    // The first run's delivery is not undone: without a re-drive, the replay
    // may duplicate it, so this path is not exactly-once.
    assert_eq!(remote_value(&remote, "a").as_deref(), Some(b"a".as_ref()));
}
