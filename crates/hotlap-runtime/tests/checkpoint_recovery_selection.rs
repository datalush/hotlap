//! Recovery must select the newest valid body even when `latest` lags, and
//! storage failures during recovery must stop it before touching any source.

#[path = "common/recovery.rs"]
mod recovery;

use hotlap::state::StateBackend;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;

use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, sources, take};

/// A log with retention keeping every record.
fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]])
        .with_retention(0)
        .with_physical_identity("test/checkpoint-recovery-selection/log")
}

#[test]
fn stale_latest_does_not_hide_a_newer_valid() {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 2);
    let e1 = engine.checkpoint().unwrap().epoch;
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources), 1);
    drain(&mut engine, &pipe.sources, &mut stream, 2);
    let e2 = engine.checkpoint().unwrap().epoch;
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources), 2);
    assert_ne!(e1, e2);

    // The newest valid id 2 was published, but the pointer still names id 1
    // (as after a failed `latest` write).
    let mut writer = backend.clone();
    writer
        .put(b"checkpoint/latest", 1u64.to_le_bytes().to_vec())
        .unwrap();

    let loaded = Recovery::load(&checkpointer, &sources(ResumableSource::new(log())))
        .unwrap()
        .unwrap();
    assert_eq!(loaded.id, 2, "the newest valid body must win");
    assert_eq!(loaded.engine.epoch, e2);

    let (mut hotlap, pipe) = engine_with(ResumableSource::new(log()));
    let mut startup = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let signal = std::sync::Mutex::new(None);
    let metrics = hotlap_engine::MetricsRegistry::new();
    let _stream = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &pipe.sources,
        &mut startup,
        &signal,
        &metrics,
    ))
    .unwrap();
    assert_eq!(
        rows(&hotlap.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2], vec![3, 1]],
        "startup must restore the newest valid body"
    );
}
