//! Recovery by checkpoint + replay: differential, boundary and retention.

#[path = "common/recovery.rs"]
mod recovery;

use hotlap::state::StateBackend;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;
use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, take};

/// The fixed log used by the differential and boundary tests.
fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3], vec![3, 4]])
}

/// A clean run over the whole log, used as the no-crash reference.
fn reference() -> Vec<Vec<i64>> {
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, usize::MAX);
    rows(&engine.snapshot("c").unwrap())
}

#[test]
fn crash_and_recovery_equals_no_crash() {
    let expected = reference();

    // Checkpoint after three batches, then apply two more that the crash
    // loses from memory (they are only in the engine, not the checkpoint).
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut crashed, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut crashed, &pipe.sources, &mut stream, 3);
    let id = take(&mut checkpointer, &crashed, &pipe.sources);
    assert_eq!(id, 1);
    drain(&mut crashed, &pipe.sources, &mut stream, 2);
    drop(crashed);

    // Recovery restores the checkpoint and replays from the captured offset.
    let loaded = Recovery::load(&checkpointer, &pipe.sources)
        .unwrap()
        .unwrap();
    let (mut recovered, pipe) = engine_with(ResumableSource::new(log()));
    let mut replay = Recovery::resume(&mut recovered, &pipe.sources, &loaded).unwrap();
    drain(&mut recovered, &pipe.sources, &mut replay, usize::MAX);
    let after = rows(&recovered.snapshot("c").unwrap());

    assert_eq!(after, expected, "recovery must match the no-crash run");
}

#[test]
fn recovery_does_not_lose_or_duplicate_at_the_boundary() {
    // The reference counts are 1->2, 2->2, 3->2, 4->1. A duplicate replay of
    // the boundary would bump one count; a skipped replay would drop one.
    let expected = reference();
    assert_eq!(
        expected,
        vec![vec![1, 2], vec![2, 2], vec![3, 2], vec![4, 1]]
    );

    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut crashed, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut crashed, &pipe.sources, &mut stream, 3);
    take(&mut checkpointer, &crashed, &pipe.sources);
    drop(crashed);

    let loaded = Recovery::load(&checkpointer, &pipe.sources)
        .unwrap()
        .unwrap();
    let (mut recovered, pipe) = engine_with(ResumableSource::new(log()));
    let mut replay = Recovery::resume(&mut recovered, &pipe.sources, &loaded).unwrap();
    drain(&mut recovered, &pipe.sources, &mut replay, usize::MAX);

    assert_eq!(rows(&recovered.snapshot("c").unwrap()), expected);
}

#[test]
fn resume_seeds_the_captured_offsets_into_the_source() {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    take(&mut checkpointer, &engine, &pipe.sources);
    let loaded = Recovery::load(&checkpointer, &pipe.sources)
        .unwrap()
        .unwrap();

    // A freshly built source has no applied position until recovery seeds it.
    let (mut target, pipe) = engine_with(ResumableSource::new(log()));
    assert!(pipe.sources.entries()[0].source.state().offsets.is_empty());

    let _ = Recovery::resume(&mut target, &pipe.sources, &loaded).unwrap();

    // Otherwise a checkpoint taken before the next commit captures nothing and
    // a second crash replays the log over the restored snapshot.
    assert_eq!(
        pipe.sources.entries()[0]
            .source
            .state()
            .offsets
            .get(&0)
            .copied(),
        Some(3)
    );
}

#[test]
fn missing_checkpoint_is_a_clean_start() {
    let backend = SharedBackend::default();
    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let sources = recovery::sources(ResumableSource::new(log()));
    assert!(Recovery::load(&checkpointer, &sources).unwrap().is_none());
}

#[test]
fn corrupt_latest_falls_back_to_an_older_checkpoint() {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 2);
    take(&mut checkpointer, &engine, &pipe.sources);
    drain(&mut engine, &pipe.sources, &mut stream, 1);
    take(&mut checkpointer, &engine, &pipe.sources);
    assert_eq!(checkpointer.latest().unwrap(), Some(2));

    // Corrupt the newest body while it stays the `latest` pointer.
    let mut writer = backend.clone();
    writer
        .put(b"checkpoint/2/engine", b"not-a-snapshot".to_vec())
        .unwrap();

    let loaded = Recovery::load(&checkpointer, &pipe.sources)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.id, 1, "expected fallback to the older checkpoint");
}

#[test]
fn insufficient_retention_is_an_explicit_error() {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    take(&mut checkpointer, &engine, &pipe.sources);
    let loaded = Recovery::load(&checkpointer, &pipe.sources)
        .unwrap()
        .unwrap();

    // The broker has dropped the records the checkpoint points at (offset 3).
    let stale = log().with_retention(4);
    let (mut target, pipe) = engine_with(ResumableSource::new(stale));
    let error = match Recovery::resume(&mut target, &pipe.sources, &loaded) {
        Ok(_) => panic!("expected an explicit retention error"),
        Err(error) => error,
    };
    assert!(
        matches!(error, hotlap_connectors::ConnectorError::Unsupported(_)),
        "expected an explicit retention error, got {error:?}"
    );
}
