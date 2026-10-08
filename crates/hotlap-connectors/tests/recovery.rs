//! Recovery by checkpoint + replay: differential, boundary and retention.

#[path = "common/recovery.rs"]
mod recovery;

use hotlap_connectors::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_connectors::runtime::pipeline;
use hotlap_connectors::runtime::recovery::Recovery;
use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, take};

/// The fixed log used by the differential and boundary tests.
fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3], vec![3, 4]])
}

/// A clean run over the whole log, used as the no-crash reference.
fn reference() -> Vec<Vec<i64>> {
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipeline::merged_stream(pipe.source.as_ref()).unwrap();
    drain(&mut engine, &mut stream, usize::MAX);
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
    let mut stream = pipeline::merged_stream(pipe.source.as_ref()).unwrap();
    drain(&mut crashed, &mut stream, 3);
    let id = take(&mut checkpointer, &crashed, pipe.source.as_ref());
    assert_eq!(id, 1);
    drain(&mut crashed, &mut stream, 2);
    drop(crashed);

    // Recovery restores the checkpoint and replays from the captured offset.
    let loaded = Recovery::load(&checkpointer).unwrap().unwrap();
    let (mut recovered, pipe) = engine_with(ResumableSource::new(log()));
    let mut replay = Recovery::resume(&mut recovered, pipe.source.as_ref(), &loaded).unwrap();
    drain(&mut recovered, &mut replay, usize::MAX);
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
    let mut stream = pipeline::merged_stream(pipe.source.as_ref()).unwrap();
    drain(&mut crashed, &mut stream, 3);
    take(&mut checkpointer, &crashed, pipe.source.as_ref());
    drop(crashed);

    let loaded = Recovery::load(&checkpointer).unwrap().unwrap();
    let (mut recovered, pipe) = engine_with(ResumableSource::new(log()));
    let mut replay = Recovery::resume(&mut recovered, pipe.source.as_ref(), &loaded).unwrap();
    drain(&mut recovered, &mut replay, usize::MAX);

    assert_eq!(rows(&recovered.snapshot("c").unwrap()), expected);
}

#[test]
fn missing_checkpoint_is_a_clean_start() {
    let backend = SharedBackend::default();
    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    assert!(Recovery::load(&checkpointer).unwrap().is_none());
}

#[test]
fn insufficient_retention_is_an_explicit_error() {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipeline::merged_stream(pipe.source.as_ref()).unwrap();
    drain(&mut engine, &mut stream, 3);
    take(&mut checkpointer, &engine, pipe.source.as_ref());
    let loaded = Recovery::load(&checkpointer).unwrap().unwrap();

    // The broker has dropped the records the checkpoint points at (offset 3).
    let stale = log().with_retention(4);
    let (mut target, pipe) = engine_with(ResumableSource::new(stale));
    let error = match Recovery::resume(&mut target, pipe.source.as_ref(), &loaded) {
        Ok(_) => panic!("expected an explicit retention error"),
        Err(error) => error,
    };
    assert!(
        matches!(error, hotlap_connectors::ConnectorError::Unsupported(_)),
        "expected an explicit retention error, got {error:?}"
    );
}
