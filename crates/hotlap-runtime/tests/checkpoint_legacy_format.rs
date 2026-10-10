//! A genuine older snapshot format is fatal: never a silent fallback to an
//! older checkpoint and never a discard of an interrupted commit.

#[path = "common/legacy_snapshot.rs"]
mod legacy_snapshot;
#[path = "common/recovery.rs"]
mod recovery;

use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;

use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, sources, take};

/// The fixed log, with retention that keeps every record.
fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3], vec![3, 4]])
        .with_retention(0)
        .with_physical_identity("test/checkpoint-legacy-format/log")
}

/// Persist one valid checkpoint over `backend` and return its id.
fn seed_valid(backend: &SharedBackend) -> u64 {
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    assert_eq!(
        rows(&engine.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]]
    );
    take(&mut checkpointer, &engine, &pipe.sources)
}

/// Copy checkpoint `valid` under `pending` plus a commit marker, no `valid`.
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

/// A genuine v4 body as the newest complete checkpoint must be fatal, never a
/// silent fallback to the older body.
#[test]
fn an_old_snapshot_version_is_fatal_not_a_fallback() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    let mut writer = backend.clone();
    for part in ["engine", "sources", "participants"] {
        let value = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), value)
            .unwrap();
    }
    writer
        .put(
            b"checkpoint/2/engine",
            legacy_snapshot::old_v4_snapshot_bytes(),
        )
        .unwrap();
    writer.put(b"checkpoint/2/valid", b"1".to_vec()).unwrap();
    writer
        .put(b"checkpoint/latest", 2u64.to_le_bytes().to_vec())
        .unwrap();

    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let error = Recovery::load(&checkpointer, &sources(ResumableSource::new(log())))
        .expect_err("an old snapshot version is fatal");
    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert!(
        !backend.list(b"checkpoint/1/").unwrap().is_empty(),
        "an incompatible newer checkpoint must not delete the older body"
    );
}

/// A genuine v4 body left by an interrupted commit must be fatal too, and must
/// not be discarded or fall back to the older checkpoint.
#[test]
fn an_old_pending_snapshot_version_is_fatal_without_discard() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    seed_pending(&backend, 1, 2);
    let mut writer = backend.clone();
    writer
        .put(
            b"checkpoint/2/engine",
            legacy_snapshot::old_v4_snapshot_bytes(),
        )
        .unwrap();

    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let error = Recovery::inspect(&checkpointer, &sources(ResumableSource::new(log())))
        .expect_err("an old pending snapshot is fatal");
    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert!(
        !backend.list(b"checkpoint/2/").unwrap().is_empty(),
        "a foreign pending body must not be discarded"
    );
}
