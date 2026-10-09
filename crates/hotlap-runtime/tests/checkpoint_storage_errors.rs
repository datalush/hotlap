//! Checkpoint error taxonomy: store and internal failures are fatal, only
//! current-format corruption or absence may fall back, and foreign magic or
//! versions stay fatal.

#[path = "common/fault.rs"]
mod fault;
#[path = "common/recovery.rs"]
mod recovery;

use std::error::Error;
use std::io;

use hotlap::state::{StateBackend, StateError};
use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;

use fault::FaultBackend;
use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, sources, take};

/// The fixed log, with retention that keeps every record.
fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3], vec![3, 4]]).with_retention(0)
}

/// Persist one valid checkpoint over `backend` and return its id.
fn seed_valid(backend: &SharedBackend) -> u64 {
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    take(&mut checkpointer, &engine, &pipe.sources)
}

/// Copy checkpoint `valid` under `pending` plus a commit marker, no `valid`.
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
fn a_store_read_error_is_error_not_absence() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("get", b"checkpoint/1/engine", false);
    let checkpointer = Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN);

    let error = Recovery::load(&checkpointer, &sources(ResumableSource::new(log())))
        .expect_err("an I/O read failure must not look like a clean start");
    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
    assert!(error.source().is_some(), "the store cause must be retained");
}

#[test]
fn a_store_list_error_stops_recovery_before_cleanup() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    seed_pending(&backend, 1, 2);
    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("list", b"checkpoint/", false);
    let checkpointer = Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN);

    let error = Recovery::inspect(&checkpointer, &sources(ResumableSource::new(log())))
        .expect_err("a listing failure must abort before any discard");
    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
    assert!(
        !backend.list(b"checkpoint/2/").unwrap().is_empty(),
        "nothing may be deleted while the store is failing"
    );
}

#[test]
fn a_delete_error_during_discard_propagates() {
    let backend = SharedBackend::default();
    let mut seeder = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (engine, pipe) = engine_with(ResumableSource::new(log()));
    take(&mut seeder, &engine, &pipe.sources);

    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("delete", b"checkpoint/1/engine", true);
    let mut checkpointer = Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN);
    let error = checkpointer
        .discard_commit(1)
        .expect_err("a delete failure must surface");
    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");
}

#[test]
fn current_format_corruption_falls_back_to_an_older_body() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let value = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), value)
            .unwrap();
    }
    writer.put(b"checkpoint/2/valid", b"1".to_vec()).unwrap();
    writer
        .put(b"checkpoint/latest", 2u64.to_le_bytes().to_vec())
        .unwrap();
    writer
        .put(b"checkpoint/2/engine", b"not-a-snapshot".to_vec())
        .unwrap();

    let checkpointer = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);
    let loaded = Recovery::load(&checkpointer, &sources(ResumableSource::new(log())))
        .expect("corruption is tolerated")
        .expect("an older body exists");
    assert_eq!(loaded.id, 1);
}

#[test]
fn a_corrupt_latest_pointer_is_tolerated() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    let mut writer = backend.clone();
    writer.put(b"checkpoint/latest", vec![1, 2, 3]).unwrap();

    let checkpointer = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);
    let loaded = Recovery::load(&checkpointer, &sources(ResumableSource::new(log())))
        .expect("a damaged pointer must not abort startup")
        .expect("a valid body exists");
    assert_eq!(loaded.id, 1);
    assert_eq!(loaded.engine.epoch, 3);
}

#[test]
fn an_unknown_snapshot_version_stays_fatal() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    let mut engine = backend.get(b"checkpoint/1/engine").unwrap().unwrap();
    engine[4..8].copy_from_slice(&9u32.to_le_bytes());
    let mut writer = backend.clone();
    writer.put(b"checkpoint/1/engine", engine).unwrap();

    let checkpointer = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);
    let error = Recovery::load(&checkpointer, &sources(ResumableSource::new(log())))
        .expect_err("an incompatible version is fatal");
    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
}

#[test]
fn a_seeded_body_matches_the_live_view() {
    let backend = SharedBackend::default();
    seed_valid(&backend);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    assert_eq!(
        rows(&engine.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]]
    );
}

#[test]
fn a_storage_error_preserves_its_cause_and_display() {
    let error = ConnectorError::Storage(StateError::Io(io::Error::other("boom")));
    assert!(error.to_string().starts_with("storage: "));
    assert!(error.source().is_some());
}
