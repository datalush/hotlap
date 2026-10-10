//! Restart behavior at every persistent delete boundary in retention and discard.

use std::io;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use hotlap::state::{StateBackend, StateEntry, StateError};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;

#[path = "checkpoint_uncertain/support.rs"]
mod support;
use support::{Dataset, ResumableSource, SharedBackend, bag, engine_with, ingest};

#[derive(Clone)]
struct DeleteBackend {
    inner: SharedBackend,
    deletes: Arc<Mutex<Vec<Vec<u8>>>>,
    fail_at: Option<(usize, bool)>,
}

impl StateBackend for DeleteBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        self.inner.get(key)
    }
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        self.inner.put(key, value)
    }
    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        self.inner.scan(prefix)
    }
    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        self.inner.list(prefix)
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        let mut deletes = self.deletes.lock().unwrap();
        let index = deletes.len();
        deletes.push(key.to_vec());
        let failure = self.fail_at.filter(|(at, _)| *at == index);
        drop(deletes);
        if failure.is_some_and(|(_, after)| !after) {
            return Err(StateError::Io(io::Error::other("crash before delete")));
        }
        self.inner.delete(key)?;
        if failure.is_some() {
            return Err(StateError::Io(io::Error::other("crash after delete")));
        }
        Ok(())
    }
}

fn wrapped(
    backend: SharedBackend,
    fail_at: Option<(usize, bool)>,
) -> (DeleteBackend, Arc<Mutex<Vec<Vec<u8>>>>) {
    let deletes = Arc::new(Mutex::new(Vec::new()));
    (
        DeleteBackend {
            inner: backend,
            deletes: deletes.clone(),
            fail_at,
        },
        deletes,
    )
}

fn log() -> Dataset {
    Dataset::new((1..=12).map(|value| vec![value]).collect())
        .with_retention(0)
        .with_physical_identity("test/checkpoint-cleanup-crashes/log")
}

fn ingest_next(
    stream: &mut hotlap_runtime::runtime::sources::InputStream,
    engine: &mut hotlap::Hotlap,
    sources: &hotlap_runtime::runtime::sources::Sources,
) {
    let event = futures::executor::block_on(stream.next()).unwrap().unwrap();
    ingest(engine, sources, &event);
}

fn seeded() -> (
    SharedBackend,
    hotlap::Hotlap,
    hotlap_runtime::runtime::pipeline::Pipeline,
    hotlap_runtime::runtime::sources::InputStream,
) {
    let backend = SharedBackend::default();
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    let mut writer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    for _ in 0..7 {
        ingest_next(&mut stream, &mut engine, &pipe.sources);
    }
    futures::executor::block_on(writer.take(&engine, &pipe.sources)).unwrap();
    // This is a published checkpoint with stale phase markers left by cleanup.
    let mut writer_backend = backend.clone();
    writer_backend
        .put(b"checkpoint/1/prepare", b"prepare-v1".to_vec())
        .unwrap();
    writer_backend
        .put(b"checkpoint/1/commit", b"1".to_vec())
        .unwrap();
    ingest_next(&mut stream, &mut engine, &pipe.sources);
    futures::executor::block_on(writer.take(&engine, &pipe.sources)).unwrap();
    (backend, engine, pipe, stream)
}

fn clone_store(source: &SharedBackend) -> SharedBackend {
    let mut copy = SharedBackend::default();
    for (key, value) in source.scan(b"").unwrap() {
        copy.put(&key, value).unwrap();
    }
    copy
}

fn retention_run(
    seed: &SharedBackend,
    fail_at: Option<(usize, bool)>,
) -> (SharedBackend, Vec<Vec<u8>>, Result<(), String>) {
    let backend = clone_store(seed);
    let (faulty, deletes) = wrapped(backend.clone(), fail_at);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let writer = Checkpointer::new(Box::new(backend.clone()), 1);
    // Restore checkpoint 2 (eight rows) before capturing the ninth.
    let checkpoint = writer.read(2).unwrap();
    let mut stream = Recovery::resume(&mut engine, &pipe.sources, &checkpoint).unwrap();
    let event = futures::executor::block_on(stream.next()).unwrap().unwrap();
    ingest(&mut engine, &pipe.sources, &event);
    let mut writer = Checkpointer::new(Box::new(faulty), 1);
    let result = futures::executor::block_on(writer.take(&engine, &pipe.sources))
        .map(|_| ())
        .map_err(|e| format!("{e:?}"));
    let calls = deletes.lock().unwrap().clone();
    (backend, calls, result)
}

fn recover_rows(backend: &SharedBackend) -> (Vec<(Vec<i64>, i64)>, i64) {
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let mut stream = futures::executor::block_on(Recovery::start(
        &mut engine,
        &pipe.sources,
        &mut checkpointer,
        &Mutex::new(None),
        &hotlap_engine::MetricsRegistry::new(),
    ))
    .unwrap();
    let next = futures::executor::block_on(stream.next()).unwrap().unwrap();
    let next_offset = next.batch.base_offset;
    let mut rows = bag(&engine.snapshot(support::ROWS).unwrap())
        .into_iter()
        .filter(|(_, weight)| *weight != 0)
        .collect::<Vec<_>>();
    rows.sort();
    (rows, next_offset)
}

#[test]
fn every_retention_delete_boundary_restarts_with_latest_rows_and_offset() {
    let (seed, _, _, _) = seeded();
    let (_, sequence, result) = retention_run(&seed, None);
    result.unwrap();
    assert!(!sequence.is_empty());
    for index in 0..sequence.len() {
        for after in [false, true] {
            let (backend, _, result) = retention_run(&seed, Some((index, after)));
            assert!(result.is_err(), "fault at delete {index}, after={after}");
            for id in 1..=3 {
                let prefix = format!("checkpoint/{id}/");
                let keys = backend.list(prefix.as_bytes()).unwrap();
                let marked = keys.iter().any(|key| {
                    [b"/valid".as_slice(), b"/prepare", b"/commit"]
                        .iter()
                        .any(|suffix| key.ends_with(suffix))
                });
                assert!(
                    !marked || keys.iter().any(|key| key.ends_with(b"/participants")),
                    "marked checkpoint {id} lost its participant manifest"
                );
            }
            let (rows, offset) = recover_rows(&backend);
            assert_eq!(
                rows,
                (1..=9).map(|value| (vec![value], 1)).collect::<Vec<_>>(),
                "delete {index}, after={after}"
            );
            assert_eq!(offset, 9, "delete {index}, after={after}");
        }
    }
}

#[test]
fn every_discard_delete_boundary_replays_from_latest_valid_fallback() {
    // The retention matrix above exercises every key class in one checkpoint.
    // Discard uses the same key-ordering contract, with an explicit two-phase marker set.
    let (seed, _, _, _) = seeded();
    let mut seed = clone_store(&seed);
    seed.delete(b"checkpoint/2/valid").unwrap();
    seed.put(b"checkpoint/2/prepare", b"prepare-v1".to_vec())
        .unwrap();
    let probe = clone_store(&seed);
    let (faulty, calls) = wrapped(probe.clone(), None);
    let mut checkpointer = Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN);
    checkpointer.discard_commit(2).unwrap();
    let sequence = calls.lock().unwrap().clone();
    assert!(sequence.iter().any(|key| key.ends_with(b"/prepare")));
    for index in 0..sequence.len() {
        for after in [false, true] {
            let backend = clone_store(&seed);
            let (faulty, _) = wrapped(backend.clone(), Some((index, after)));
            let mut checkpointer = Checkpointer::new(Box::new(faulty), DEFAULT_RETAIN);
            assert!(checkpointer.discard_commit(2).is_err());
            let keys = backend.list(b"checkpoint/2/").unwrap();
            let marked = keys.iter().any(|key| {
                [b"/valid".as_slice(), b"/prepare", b"/commit"]
                    .iter()
                    .any(|suffix| key.ends_with(suffix))
            });
            assert!(!marked || keys.iter().any(|key| key.ends_with(b"/participants")));
            let (rows, offset) = recover_rows(&backend);
            assert_eq!(
                rows,
                (1..=7).map(|value| (vec![value], 1)).collect::<Vec<_>>(),
                "delete {index}, after={after}"
            );
            assert_eq!(offset, 7, "delete {index}, after={after}");
        }
    }
}
