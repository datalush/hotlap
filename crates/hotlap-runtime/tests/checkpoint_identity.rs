//! Checkpoint identity: reserve before an ambiguous attempt; never reuse an id.

#[path = "common/fault.rs"]
mod fault;
#[path = "common/recovery.rs"]
mod recovery;

use hotlap::state::StateBackend;
use hotlap_engine::decode_snapshot;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::InputStream;

use fault::FaultBackend;
use recovery::{Dataset, ResumableSource, SharedBackend, engine_with, rows, take};

/// The fixed log every attempt reads from; retention keeps all records.
fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3], vec![3, 4]]).with_retention(0)
}

/// Applied offset of the single source.
fn offset(pipe: &Pipeline) -> i64 {
    *pipe.sources.entries()[0]
        .source
        .state()
        .offsets
        .get(&0)
        .expect("source offset")
}

/// Drain `n` events, returning the engine epoch and source offset afterwards.
fn advance(
    engine: &mut hotlap::Hotlap,
    pipe: &Pipeline,
    stream: &mut InputStream,
    n: usize,
) -> (u64, i64) {
    recovery::drain(engine, &pipe.sources, stream, n);
    (engine.checkpoint().unwrap().epoch, offset(pipe))
}

#[test]
fn a_published_id_is_not_overwritten_after_a_valid_ack_ambiguity() {
    let backend = SharedBackend::default();
    let faulty = FaultBackend::new(backend.clone());
    let mut checkpointer = Checkpointer::new(Box::new(faulty.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    let (e1, o1) = advance(&mut engine, &pipe, &mut stream, 3);

    // `valid` becomes durable, but publishing `latest` fails.
    faulty.fail("put", b"checkpoint/latest", false);
    assert!(futures::executor::block_on(checkpointer.take(&engine, &pipe.sources)).is_err());
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/latest").unwrap(), None);
    let first_engine = backend.get(b"checkpoint/1/engine").unwrap().unwrap();

    // A second, different attempt must get a fresh id and leave id 1 untouched.
    let (e2, o2) = advance(&mut engine, &pipe, &mut stream, 2);
    assert_ne!(e1, e2);
    assert_ne!(o1, o2);
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources), 2);
    assert_eq!(
        backend.get(b"checkpoint/1/engine").unwrap().unwrap(),
        first_engine
    );

    let reader = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let checkpoint = reader.read(2).unwrap();
    assert_eq!(checkpoint.engine.epoch, e2);
    assert_eq!(
        checkpoint.sources.entries[0].state.offsets.get(&0),
        Some(&o2)
    );
    assert_eq!(
        rows(&engine.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2], vec![3, 2], vec![4, 1]]
    );
}

#[test]
fn a_failed_prune_after_valid_does_not_reopen_the_id() {
    let backend = SharedBackend::default();
    let mut seeder = Checkpointer::new(Box::new(backend.clone()), 1);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    advance(&mut engine, &pipe, &mut stream, 3);
    assert_eq!(take(&mut seeder, &engine, &pipe.sources), 1);

    // Publishing id 2 succeeds, but pruning the older id fails: the id is
    // already published and must not be handed out again.
    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("delete", b"checkpoint/1/engine", false);
    let mut checkpointer = Checkpointer::new(Box::new(faulty), 1);
    assert!(futures::executor::block_on(checkpointer.take(&engine, &pipe.sources)).is_err());
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
    assert_eq!(
        backend.get(b"checkpoint/latest").unwrap(),
        Some(2u64.to_le_bytes().to_vec())
    );
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources), 3);
}

#[test]
fn a_crash_before_publish_does_not_reuse_the_partial_id() {
    let backend = SharedBackend::default();
    let faulty = FaultBackend::new(backend.clone());
    let mut first = Checkpointer::new(Box::new(faulty.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    let (e1, _) = advance(&mut engine, &pipe, &mut stream, 3);

    faulty.fail("put", b"checkpoint/1/sources", false);
    assert!(futures::executor::block_on(first.take(&engine, &pipe.sources)).is_err());
    assert!(backend.get(b"checkpoint/1/engine").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/1/valid").unwrap(), None);
    drop(first);

    // A new checkpointer over the same store must not start at id 1.
    let mut second = Checkpointer::new(Box::new(faulty.clone()), DEFAULT_RETAIN);
    let (e2, _) = advance(&mut engine, &pipe, &mut stream, 2);
    assert_eq!(take(&mut second, &engine, &pipe.sources), 2);
    let bytes = backend.get(b"checkpoint/1/engine").unwrap().unwrap();
    assert_eq!(decode_snapshot(&bytes).unwrap().epoch, e1);
    assert_ne!(e1, e2);
}

#[test]
fn resume_after_respects_the_durable_reservation() {
    let backend = SharedBackend::default();
    let mut writer = backend.clone();
    writer
        .put(b"checkpoint/reserved", 9u64.to_le_bytes().to_vec())
        .unwrap();

    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    checkpointer.resume_after(5);
    let (engine, pipe) = engine_with(ResumableSource::new(log()));
    assert_eq!(
        take(&mut checkpointer, &engine, &pipe.sources),
        10,
        "a recovered id below the reservation must not be reused"
    );
}

#[test]
fn a_reservation_survives_pruning_and_new_checkpointers() {
    let backend = SharedBackend::default();
    let mut first = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (engine, pipe) = engine_with(ResumableSource::new(log()));
    assert_eq!(take(&mut first, &engine, &pipe.sources), 1);
    drop(first);

    let mut writer = backend.clone();
    for key in writer.list(b"checkpoint/").unwrap() {
        if key != b"checkpoint/reserved" {
            writer.delete(&key).unwrap();
        }
    }
    assert_eq!(
        writer.list(b"checkpoint/").unwrap(),
        vec![b"checkpoint/reserved".to_vec()]
    );

    let mut second = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    assert_eq!(take(&mut second, &engine, &pipe.sources), 2);
}

#[test]
fn an_exhausted_id_space_fails_instead_of_wrapping() {
    let backend = SharedBackend::default();
    let mut writer = backend.clone();
    writer
        .put(b"checkpoint/reserved", u64::MAX.to_le_bytes().to_vec())
        .unwrap();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (engine, pipe) = engine_with(ResumableSource::new(log()));
    assert!(futures::executor::block_on(checkpointer.take(&engine, &pipe.sources)).is_err());
    assert_eq!(
        backend.list(b"checkpoint/0/").unwrap(),
        Vec::<Vec<u8>>::new()
    );
}

#[test]
fn an_ambiguous_engine_write_still_advances_the_next_id() {
    let backend = SharedBackend::default();
    let faulty = FaultBackend::new(backend.clone());
    let mut checkpointer = Checkpointer::new(Box::new(faulty.clone()), DEFAULT_RETAIN);
    let (engine, pipe) = engine_with(ResumableSource::new(log()));

    // Durable write, reported failure: the classic ambiguous acknowledgement.
    faulty.fail("put", b"checkpoint/1/engine", true);
    assert!(futures::executor::block_on(checkpointer.take(&engine, &pipe.sources)).is_err());
    assert!(backend.get(b"checkpoint/1/engine").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/1/sources").unwrap(), None);
    assert_eq!(take(&mut checkpointer, &engine, &pipe.sources), 2);
}
