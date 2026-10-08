//! Startup recovery: an engine with a checkpoint resumes from it and replays.

#[path = "common/recovery.rs"]
mod recovery;

use std::time::{Duration, Instant};

use hotlap_connectors::runtime::checkpoint::{CheckpointConfig, Checkpointer, DEFAULT_RETAIN};
use hotlap_connectors::runtime::handle::EngineHandle;
use hotlap_connectors::runtime::pipeline as runtime;
use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, pipeline, rows, take};

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3], vec![3, 4]])
}

#[test]
fn startup_recovers_from_the_last_checkpoint() {
    let backend = SharedBackend::default();
    let (mut reference, ref_pipe) = engine_with(ResumableSource::new(log()));
    let mut ref_stream = runtime::merged_stream(ref_pipe.source.as_ref()).unwrap();
    drain(&mut reference, ref_pipe.source.as_ref(), &mut ref_stream, usize::MAX);
    let expected = rows(&reference.snapshot("c").unwrap());

    // Seed a checkpoint at read offset 3.
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut seeded, seed_pipe) = engine_with(ResumableSource::new(log()));
    let mut seed_stream = runtime::merged_stream(seed_pipe.source.as_ref()).unwrap();
    drain(&mut seeded, seed_pipe.source.as_ref(), &mut seed_stream, 3);
    take(&mut checkpointer, &seeded, seed_pipe.source.as_ref());
    drop(seeded);

    // The broker retains only the tail; without recovery the engine would
    // start at offset 0 and fail to read the dropped records.
    let retained = log().with_retention(3);
    let config = CheckpointConfig {
        interval: Duration::from_secs(3600),
        backend: Box::new(backend.clone()),
        retain: DEFAULT_RETAIN,
    };
    let handle =
        EngineHandle::start(pipeline(ResumableSource::new(retained), Some(config))).unwrap();
    assert!(wait_rows(&handle, &expected));
    assert_eq!(handle.last_error().unwrap(), None);
    handle.shutdown().unwrap();
}

/// Poll the group-count view until it matches `expected`.
fn wait_rows(handle: &EngineHandle, expected: &[Vec<i64>]) -> bool {
    let snap = handle.snapshot_handle();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(zset) = snap.snapshot("c")
            && rows(&zset) == expected
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}
