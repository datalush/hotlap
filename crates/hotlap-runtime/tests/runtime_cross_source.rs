//! Runtime ingestion of two independent input sources into one join view.

#[path = "common/backend.rs"]
mod backend;
#[path = "runtime_cross_source/harness.rs"]
mod harness;

use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use hotlap::{InputId, Plan};
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;
use harness::{ControlledSource, batch_on, schema, sources, split};

/// A join of both inputs on column 0, exposed as view `j`.
fn pipeline(sources: Sources, backend: SharedBackend) -> Pipeline {
    Pipeline {
        sources,
        views: vec![(
            "j".into(),
            Plan::Join {
                left: Box::new(Plan::Source(InputId(0))),
                right: Box::new(Plan::Source(InputId(1))),
                left_key: vec![0],
                right_key: vec![0],
            },
        )],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    }
}

fn rows(zset: &hotlap::ZSetBatch) -> Vec<Vec<i64>> {
    let columns: Vec<&Int64Array> = zset
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let mut out: Vec<Vec<i64>> = (0..zset.len())
        .map(|row| columns.iter().map(|column| column.value(row)).collect())
        .collect();
    out.sort();
    out
}

fn wait_rows(handle: &EngineHandle, view: &str, expected: &[Vec<i64>]) -> bool {
    let snapshot = handle.snapshot_handle();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(zset) = snapshot.snapshot(view)
            && rows(&zset) == expected
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

fn wait_for(done: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

#[test]
fn split_zero_of_each_source_commits_independently() {
    let (a, a_tx) = ControlledSource::new(schema(), vec![split(0)]);
    let (b, b_tx) = ControlledSource::new(schema(), vec![split(0)]);
    let handle = EngineHandle::start(pipeline(
        sources(a.clone(), b.clone()),
        SharedBackend::default(),
    ))
    .unwrap();

    assert!(a.commits().is_empty());
    assert!(b.commits().is_empty());

    a_tx[0].send(Ok(batch_on(0, 1))).unwrap();
    b_tx[0].send(Ok(batch_on(0, 1))).unwrap();
    assert!(
        wait_rows(&handle, "j", &[vec![1, 1]]),
        "join must produce a match"
    );

    assert!(wait_for(
        || !a.commits().is_empty() && !b.commits().is_empty()
    ));
    assert_eq!(a.commits(), vec![(0, 1)]);
    assert_eq!(b.commits(), vec![(0, 1)]);
    assert_eq!(a.applied().offsets.get(&0), Some(&1));
    assert_eq!(b.applied().offsets.get(&0), Some(&1));

    handle.shutdown().unwrap();
}

/// One source permanently pending must not block the other or checkpointing.
#[test]
fn a_pending_source_does_not_block_ingest_or_checkpoint() {
    let (a, a_tx) = ControlledSource::new(schema(), vec![split(0)]);
    let (b, _b_tx) = ControlledSource::new(schema(), vec![split(0)]);
    let handle = EngineHandle::start(pipeline(
        sources(a.clone(), b.clone()),
        SharedBackend::default(),
    ))
    .unwrap();

    // A checkpoint before the first batch is valid and reads empty.
    assert!(handle.snapshot("j").unwrap().is_empty());
    assert!(handle.checkpoint().is_ok());

    // B never sends, so its read future stays pending; A is still ingested and
    // acked while the join view (needing both) stays empty.
    a_tx[0].send(Ok(batch_on(0, 1))).unwrap();
    assert!(wait_for(|| !a.commits().is_empty()));
    assert!(b.commits().is_empty());
    assert!(handle.snapshot("j").unwrap().is_empty());
    assert!(handle.checkpoint().is_ok());
    handle.shutdown().unwrap();
}

#[test]
fn a_source_that_cannot_open_aborts_startup() {
    let bad = ControlledSource::failing_read(schema());
    let pipeline = Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "bad".into(),
            source: bad,
            watermark: None,
        }])
        .unwrap(),
        views: vec![],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    assert!(EngineHandle::start(pipeline).is_err());
}

#[test]
fn checkpoint_after_a_normal_end_still_works() {
    let (a, a_tx) = ControlledSource::new(schema(), vec![split(0)]);
    let (b, b_tx) = ControlledSource::new(schema(), vec![split(0)]);
    let backend = SharedBackend::default();
    let handle =
        EngineHandle::start(pipeline(sources(a.clone(), b.clone()), backend.clone())).unwrap();

    a_tx[0].send(Ok(batch_on(0, 1))).unwrap();
    b_tx[0].send(Ok(batch_on(0, 1))).unwrap();
    assert!(wait_rows(&handle, "j", &[vec![1, 1]]));

    drop(a_tx);
    drop(b_tx);
    assert!(wait_for(|| a.is_exhausted() && b.is_exhausted()), "no EOF");
    let id = handle.checkpoint().expect("checkpoint after a normal end");
    assert_eq!(id, 1);

    let reader = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);
    let saved = reader.read(id).unwrap();
    assert_eq!(saved.sources.entries.len(), 2);
    assert_eq!(saved.sources.entries[0].id, InputId(0));
    assert_eq!(saved.sources.entries[1].id, InputId(1));

    handle.shutdown().unwrap();
}
