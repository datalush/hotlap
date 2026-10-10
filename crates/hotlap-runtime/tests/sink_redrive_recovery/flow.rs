//! Deposit and crash/replay drivers over real checkpoints and `Sink::write`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::executor::block_on;
use hotlap::{AggSpec, Hotlap, InputId, Plan};
use hotlap_engine::{EngineCore, MetricsRegistry};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputSource, InputStream, SourceEvent, Sources};

use super::harness::{
    Dataset, RemoteStore, ResumableSource, SharedBackend, VolatileSink, diff_column, int_column,
};

fn group_count() -> (String, Plan) {
    (
        "c".into(),
        Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![AggSpec::count()],
        },
    )
}

fn sources(source: ResumableSource) -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(source),
        watermark: None,
    }])
    .unwrap()
}

fn pipeline(dataset: Dataset) -> Pipeline {
    Pipeline {
        sources: sources(ResumableSource::new(dataset)),
        views: vec![group_count()],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    }
}

fn open(pipeline: &Pipeline) -> Hotlap {
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, pipeline).unwrap();
    hotlap.tap_view("c").unwrap();
    hotlap
}

/// Ingest one event and push its real changelog through `Sink::write`.
async fn feed(shared: &SharedSink, engine: &mut Hotlap, sources: &Sources, event: &SourceEvent) {
    pipeline::ingest_event(engine, sources, event).unwrap();
    let changes = engine.take_changes("c").unwrap();
    if !changes.is_empty() {
        shared.write_batch(changes).await.unwrap();
    }
    sources
        .get(event.input)
        .unwrap()
        .source
        .commit(event.batch.split, event.batch.next_offset)
        .unwrap();
}

fn next(stream: &mut InputStream) -> SourceEvent {
    block_on(stream.next()).expect("event").expect("ok")
}

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1], vec![2], vec![3]])
        .with_retention(0)
        .with_physical_identity("test/sink-redrive-recovery/source")
}

/// The engine's consolidated view as `key -> count`.
fn snapshot_map(engine: &mut Hotlap) -> BTreeMap<i64, i64> {
    let zset = engine.snapshot("c").unwrap();
    let keys = int_column(&zset, 0);
    let values = int_column(&zset, 1);
    let diffs = diff_column(&zset);
    (0..zset.len())
        .filter(|&index| diffs.value(index) > 0)
        .map(|index| (keys.value(index), values.value(index)))
        .collect()
}

/// Commit three events, then crash while the fourth event's commit is pending;
/// returns the output delivered before the crash.
pub async fn crash_mid_commit(backend: &SharedBackend, remote: &RemoteStore) -> BTreeMap<i64, i64> {
    let pipeline = pipeline(log());
    let mut engine = open(&pipeline);
    let (sink, entered, _release) = VolatileSink::new(remote.clone());
    let shared = SharedSink::new(sink.clone());
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(shared.clone(), "output".into(), "c".into()),
        ]);
    let mut stream = pipeline.sources.stream().unwrap();
    for _ in 0..3 {
        let event = next(&mut stream);
        feed(&shared, &mut engine, &pipeline.sources, &event).await;
    }
    let id = block_on(checkpointer.take(&engine, &pipeline.sources)).unwrap();
    assert_eq!(id, 1, "the first checkpoint is valid");
    let delivered = remote.snapshot();

    sink.set_pending(true);
    let event = next(&mut stream);
    feed(&shared, &mut engine, &pipeline.sources, &event).await;
    let take = checkpointer.take(&engine, &pipeline.sources);
    tokio::pin!(take);
    tokio::select! {
        _ = entered.notified() => {}
        result = &mut take => panic!("commit finished without staying pending: {result:?}"),
    }
    // Returning drops the pinned future mid-commit along with the checkpointer,
    // the shared sink and this writer: the body and marker stay durable, but the
    // queued rows are lost without a graceful flush.
    delivered
}

/// A fresh writer with an empty queue recovers, discards the pending commit and
/// replays; returns the offsets it read and the engine's restored snapshot.
pub async fn replay_after_crash(
    backend: &SharedBackend,
    remote: &RemoteStore,
) -> (Vec<i64>, BTreeMap<i64, i64>) {
    let pipeline = pipeline(log());
    let mut engine = open(&pipeline);
    let (sink, _, _) = VolatileSink::new(remote.clone());
    let shared = SharedSink::new(sink.clone());
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(shared.clone(), "output".into(), "c".into()),
        ]);
    match Recovery::inspect(&checkpointer, &pipeline.sources).unwrap() {
        RecoveryDecision::Discard {
            pending, fallback, ..
        } => {
            assert_eq!(pending, 2);
            assert_eq!(fallback.expect("fallback").id, 1);
        }
        other => panic!("a fresh volatile writer must not promote: {other:?}"),
    }

    let mut resumed = block_on(Recovery::start(
        &mut engine,
        &pipeline.sources,
        &mut checkpointer,
        &Mutex::new(None),
        &MetricsRegistry::new(),
    ))
    .unwrap();
    let mut offsets = Vec::new();
    while let Some(item) = block_on(resumed.next()) {
        let event = item.unwrap();
        offsets.push(event.batch.base_offset);
        feed(&shared, &mut engine, &pipeline.sources, &event).await;
        block_on(shared.commit()).unwrap();
    }

    let expected = snapshot_map(&mut engine);
    let before = remote.snapshot();
    block_on(shared.commit()).unwrap();
    assert_eq!(
        remote.snapshot(),
        before,
        "a repeated commit must not change the output"
    );
    (offsets, expected)
}
