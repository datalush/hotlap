use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::{Hotlap, InputId, Plan};
use hotlap_connectors::source::SourceBatch;
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

#[path = "pipeline_differential/harness.rs"]
mod harness;

use harness::{FakeSource, TwoSplit, batch, group_count, open, snap_rows};

/// Drain the pipeline's tagged stream into `hotlap`.
async fn ingest_all(hotlap: &mut Hotlap, pipeline: &Pipeline) {
    let mut stream = pipeline.sources.stream().unwrap();
    while let Some(item) = stream.next().await {
        pipeline::ingest_event(hotlap, &pipeline.sources, &item.unwrap()).unwrap();
    }
}

#[tokio::test]
async fn fake_source_feeds_view() {
    let source = Arc::new(FakeSource {
        schema: batch(&[0], &[0]).batch.schema(),
        batches: vec![batch(&[1, 1, 2], &[10, 10, 10]), batch(&[2, 3], &[20, 20])],
    });
    let (mut hotlap, pipeline) = group_count(source);
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    ingest_all(&mut hotlap, &pipeline).await;
    assert_eq!(
        snap_rows(&mut hotlap, "c"),
        vec![vec![1, 2], vec![2, 2], vec![3, 1]]
    );
}

#[tokio::test]
async fn empty_batch_is_a_noop() {
    let good = batch(&[1], &[10]);
    let empty = SourceBatch {
        batch: RecordBatch::new_empty(good.batch.schema()),
        base_offset: 0,
        next_offset: 0,
        split: 0,
    };
    let (mut hotlap, pipeline) = group_count(Arc::new(FakeSource {
        schema: good.batch.schema(),
        batches: vec![good, empty],
    }));
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    ingest_all(&mut hotlap, &pipeline).await;
    assert_eq!(snap_rows(&mut hotlap, "c"), vec![vec![1, 1]]);
}

#[tokio::test]
async fn two_splits_merge() {
    let a = batch(&[1, 1], &[10, 10]);
    let b = batch(&[1, 2], &[10, 10]);
    let source = Arc::new(TwoSplit {
        schema: a.batch.schema(),
        a: vec![a],
        b: vec![b],
    });
    let (mut hotlap, pipeline) = group_count(source);
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    ingest_all(&mut hotlap, &pipeline).await;
    assert_eq!(snap_rows(&mut hotlap, "c"), vec![vec![1, 3], vec![2, 1]]);
}

#[tokio::test]
async fn window_without_watermark_is_rejected_at_push() {
    // No event-time -> the engine must reject a windowed view.
    let source = Arc::new(FakeSource {
        schema: batch(&[0], &[0]).batch.schema(),
        batches: vec![batch(&[1], &[10])],
    });
    let pipeline = Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source,
            watermark: None,
        }])
        .unwrap(),
        views: vec![(
            "w".into(),
            Plan::TumbleCount {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                time_col: 1,
                size: 10,
            },
        )],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = open();
    pipeline::setup(&mut hotlap, &pipeline).unwrap();
    let mut stream = pipeline.sources.stream().unwrap();
    let event = stream.next().await.unwrap().unwrap();
    assert!(pipeline::ingest_event(&mut hotlap, &pipeline.sources, &event).is_err());
}
