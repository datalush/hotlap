//! Independent per-source watermarks through the runtime pipeline.
//!
//! Two sources on split 0 declare the same watermark policy. A fast source
//! racing ahead must neither drop the slow source's valid records nor close the
//! slow source's tumble window; each input keeps its own frontier.

#[path = "cross_source_support/sql_source.rs"]
mod sql_source;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap::{InputId, Plan, ZSetBatch};
use hotlap_connectors::source::SourceBatch;
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, Watermark};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use sql_source::{BatchSender, SqlSource};

/// `(k, value, _event_time)` with event-time at index 2.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

/// An append batch `(k, 0, event_time)` over the shared schema.
fn batch(k: i64, event_time: i64) -> SourceBatch {
    let columns = vec![ints(&[k]), ints(&[0]), ints(&[event_time])];
    let batch = RecordBatch::try_new(schema(), columns).unwrap();
    SourceBatch {
        batch,
        base_offset: 0,
        next_offset: 1,
        split: 0,
    }
}

/// The window output `(key, window_start, count)` as sorted tuples.
fn window_rows(zset: &ZSetBatch) -> Vec<(i64, i64, i64)> {
    if zset.is_empty() {
        return Vec::new();
    }
    let columns: Vec<&Int64Array> = zset
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let mut out: Vec<(i64, i64, i64)> = (0..zset.len())
        .map(|row| {
            (
                columns[0].value(row),
                columns[1].value(row),
                columns[2].value(row),
            )
        })
        .collect();
    out.sort_unstable();
    out
}

fn wait_commits(source: &SqlSource, count: usize) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if source.commits().len() >= count {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

fn wait_late(handle: &EngineHandle, input: &str, want: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = handle.late_dropped(input).expect("late_dropped");
        if got == want || Instant::now() >= deadline {
            return got;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn wait_window(handle: &EngineHandle, want: &[(i64, i64, i64)]) -> Vec<(i64, i64, i64)> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = window_rows(&handle.snapshot("wb").expect("window snapshot"));
        if got == want || Instant::now() >= deadline {
            return got;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Drives the fast/slow sequence and checks the watermarks and window.
fn drive(
    handle: &EngineHandle,
    a: &SqlSource,
    b: &SqlSource,
    a_tx: &[BatchSender],
    b_tx: &[BatchSender],
) {
    // B is slow (ts 5); A races ahead to ts 100.
    b_tx[0].send(Ok(batch(1, 5))).unwrap();
    a_tx[0].send(Ok(batch(1, 100))).unwrap();
    assert!(wait_commits(a, 1) && wait_commits(b, 1));

    // B advances inside its window but stays below its own 10 boundary.
    b_tx[0].send(Ok(batch(1, 8))).unwrap();
    assert!(wait_commits(b, 2));
    assert!(
        wait_window(handle, &[]).is_empty(),
        "A's advance must not close B's window"
    );

    // A record below A's own watermark (100) is late; B's records are valid.
    a_tx[0].send(Ok(batch(2, 10))).unwrap();
    assert_eq!(wait_late(handle, "a", 1), 1);
    assert_eq!(handle.late_dropped("b").unwrap(), 0);

    // B reaches 15: only now does [0, 10) close, with both valid records.
    b_tx[0].send(Ok(batch(1, 15))).unwrap();
    assert_eq!(wait_window(handle, &[(1, 0, 2)]), vec![(1, 0, 2)]);
    assert_eq!(handle.late_dropped("b").unwrap(), 0);
}

#[test]
fn slow_source_watermark_is_independent_of_a_fast_source() {
    let (a, a_tx) = SqlSource::new(schema(), 2);
    let (b, b_tx) = SqlSource::new(schema(), 2);
    let sources = Sources::new(vec![
        InputSource {
            id: InputId(0),
            name: "a".into(),
            source: a.clone(),
            watermark: Some(Watermark { lag: 0 }),
        },
        InputSource {
            id: InputId(1),
            name: "b".into(),
            source: b.clone(),
            watermark: Some(Watermark { lag: 0 }),
        },
    ])
    .unwrap();
    let handle = EngineHandle::start(Pipeline {
        sources,
        views: vec![(
            "wb".into(),
            Plan::TumbleCount {
                input: Box::new(Plan::Source(InputId(1))),
                key: vec![0],
                time_col: 2,
                size: 10,
            },
        )],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    })
    .unwrap();

    drive(&handle, &a, &b, &a_tx, &b_tx);
    handle.shutdown().unwrap();
}
