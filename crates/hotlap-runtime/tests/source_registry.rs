//! Tests for the typed source set and its identity-tagged event streams.

#[path = "cross_source_support/source.rs"]
mod source_support;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::InputId;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, Split};
use hotlap_runtime::runtime::sources::{InputSource, InputStream, SourceEvent, Sources};
use source_support::ControlledSource;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

fn split(id: i32) -> Split {
    Split { id, start: 0 }
}

fn batch_on(split: i32) -> SourceBatch {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
    let rb = RecordBatch::try_new(schema(), vec![array]).unwrap();
    SourceBatch {
        batch: rb,
        base_offset: 0,
        next_offset: 1,
        split,
    }
}

fn input(id: u32, name: &str, source: Arc<ControlledSource>) -> InputSource {
    InputSource {
        id: InputId(id),
        name: name.to_string(),
        source,
        watermark: None,
    }
}

/// Await one event, failing the test on timeout, early end or stream error.
async fn next_event(stream: &mut InputStream) -> SourceEvent {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timed out")
        .expect("stream ended early")
        .expect("stream errored")
}

#[test]
fn entries_are_sorted_by_id() {
    let s = schema();
    let (a, _ta) = ControlledSource::new(s.clone(), vec![split(0)]);
    let (b, _tb) = ControlledSource::new(s, vec![split(0)]);
    let sources = Sources::new(vec![input(7, "b", b), input(2, "a", a)]).unwrap();
    let ids: Vec<u32> = sources.entries().iter().map(|e| e.id.0).collect();
    assert_eq!(ids, vec![2, 7]);
}

#[test]
fn duplicate_ids_and_names_are_rejected() {
    let s = schema();
    let (a1, _t1) = ControlledSource::new(s.clone(), vec![split(0)]);
    let (a2, _t2) = ControlledSource::new(s.clone(), vec![split(0)]);
    let (a3, _t3) = ControlledSource::new(s, vec![split(0)]);
    assert!(Sources::new(vec![input(0, "a", a1), input(0, "b", a2)]).is_err());
    assert!(Sources::new(vec![input(0, "a", a3.clone()), input(1, "a", a3)]).is_err());
    assert!(Sources::new(Vec::new()).is_err());
}

#[test]
fn unknown_lookup_is_an_error() {
    let s = schema();
    let (a, _ta) = ControlledSource::new(s, vec![split(0)]);
    let sources = Sources::new(vec![input(3, "a", a)]).unwrap();
    assert!(sources.get(InputId(3)).is_ok());
    assert!(sources.get(InputId(4)).is_err());
}

#[tokio::test]
async fn tags_events_with_their_source_id() {
    let s = schema();
    let (a, _ta) = ControlledSource::new(s.clone(), vec![split(0)]);
    let (b, b_tx) = ControlledSource::new(s, vec![split(0)]);
    let sources = Sources::new(vec![input(0, "a", a), input(1, "b", b)]).unwrap();
    let mut stream = sources.stream().unwrap();

    // Only B has data; A stays pending, so the event must be tagged as B.
    b_tx[0].send(Ok(batch_on(0))).unwrap();
    let event = next_event(&mut stream).await;
    assert_eq!(event.input, InputId(1));
    assert_eq!(event.batch.split, 0);
}

#[tokio::test]
async fn every_declared_split_is_read_and_tagged() {
    let s = schema();
    let (a, a_tx) = ControlledSource::new(s, vec![split(0), split(1)]);
    let sources = Sources::new(vec![input(0, "a", a)]).unwrap();
    let mut stream = sources.stream().unwrap();

    // Each split has its own channel; both must be opened and tagged.
    a_tx[1].send(Ok(batch_on(1))).unwrap();
    let second = next_event(&mut stream).await;
    assert_eq!((second.input, second.batch.split), (InputId(0), 1));

    a_tx[0].send(Ok(batch_on(0))).unwrap();
    let first = next_event(&mut stream).await;
    assert_eq!((first.input, first.batch.split), (InputId(0), 0));
}

#[tokio::test]
async fn two_ready_sources_are_both_consumed() {
    let s = schema();
    let (a, a_tx) = ControlledSource::new(s.clone(), vec![split(0)]);
    let (b, b_tx) = ControlledSource::new(s, vec![split(0)]);
    let sources = Sources::new(vec![input(0, "a", a), input(1, "b", b)]).unwrap();
    let mut stream = sources.stream().unwrap();

    a_tx[0].send(Ok(batch_on(0))).unwrap();
    b_tx[0].send(Ok(batch_on(0))).unwrap();
    let mut seen = vec![
        next_event(&mut stream).await.input,
        next_event(&mut stream).await.input,
    ];
    seen.sort();
    // Both are drained; the interleaving order is scheduler-dependent.
    assert_eq!(seen, vec![InputId(0), InputId(1)]);
}

#[tokio::test]
async fn remaining_source_keeps_going_after_another_closes() {
    let s = schema();
    let (a, a_tx) = ControlledSource::new(s.clone(), vec![split(0)]);
    let (b, b_tx) = ControlledSource::new(s, vec![split(0)]);
    let sources = Sources::new(vec![input(0, "a", a), input(1, "b", b)]).unwrap();
    let mut stream = sources.stream().unwrap();

    // B produces then closes; the merged stream must not end with B.
    b_tx[0].send(Ok(batch_on(0))).unwrap();
    drop(b_tx);
    assert_eq!(next_event(&mut stream).await.input, InputId(1));

    a_tx[0].send(Ok(batch_on(0))).unwrap();
    assert_eq!(next_event(&mut stream).await.input, InputId(0));
}

#[tokio::test]
async fn stream_error_propagates_instead_of_eof() {
    let s = schema();
    let (a, a_tx) = ControlledSource::new(s, vec![split(0)]);
    let sources = Sources::new(vec![input(0, "a", a)]).unwrap();
    let mut stream = sources.stream().unwrap();

    a_tx[0]
        .send(Err(ConnectorError::Infrastructure("boom".into())))
        .unwrap();
    let item = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item, Err(ConnectorError::Infrastructure(m)) if m == "boom"));

    // An error is not an EOF: the merge keeps yielding for that source.
    a_tx[0].send(Ok(batch_on(0))).unwrap();
    assert_eq!(next_event(&mut stream).await.input, InputId(0));
}

#[test]
fn stream_open_failure_is_an_error() {
    let bad = ControlledSource::failing_read(schema());
    let sources = Sources::new(vec![input(0, "bad", bad)]).unwrap();
    assert!(sources.stream().is_err());
}

#[test]
fn fixture_records_commits_and_applied_state() {
    let s = schema();
    let (a, _tx) = ControlledSource::new(s, vec![split(0)]);
    a.commit(0, 7).unwrap();
    assert_eq!(a.commits(), vec![(0, 7)]);
    assert_eq!(a.applied().offsets.get(&0), Some(&7));
}

#[test]
fn stream_with_rejects_a_split_count_mismatch_before_opening() {
    let (a, _ta) = ControlledSource::new(schema(), vec![split(0)]);
    let (b, _tb) = ControlledSource::new(schema(), vec![split(0)]);
    let sources = Sources::new(vec![input(0, "a", a), input(1, "b", b)]).unwrap();
    assert!(sources.stream_with(&[vec![split(0)]]).is_err());
    let both = [vec![split(0)], vec![split(0)]];
    assert!(sources.stream_with(&both).is_ok());
}
