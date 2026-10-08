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
use hotlap_connectors::source::{SourceBatch, Split};
use hotlap_runtime::runtime::sources::{InputSource, Sources};
use source_support::ControlledSource;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

fn split0() -> Split {
    Split { id: 0, start: 0 }
}

fn batch() -> SourceBatch {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
    let rb = RecordBatch::try_new(schema(), vec![array]).unwrap();
    SourceBatch {
        batch: rb,
        base_offset: 0,
        next_offset: 1,
        split: 0,
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

#[test]
fn entries_are_sorted_by_id() {
    let s = schema();
    let (a, _ta) = ControlledSource::new(s.clone(), vec![split0()]);
    let (b, _tb) = ControlledSource::new(s, vec![split0()]);
    let sources = Sources::new(vec![input(7, "b", b), input(2, "a", a)]).unwrap();
    let ids: Vec<u32> = sources.entries().iter().map(|e| e.id.0).collect();
    assert_eq!(ids, vec![2, 7]);
}

#[test]
fn duplicate_ids_and_names_are_rejected() {
    let s = schema();
    let (a1, _t1) = ControlledSource::new(s.clone(), vec![split0()]);
    let (a2, _t2) = ControlledSource::new(s.clone(), vec![split0()]);
    let (a3, _t3) = ControlledSource::new(s, vec![split0()]);
    assert!(Sources::new(vec![input(0, "a", a1), input(0, "b", a2)]).is_err());
    assert!(Sources::new(vec![input(0, "a", a3.clone()), input(1, "a", a3)]).is_err());
    assert!(Sources::new(Vec::new()).is_err());
}

#[test]
fn unknown_lookup_is_an_error() {
    let s = schema();
    let (a, _ta) = ControlledSource::new(s, vec![split0()]);
    let sources = Sources::new(vec![input(3, "a", a)]).unwrap();
    assert!(sources.get(InputId(3)).is_ok());
    assert!(sources.get(InputId(4)).is_err());
}

#[tokio::test]
async fn tags_events_with_their_source_id() {
    let s = schema();
    let (a, _ta) = ControlledSource::new(s.clone(), vec![split0()]);
    let (b, b_tx) = ControlledSource::new(s, vec![split0()]);
    let sources = Sources::new(vec![input(0, "a", a), input(1, "b", b)]).unwrap();
    let mut stream = sources.stream().unwrap();

    // Only B has data; A stays pending, so the event must be tagged as B.
    b_tx.send(Ok(batch())).unwrap();
    let item = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(item.input, InputId(1));
    assert_eq!(item.batch.split, 0);
}

#[tokio::test]
async fn remaining_source_keeps_going_after_another_closes() {
    let s = schema();
    let (a, a_tx) = ControlledSource::new(s.clone(), vec![split0()]);
    let (b, b_tx) = ControlledSource::new(s, vec![split0()]);
    let sources = Sources::new(vec![input(0, "a", a), input(1, "b", b)]).unwrap();
    let mut stream = sources.stream().unwrap();

    // B produces then closes; the merged stream must not end with B.
    b_tx.send(Ok(batch())).unwrap();
    drop(b_tx);
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.input, InputId(1));

    a_tx.send(Ok(batch())).unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.input, InputId(0));
}

#[test]
fn stream_open_failure_is_an_error() {
    let bad = ControlledSource::failing_read(schema());
    let sources = Sources::new(vec![input(0, "bad", bad)]).unwrap();
    assert!(sources.stream().is_err());
}
