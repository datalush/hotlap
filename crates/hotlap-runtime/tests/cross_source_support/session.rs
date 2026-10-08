//! Shared schemas, batches and polling helpers for the SQL cross-source tests.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap_connectors::source::SourceBatch;
use hotlap_runtime::{QueryResult, SqlSession};

use crate::factory::CrossSourceFactory;

/// Source `a`: `(k, _event_time)` with the event-time column at index 1.
pub fn schema_a() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// Source `b`: `(k, value, _event_time)` with the event-time column at index 2.
pub fn schema_b() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// An Arrow array of `i64`, for building source batches.
pub fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

/// A split-0 source batch over `schema` with `columns`.
pub fn batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> SourceBatch {
    let rows = columns.first().map(|c| c.len()).unwrap_or(0);
    SourceBatch {
        batch: RecordBatch::try_new(schema, columns).unwrap(),
        base_offset: 0,
        next_offset: rows as i64,
        split: 0,
    }
}

fn int_column(batch: &RecordBatch, index: usize) -> &Int64Array {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}

/// Read a two-column result set into sorted `(i64, i64)` rows.
pub fn two_ints(result: QueryResult) -> Vec<(i64, i64)> {
    let QueryResult::Rows(batches) = result else {
        panic!("expected a result set");
    };
    let mut out = Vec::new();
    for batch in batches {
        let (left, right) = (int_column(&batch, 0), int_column(&batch, 1));
        for row in 0..batch.num_rows() {
            out.push((left.value(row), right.value(row)));
        }
    }
    out.sort_unstable();
    out
}

/// Poll a two-column view query until it equals `want`.
pub async fn wait_pairs(
    session: &mut SqlSession,
    query: &str,
    want: &[(i64, i64)],
) -> Vec<(i64, i64)> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = two_ints(session.sql(query).await.expect("view query"));
        if got == want || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Poll until every named source has acked at least one batch.
pub async fn wait_acks(factory: &CrossSourceFactory, names: &[&str]) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if names
            .iter()
            .all(|name| !factory.source(name).commits().is_empty())
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

/// Push `batch` into the single split of source `name`.
pub fn send(factory: &CrossSourceFactory, name: &str, batch: SourceBatch) {
    factory.senders(name)[0]
        .send(Ok(batch))
        .expect("source channel open");
}
