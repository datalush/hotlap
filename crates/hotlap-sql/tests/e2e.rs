//! End-to-end tests for the embedded `SqlSession`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_sql::{QueryResult, SourceFactory, SqlError, SqlSession};

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src \
     GROUP BY k, tumble(_event_time, INTERVAL '10 s');";

struct FakeSource {
    schema: SchemaRef,
    batches: Vec<SourceBatch>,
}

impl Source for FakeSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let items: Vec<Result<SourceBatch, ConnectorError>> =
            self.batches.iter().cloned().map(Ok).collect();
        Ok(Box::pin(stream::iter(items)))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

struct FakeFactory {
    schema: SchemaRef,
    batches: Vec<SourceBatch>,
}

impl SourceFactory for FakeFactory {
    fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        let source = FakeSource {
            schema: self.schema.clone(),
            batches: self.batches.clone(),
        };
        Ok(Box::new(source))
    }
}

fn kv_schema() -> SchemaRef {
    let fields = vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ];
    Arc::new(Schema::new(fields))
}

fn batch(keys: &[i64], times: &[i64]) -> SourceBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys.to_vec())),
        Arc::new(Int64Array::from(times.to_vec())),
    ];
    SourceBatch {
        batch: RecordBatch::try_new(kv_schema(), cols).unwrap(),
        base_offset: 0,
    }
}

fn factory(batches: Vec<SourceBatch>) -> Arc<FakeFactory> {
    let factory = FakeFactory {
        schema: kv_schema(),
        batches,
    };
    Arc::new(factory)
}

fn col(batch: &RecordBatch, index: usize) -> &Int64Array {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}

fn rows(result: QueryResult) -> Vec<(i64, i64, i64)> {
    let QueryResult::Rows(batches) = result else {
        panic!("expected a result set");
    };
    let mut out = Vec::new();
    for batch in batches {
        let (k, w, c) = (col(&batch, 0), col(&batch, 1), col(&batch, 2));
        for i in 0..batch.num_rows() {
            out.push((k.value(i), w.value(i), c.value(i)));
        }
    }
    out
}

/// Full recomputation of the closed tumbling-window counts from the raw events.
fn recompute(batches: &[SourceBatch], size: i64, lag: i64) -> Vec<(i64, i64, i64)> {
    let mut events = Vec::new();
    for b in batches {
        for i in 0..b.batch.num_rows() {
            events.push((col(&b.batch, 0).value(i), col(&b.batch, 1).value(i)));
        }
    }
    let max_ts = events.iter().map(|(_, t)| *t).max().unwrap_or(0);
    let watermark = (max_ts - lag).max(0);
    let mut counts: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for (key, ts) in &events {
        let start = (ts / size) * size;
        if watermark >= start + size {
            *counts.entry((*key, start)).or_insert(0) += 1;
        }
    }
    counts.into_iter().map(|((k, w), c)| (k, w, c)).collect()
}

async fn started(batches: Vec<SourceBatch>) -> SqlSession {
    let mut session = SqlSession::open(factory(batches));
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql("START;").await.unwrap();
    session
}

async fn wait_for_rows(session: &mut SqlSession, want: &[(i64, i64, i64)]) -> Vec<(i64, i64, i64)> {
    let query = "SELECT k, window_start, count FROM mv ORDER BY k, window_start";
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = rows(session.sql(query).await.expect("mv query failed"));
        if got == want || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn sql_result_matches_full_recomputation() {
    let data = vec![
        batch(&[1, 1, 1, 2], &[1000, 2000, 3000, 1000]),
        batch(&[1, 2], &[12000, 12000]),
        batch(&[1], &[21000]),
    ];
    let mut session = started(data.clone()).await;
    let want = recompute(&data, 10_000, 1_000);
    assert_eq!(wait_for_rows(&mut session, &want).await, want);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_mv_select() {
    let mut session = started(vec![]).await;
    let query = session.sql("SELECT k, window_start, count FROM mv");
    let result = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("empty MV query hung");
    assert!(rows(result.unwrap()).is_empty());
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn ddl_after_start_rejected() {
    let mut session = started(vec![batch(&[1], &[1000])]).await;
    assert!(session.sql(VIEW).await.is_err(), "DDL after START");
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_session_stops_engine() {
    // Dropping without `shutdown().await` must not panic on the executor.
    let session = started(vec![batch(&[1], &[1000])]).await;
    drop(session);
}
