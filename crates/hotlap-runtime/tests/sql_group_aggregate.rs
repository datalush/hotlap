//! End-to-end `GROUP BY` aggregates: count/sum/avg must match recomputation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_runtime::{FlussSinkFactory, QueryResult, SourceFactory, SqlSession};
use hotlap_sql::SqlError;

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*), sum(v), avg(v) \
     FROM src GROUP BY k;";

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
        Some(2)
    }
}

struct FakeFactory {
    schema: SchemaRef,
    batches: Vec<SourceBatch>,
}

#[async_trait::async_trait]
impl SourceFactory for FakeFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        Ok(Box::new(FakeSource {
            schema: self.schema.clone(),
            batches: self.batches.clone(),
        }))
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

fn batch(rows: &[(i64, i64)]) -> SourceBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(vec![0i64; rows.len()])),
    ];
    SourceBatch {
        batch: RecordBatch::try_new(schema(), columns).unwrap(),
        base_offset: 0,
        next_offset: rows.len() as i64,
        split: 0,
    }
}

fn factory(batches: Vec<SourceBatch>) -> Arc<FakeFactory> {
    Arc::new(FakeFactory {
        schema: schema(),
        batches,
    })
}

fn ints(batch: &RecordBatch, index: usize) -> Vec<i64> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn floats(batch: &RecordBatch, index: usize) -> Vec<f64> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn rows(result: QueryResult) -> Vec<(i64, i64, i64, f64)> {
    let QueryResult::Rows(batches) = result else {
        panic!("expected a result set");
    };
    let mut out = Vec::new();
    for batch in batches {
        let (keys, counts, sums) = (ints(&batch, 0), ints(&batch, 1), ints(&batch, 2));
        let avgs = floats(&batch, 3);
        for i in 0..batch.num_rows() {
            out.push((keys[i], counts[i], sums[i], avgs[i]));
        }
    }
    out
}

/// Full recomputation of count/sum/avg per key from the raw events.
fn recompute(batches: &[SourceBatch]) -> Vec<(i64, i64, i64, f64)> {
    let mut groups: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    for batch in batches {
        let keys = ints(&batch.batch, 0);
        let values = ints(&batch.batch, 1);
        for (key, value) in keys.into_iter().zip(values) {
            let entry = groups.entry(key).or_insert((0, 0));
            entry.0 += 1;
            entry.1 += value;
        }
    }
    groups
        .into_iter()
        .map(|(key, (count, sum))| (key, count, sum, sum as f64 / count as f64))
        .collect()
}

async fn started(batches: Vec<SourceBatch>) -> SqlSession {
    let mut session = SqlSession::open_with_factories(factory(batches), Arc::new(FlussSinkFactory));
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql("START;").await.unwrap();
    session
}

async fn wait_for_rows(
    session: &mut SqlSession,
    want: &[(i64, i64, i64, f64)],
) -> Vec<(i64, i64, i64, f64)> {
    let query = "SELECT k, count, sum, avg FROM mv ORDER BY k";
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
async fn grouped_aggregates_match_full_recomputation() {
    let data = vec![
        batch(&[(1, 10), (1, 20), (2, 7)]),
        batch(&[(1, 30), (2, 3), (3, 5)]),
        batch(&[(1, 40)]),
    ];
    let mut session = started(data.clone()).await;
    let want = recompute(&data);
    assert_eq!(wait_for_rows(&mut session, &want).await, want);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_grouped_aggregate_select() {
    let mut session = started(vec![]).await;
    let query = session.sql("SELECT k, count, sum, avg FROM mv");
    let result = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("empty MV query hung");
    assert!(rows(result.unwrap()).is_empty());
    session.shutdown().await.unwrap();
}
