//! Fixtures and readers shared by the float group-aggregate tests.

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

pub const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
pub const VIEW: &str = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*), sum(v), avg(v), \
     min(v), max(v) FROM src GROUP BY k;";
pub const SELECT: &str = "SELECT k, count, sum, avg, min, max FROM mv ORDER BY k";

/// `(k, count, sum, avg, min, max)`; `min`/`max` are null when the group's only
/// non-null inputs are `NaN`.
pub type Row = (i64, i64, f64, f64, Option<f64>, Option<f64>);

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
        Field::new("v", DataType::Float64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// Builds the source batches, splitting `data` across two deltas.
fn source_batches(data: &[(i64, f64)]) -> Vec<SourceBatch> {
    let half = data.len() / 2;
    [&data[..half], &data[half..]]
        .into_iter()
        .map(|rows| {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
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
        })
        .collect()
}

/// Opens a session whose source replays `data`.
pub async fn started(data: &[(i64, f64)]) -> SqlSession {
    let factory = Arc::new(FakeFactory {
        schema: schema(),
        batches: source_batches(data),
    });
    let mut session = SqlSession::open_with_factories(factory, Arc::new(FlussSinkFactory));
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql("START;").await.unwrap();
    session
}

pub fn floats(batch: &RecordBatch, index: usize) -> Vec<Option<f64>> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..array.len())
        .map(|i| (!array.is_null(i)).then(|| array.value(i)))
        .collect()
}

pub fn ints(batch: &RecordBatch, index: usize) -> Vec<i64> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..array.len()).map(|i| array.value(i)).collect()
}

/// Collects our MV's rows.
pub fn our_rows(result: QueryResult) -> Vec<Row> {
    let QueryResult::Rows(batches) = result else {
        panic!("expected a result set");
    };
    let mut out = Vec::new();
    for batch in &batches {
        let (k, count, sum) = (ints(batch, 0), ints(batch, 1), floats(batch, 2));
        let (avg, min, max) = (floats(batch, 3), floats(batch, 4), floats(batch, 5));
        for i in 0..batch.num_rows() {
            out.push((
                k[i],
                count[i],
                sum[i].unwrap(),
                avg[i].unwrap(),
                min[i],
                max[i],
            ));
        }
    }
    out
}

/// Polls the MV until `ready` holds, or the deadline passes.
pub async fn wait_for(session: &mut SqlSession, ready: impl Fn(&[Row]) -> bool) -> Vec<Row> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = our_rows(session.sql(SELECT).await.expect("mv query failed"));
        if ready(&got) || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
