//! Fixtures for the embedded [`Session`](hotlap_runtime::Session) test.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream;
use hotlap::state::{StateBackend, StateEntry, StateError};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_runtime::{QueryResult, Session, SourceFactory};
use hotlap_sql::SqlError;

/// In-memory checkpoint store, so the manual checkpoint stays observable.
#[derive(Clone, Default)]
pub struct MemBackend {
    map: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl StateBackend for MemBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        self.map.lock().unwrap().insert(key.to_vec(), value);
        Ok(())
    }
    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        self.map.lock().unwrap().remove(key);
        Ok(())
    }
}

struct FakeSource {
    schema: SchemaRef,
    batches: Vec<SourceBatch>,
}

impl Source for FakeSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/session-fake-dataset".into())
    }

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

fn kv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// Build one two-column (`k`, `_event_time`) source batch.
pub fn batch(keys: &[i64], times: &[i64]) -> SourceBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys.to_vec())),
        Arc::new(Int64Array::from(times.to_vec())),
    ];
    SourceBatch {
        batch: RecordBatch::try_new(kv_schema(), cols).unwrap(),
        base_offset: 0,
        next_offset: keys.len() as i64,
        split: 0,
    }
}

/// Build a source factory replaying `batches`.
pub fn factory(batches: Vec<SourceBatch>) -> Arc<dyn SourceFactory> {
    Arc::new(FakeFactory {
        schema: kv_schema(),
        batches,
    })
}

/// Borrow the int64 column `index` of `batch`.
pub fn col(batch: &RecordBatch, index: usize) -> &Int64Array {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}

/// Read `QueryResult::Rows` as `(k, window_start, count)` tuples.
pub fn rows(result: QueryResult) -> Vec<(i64, i64, i64)> {
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

/// Poll `SELECT` on `view` until it equals `want`, or the deadline passes.
pub fn wait_for_rows(
    session: &mut Session,
    view: &str,
    want: &[(i64, i64, i64)],
) -> Vec<(i64, i64, i64)> {
    let query = format!("SELECT k, window_start, count FROM {view} ORDER BY k, window_start");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = rows(session.sql(&query).expect("mv query failed"));
        if got == want || Instant::now() >= deadline {
            return got;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
