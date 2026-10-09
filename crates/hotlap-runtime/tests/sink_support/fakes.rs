//! Fake source/sink factories shared by the `CREATE SINK` tests.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream::{self, StreamExt};
use hotlap::ZSetBatch;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError, Sink};
use hotlap_runtime::{SinkFactory, SourceFactory, SqlSession};
use hotlap_sql::SqlError;

/// A finite in-memory source replaying a fixed list of batches.
pub struct FakeSource {
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

/// Source factory returning a [`FakeSource`].
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

/// Sink accumulating every received change batch for later inspection.
struct FakeSink {
    batches: Arc<Mutex<Vec<ZSetBatch>>>,
    accepts_retractions: bool,
}

#[async_trait::async_trait]
impl Sink for FakeSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            self.batches.lock().unwrap().push(item?);
        }
        Ok(())
    }
    fn accepts_retractions(&self) -> bool {
        self.accepts_retractions
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Sink factory handing every create call a handle to one shared accumulator.
struct FakeSinkFactory {
    batches: Arc<Mutex<Vec<ZSetBatch>>>,
    accepts_retractions: bool,
}

#[async_trait::async_trait]
impl SinkFactory for FakeSinkFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        Ok(Arc::new(FakeSink {
            batches: Arc::clone(&self.batches),
            accepts_retractions: self.accepts_retractions,
        }))
    }
}

/// Build a session wired to a fake source and a retraction-capable sink.
pub fn session(batches: Vec<SourceBatch>, sink_batches: &Arc<Mutex<Vec<ZSetBatch>>>) -> SqlSession {
    session_with(batches, sink_batches, true)
}

/// Build a session whose sink declares whether it can apply retractions.
pub fn session_with(
    batches: Vec<SourceBatch>,
    sink_batches: &Arc<Mutex<Vec<ZSetBatch>>>,
    accepts_retractions: bool,
) -> SqlSession {
    let source = Arc::new(FakeFactory {
        schema: kv_schema(),
        batches,
    });
    let sink = Arc::new(FakeSinkFactory {
        batches: Arc::clone(sink_batches),
        accepts_retractions,
    });
    SqlSession::open_with_factories(source, sink)
}

fn kv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// Build one `(k, _event_time)` source batch.
pub fn source_batch(keys: &[i64], times: &[i64]) -> SourceBatch {
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
