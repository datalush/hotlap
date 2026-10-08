//! Fake source/sink fixtures for the REPL smoke test.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink};
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_runtime::{SinkFactory, SourceFactory};
use hotlap_sql::SqlError;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// Build one two-column (`k`, `_event_time`) source batch.
pub fn batch(keys: &[i64], times: &[i64]) -> SourceBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys.to_vec())),
        Arc::new(Int64Array::from(times.to_vec())),
    ];
    SourceBatch {
        batch: RecordBatch::try_new(schema(), columns).unwrap(),
        base_offset: 0,
        next_offset: keys.len() as i64,
        split: 0,
    }
}

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

struct FakeSourceFactory {
    schema: SchemaRef,
    batches: Vec<SourceBatch>,
}

#[async_trait::async_trait]
impl SourceFactory for FakeSourceFactory {
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

/// Build a source factory replaying `batches`.
pub fn source_factory(batches: Vec<SourceBatch>) -> Arc<dyn SourceFactory> {
    Arc::new(FakeSourceFactory {
        schema: schema(),
        batches,
    })
}

/// Sink factory returning a no-op sink (the smoke test declares none).
pub struct FakeSinkFactory;

#[async_trait::async_trait]
impl SinkFactory for FakeSinkFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        Ok(Arc::new(FakeSink))
    }
}

struct FakeSink;

#[async_trait::async_trait]
impl Sink for FakeSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        use futures::StreamExt;
        while let Some(item) = changes.next().await {
            item?;
        }
        Ok(())
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
