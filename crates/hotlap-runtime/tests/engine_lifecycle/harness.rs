//! Source fixtures for the engine lifecycle tests.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::stream;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};

/// A source that never yields, keeping the engine alive.
pub struct PendingSource {
    pub schema: SchemaRef,
}

impl Source for PendingSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(stream::pending()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

/// A source whose stream yields one error.
pub struct FailingSource {
    pub schema: SchemaRef,
}

impl Source for FailingSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let items: Vec<Result<SourceBatch, ConnectorError>> =
            vec![Err(ConnectorError::Fluss("boom".into()))];
        Ok(Box::pin(stream::iter(items)))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

/// A source that yields one three-row batch and then ends.
pub struct OneBatchSource {
    pub schema: SchemaRef,
}

impl Source for OneBatchSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
        )
        .unwrap();
        let item = Ok(SourceBatch {
            batch,
            base_offset: 0,
            next_offset: 3,
            split: 0,
        });
        Ok(Box::pin(stream::iter(vec![item])))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

/// Read a consolidated Z-set of Int64 columns as sorted integer rows.
pub fn zset_rows(z: &hotlap::ZSetBatch) -> Vec<Vec<i64>> {
    let columns: Vec<&Int64Array> = z
        .batch
        .columns()
        .iter()
        .map(|c| c.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let mut rows: Vec<Vec<i64>> = (0..z.len())
        .map(|row| columns.iter().map(|c| c.value(row)).collect())
        .collect();
    rows.sort();
    rows
}
