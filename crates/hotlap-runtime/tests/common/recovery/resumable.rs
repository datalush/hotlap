//! Resumable source fixture shared by the recovery tests.
//!
//! The source replays a fixed log from a split's `start` offset and records its
//! read position, so it can be reopened at a captured checkpoint offset. A
//! `retained_from` boundary models a broker that has dropped older records,
//! exercising the insufficient-retention error.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};

/// A shared, immutable log plus the earliest offset still retained.
#[derive(Clone)]
pub struct Dataset {
    pub batches: Arc<Vec<Vec<i64>>>,
    pub retained_from: usize,
    pub physical_identity: String,
}

static NEXT_DATASET_ID: AtomicU64 = AtomicU64::new(1);

impl Dataset {
    pub fn new(batches: Vec<Vec<i64>>) -> Self {
        let store_id = NEXT_DATASET_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            batches: Arc::new(batches),
            retained_from: 0,
            physical_identity: format!("test/resumable-dataset/{store_id}"),
        }
    }

    /// Reopen the same modeled physical store under a new source object.
    pub fn with_physical_identity(mut self, identity: impl Into<String>) -> Self {
        self.physical_identity = identity.into();
        self
    }

    pub fn with_retention(mut self, retained_from: usize) -> Self {
        self.retained_from = retained_from;
        self
    }
}

/// A source whose read position is observable and resumable by offset.
pub struct ResumableSource {
    schema: SchemaRef,
    dataset: Dataset,
    progress: Arc<Mutex<SourceState>>,
}

impl ResumableSource {
    pub fn new(dataset: Dataset) -> Self {
        Self {
            schema: schema(),
            dataset,
            progress: Arc::new(Mutex::new(SourceState::default())),
        }
    }
}

impl Source for ResumableSource {
    fn physical_identity(&self) -> Option<String> {
        Some(self.dataset.physical_identity.clone())
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        let start = self.progress.lock().unwrap().offsets.get(&0).copied();
        Ok(vec![Split {
            id: 0,
            start: start.unwrap_or(0),
        }])
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        let start = split.start.max(0) as usize;
        let retained = self.dataset.retained_from;
        let schema = self.schema.clone();
        let data = Arc::clone(&self.dataset.batches);
        let stream = futures::stream::iter(start..data.len()).then(move |index| {
            let schema = schema.clone();
            let rows = data[index].clone();
            async move {
                if index < retained {
                    return Err(ConnectorError::Unsupported(format!(
                        "offset {index} is older than retention start {retained}"
                    )));
                }
                let array: ArrayRef = Arc::new(Int64Array::from(rows));
                let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
                Ok(SourceBatch {
                    batch,
                    base_offset: index as i64,
                    next_offset: index as i64 + 1,
                    split: 0,
                })
            }
        });
        Ok(Box::pin(stream))
    }

    fn commit(&self, _split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.progress.lock().unwrap().offsets.insert(0, offset);
        Ok(())
    }

    fn state(&self) -> SourceState {
        self.progress.lock().unwrap().clone()
    }

    fn event_time_column(&self) -> Option<usize> {
        None
    }

    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        for (id, offset) in &state.offsets {
            if (*offset as usize) < self.dataset.retained_from {
                return Err(ConnectorError::Unsupported(format!(
                    "split {id} offset {offset} is older than retention start {}",
                    self.dataset.retained_from
                )));
            }
        }
        let mut splits = self.splits()?;
        for split in &mut splits {
            if let Some(offset) = state.offsets.get(&split.id) {
                split.start = *offset;
            }
        }
        Ok(splits)
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

#[cfg(test)]
mod tests {
    use super::Dataset;

    #[test]
    fn cloned_dataset_keeps_store_identity_but_new_store_gets_a_distinct_identity() {
        let first = Dataset::new(vec![vec![1]]);
        let reopened = first.clone();
        let other = Dataset::new(vec![vec![1]]);

        assert_eq!(first.physical_identity, reopened.physical_identity);
        assert_ne!(first.physical_identity, other.physical_identity);
    }
}
