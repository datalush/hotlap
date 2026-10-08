//! Shared fixtures for the runtime checkpoint tests.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::state::{StateBackend, StateEntry, StateError};
use hotlap::{AggSpec, InputId, Plan};
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use hotlap_runtime::runtime::checkpoint::CheckpointConfig;
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

/// In-memory backend shared with the test, so writes stay observable.
#[derive(Clone, Default)]
pub struct SharedBackend {
    map: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl StateBackend for SharedBackend {
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

/// A source that yields scripted int batches and advances its read offset.
struct ScriptSource {
    schema: SchemaRef,
    batches: Vec<Vec<i64>>,
    progress: Arc<Mutex<SourceState>>,
}

impl Source for ScriptSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, hotlap_connectors::ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, hotlap_connectors::ConnectorError> {
        let schema = self.schema.clone();
        let batches = self.batches.clone();
        let stream =
            futures::stream::iter(batches.into_iter().enumerate()).then(move |(index, rows)| {
                let schema = schema.clone();
                async move {
                    let array: ArrayRef = Arc::new(Int64Array::from(rows));
                    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
                    let item: Result<SourceBatch, hotlap_connectors::ConnectorError> =
                        Ok(SourceBatch {
                            batch,
                            base_offset: index as i64,
                            next_offset: index as i64 + 1,
                            split: 0,
                        });
                    item
                }
            });
        Ok(Box::pin(stream))
    }
    fn commit(
        &self,
        _split: SplitId,
        offset: Offset,
    ) -> Result<(), hotlap_connectors::ConnectorError> {
        self.progress.lock().unwrap().offsets.insert(0, offset);
        Ok(())
    }
    fn state(&self) -> SourceState {
        self.progress.lock().unwrap().clone()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

/// A one-view pipeline feeding three int batches through `backend`.
pub fn pipeline(backend: SharedBackend, interval: Duration, retain: usize) -> Pipeline {
    let source = Arc::new(ScriptSource {
        schema: schema(),
        batches: vec![vec![1], vec![1, 2], vec![2]],
        progress: Arc::new(Mutex::new(SourceState::default())),
    });
    Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source,
            watermark: None,
        }])
        .unwrap(),
        views: vec![(
            "c".into(),
            Plan::GroupAggregate {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                aggs: vec![AggSpec::count()],
            },
        )],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval,
            backend: Box::new(backend),
            retain,
        }),
        retention: None,
    }
}

/// Read the group-count view as sorted rows until it matches `expected`.
pub fn wait_rows(handle: &EngineHandle, expected: &[Vec<i64>]) -> bool {
    let snap = handle.snapshot_handle();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(zset) = snap.snapshot("c")
            && rows(&zset) == expected
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

/// Materialize a Z-set of Int64 columns as sorted rows.
fn rows(zset: &hotlap::ZSetBatch) -> Vec<Vec<i64>> {
    let columns: Vec<&Int64Array> = zset
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let mut out: Vec<Vec<i64>> = (0..zset.len())
        .map(|row| columns.iter().map(|column| column.value(row)).collect())
        .collect();
    out.sort();
    out
}
