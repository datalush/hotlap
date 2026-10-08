use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream;
use hotlap::{InputId, Plan};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::runtime::handle::EngineHandle;
use hotlap_connectors::runtime::pipeline::Pipeline;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};

struct PendingSource {
    schema: SchemaRef,
}

impl Source for PendingSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, hotlap_connectors::ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, hotlap_connectors::ConnectorError> {
        // Never yields: keeps the engine alive while we exercise the handle.
        Ok(Box::pin(stream::pending()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

#[test]
fn start_snapshot_shutdown() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let handle = EngineHandle::start(Pipeline {
        input: "in".into(),
        source: Box::new(PendingSource { schema }),
        watermark: None,
        views: vec![],
        sinks: vec![],
        checkpoint: None,
    })
    .unwrap();
    // No view registered: snapshot of an unknown view errors, but the handle must respond.
    assert!(handle.snapshot("nope").is_err());
    // The derived handle can read snapshots too, without owning the engine.
    let snap = handle.snapshot_handle();
    assert!(snap.snapshot("nope").is_err());
    assert!(snap.last_error().unwrap().is_none());
    handle.shutdown().unwrap();
}

struct FailingSource {
    schema: SchemaRef,
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

struct OneBatchSource {
    schema: SchemaRef,
}

impl Source for OneBatchSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let schema = self.schema.clone();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
        )
        .unwrap();
        let item = Ok(SourceBatch {
            batch,
            base_offset: 0,
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

#[test]
fn snapshot_handle_reads_a_built_view() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let handle = EngineHandle::start(Pipeline {
        input: "in".into(),
        source: Box::new(OneBatchSource {
            schema: schema.clone(),
        }),
        watermark: None,
        views: vec![(
            "c".into(),
            Plan::GroupCount {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
            },
        )],
        sinks: vec![],
        checkpoint: None,
    })
    .unwrap();
    let snap = handle.snapshot_handle();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !snap.is_built() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(snap.is_built(), "engine never built the dataflow");
    let rows = zset_rows(&snap.snapshot("c").unwrap());
    assert_eq!(rows, vec![vec![1, 2], vec![2, 1]]);
    handle.shutdown().unwrap();
}

/// Read a consolidated Z-set of Int64 columns as sorted integer rows.
fn zset_rows(z: &hotlap::ZSetBatch) -> Vec<Vec<i64>> {
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

#[test]
fn source_error_is_surfaced() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let handle = EngineHandle::start(Pipeline {
        input: "in".into(),
        source: Box::new(FailingSource { schema }),
        watermark: None,
        views: vec![],
        sinks: vec![],
        checkpoint: None,
    })
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut raised = false;
    while Instant::now() < deadline {
        if handle.last_error().unwrap().is_some() {
            raised = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(raised, "engine did not surface the source error");
    handle.shutdown().unwrap();
}

#[test]
fn start_reports_setup_failure() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let result = EngineHandle::start(Pipeline {
        input: "in".into(),
        source: Box::new(PendingSource { schema }),
        watermark: None,
        // A view referencing an unknown input fails during setup.
        views: vec![(
            "c".into(),
            Plan::GroupCount {
                input: Box::new(Plan::Source(InputId(99))),
                key: vec![0],
            },
        )],
        sinks: vec![],
        checkpoint: None,
    });
    assert!(result.is_err());
}
