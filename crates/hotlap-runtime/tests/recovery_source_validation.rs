//! Recovery validates the declared source set before restoring or promoting.

#[path = "common/backend.rs"]
mod backend;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap::state::StateBackend;
use hotlap::{Hotlap, InputId};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_engine::{EngineCore, encode_framed};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Watermark;
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;

/// A bounded source with no data, enough to declare one identity.
struct StaticSource {
    schema: SchemaRef,
    physical_identity: String,
}

impl Source for StaticSource {
    fn physical_identity(&self) -> Option<String> {
        Some(self.physical_identity.clone())
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let stream = futures::stream::empty::<Result<SourceBatch, ConnectorError>>();
        Ok(Box::pin(stream))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

fn other_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]))
}

fn input(id: u32, name: &str, schema: SchemaRef, watermark: Option<Watermark>) -> InputSource {
    input_on_dataset(id, name, schema, watermark, &format!("cluster-a/{name}"))
}

fn input_on_dataset(
    id: u32,
    name: &str,
    schema: SchemaRef,
    watermark: Option<Watermark>,
    physical_identity: &str,
) -> InputSource {
    InputSource {
        id: InputId(id),
        name: name.to_string(),
        source: Arc::new(StaticSource {
            schema,
            physical_identity: physical_identity.to_string(),
        }),
        watermark,
    }
}

/// Declare both inputs, optionally reversed, with a bad schema or a lag.
fn declared(reversed: bool, bad_schema: bool, lag: Option<i64>) -> Sources {
    let a = input(
        0,
        "a",
        if bad_schema { other_schema() } else { schema() },
        lag.map(|lag| Watermark { lag }),
    );
    let b = input(1, "b", schema(), None);
    let entries = if reversed { vec![b, a] } else { vec![a, b] };
    Sources::new(entries).unwrap()
}

/// A real engine snapshot with both inputs registered.
fn engine() -> Hotlap {
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    hotlap.register_input_with_id("a", InputId(0)).unwrap();
    hotlap.register_input_with_id("b", InputId(1)).unwrap();
    hotlap
}

/// Persist a valid checkpoint over `sources`, returning its backend and id.
fn seed(sources: &Sources) -> (SharedBackend, u64) {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let id = futures::executor::block_on(checkpointer.take(&engine(), sources)).unwrap();
    (backend, id)
}

#[test]
fn reordered_declaration_restores_the_same_ids() {
    let (backend, id) = seed(&declared(false, false, None));
    let checkpointer = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);

    let loaded = Recovery::load(&checkpointer, &declared(true, false, None))
        .unwrap()
        .expect("checkpoint");
    assert_eq!(loaded.id, id);
    assert_eq!(loaded.sources.entries[0].id, InputId(0));
    assert_eq!(loaded.sources.entries[1].id, InputId(1));
}

#[test]
fn an_incompatible_schema_is_rejected_before_resume() {
    let (backend, _) = seed(&declared(false, false, None));
    let checkpointer = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);

    let error = Recovery::load(&checkpointer, &declared(false, true, None)).unwrap_err();
    assert!(matches!(error, ConnectorError::Unsupported(_)));
}

#[test]
fn an_incompatible_watermark_is_rejected_before_resume() {
    let (backend, _) = seed(&declared(false, false, Some(5)));
    let checkpointer = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);

    let error = Recovery::load(&checkpointer, &declared(false, false, None)).unwrap_err();
    assert!(matches!(error, ConnectorError::Unsupported(_)));
}

#[test]
fn same_name_schema_and_splits_on_another_physical_dataset_are_rejected() {
    let saved = declared(false, false, None);
    let (backend, _) = seed(&saved);
    let checkpointer = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);
    let changed = Sources::new(vec![
        input_on_dataset(0, "a", schema(), None, "cluster-a/table-a-recreated"),
        input_on_dataset(1, "b", schema(), None, "cluster-a/b"),
    ])
    .unwrap();

    let error = Recovery::load(&checkpointer, &changed).unwrap_err();

    assert!(matches!(error, ConnectorError::Unsupported(_)));
}

#[test]
fn an_older_format_checkpoint_is_rejected_instead_of_skipped() {
    let (backend, id) = seed(&declared(false, false, None));
    let mut writer = backend.clone();
    // A legacy single-source payload must surface `Unsupported`, not a clean start.
    let legacy = encode_framed(&SourceState::default()).unwrap();
    writer
        .put(format!("checkpoint/{id}/sources").as_bytes(), legacy)
        .unwrap();

    let checkpointer = Checkpointer::new(Box::new(backend), DEFAULT_RETAIN);
    let error = Recovery::load(&checkpointer, &declared(false, false, None)).unwrap_err();
    assert!(matches!(error, ConnectorError::Unsupported(_)));
}
