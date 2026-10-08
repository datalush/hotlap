//! An incompatible pending commit is fatal and never re-drives the sinks.

#[path = "common/backend.rs"]
mod backend;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap::state::StateBackend;
use hotlap::{Hotlap, InputId};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_engine::{EngineCore, MetricsRegistry};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::source_checkpoint::{decode_sources, encode_sources};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;

/// A bounded source with no data, enough to declare one identity.
struct StaticSource {
    schema: SchemaRef,
}

impl Source for StaticSource {
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

/// A sink that only counts the commits recovery may re-drive.
struct CountingSink {
    commits: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Sink for CountingSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

fn declared() -> Sources {
    Sources::new(vec![
        InputSource {
            id: InputId(0),
            name: "a".into(),
            source: Arc::new(StaticSource { schema: schema() }),
            watermark: None,
        },
        InputSource {
            id: InputId(1),
            name: "b".into(),
            source: Arc::new(StaticSource { schema: schema() }),
            watermark: None,
        },
    ])
    .unwrap()
}

fn engine() -> Hotlap {
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    hotlap.register_input_with_id("a", InputId(0)).unwrap();
    hotlap.register_input_with_id("b", InputId(1)).unwrap();
    hotlap
}

#[test]
fn an_incompatible_pending_commit_errors_without_committing() {
    let backend = SharedBackend::default();
    let sources = declared();
    let mut seeder = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let valid = futures::executor::block_on(seeder.take(&engine(), &sources)).unwrap();
    let pending = valid + 1;

    // Copy the valid body to `pending`, renaming one source so it validates
    // only against a different declaration, then mark the pending commit.
    let mut writer = backend.clone();
    let engine_bytes = writer
        .get(format!("checkpoint/{valid}/engine").as_bytes())
        .unwrap()
        .unwrap();
    let raw = writer
        .get(format!("checkpoint/{valid}/sources").as_bytes())
        .unwrap()
        .unwrap();
    let mut saved = decode_sources(&raw).unwrap();
    saved.entries[0].name = "renamed".into();
    writer
        .put(
            format!("checkpoint/{pending}/engine").as_bytes(),
            engine_bytes,
        )
        .unwrap();
    writer
        .put(
            format!("checkpoint/{pending}/sources").as_bytes(),
            encode_sources(&saved).unwrap(),
        )
        .unwrap();
    writer
        .put(
            format!("checkpoint/{pending}/commit").as_bytes(),
            b"1".to_vec(),
        )
        .unwrap();

    let commits = Arc::new(AtomicU32::new(0));
    let sink = Arc::new(CountingSink {
        commits: Arc::clone(&commits),
    });
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(SharedSink::new(sink))]);
    let mut hotlap = engine();
    let signal = Mutex::new(None);
    let metrics = MetricsRegistry::new();

    let result = futures::executor::block_on(Recovery::start(
        &mut hotlap,
        &sources,
        &mut checkpointer,
        &signal,
        &metrics,
    ));
    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_eq!(commits.load(Ordering::SeqCst), 0, "must not re-drive");
    assert!(
        backend
            .get(format!("checkpoint/{pending}/valid").as_bytes())
            .unwrap()
            .is_none()
    );
}
