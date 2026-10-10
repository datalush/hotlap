//! A durable `START` that consumed its checkpoint config and failed must not be
//! retried as a non-durable start. The checkpoint backend is moved into the
//! engine at `START` and is not cloneable, so once it is consumed a failed
//! attempt leaves the session failed until it is rebuilt with a fresh config.

#[path = "common/backend.rs"]
mod backend;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::stream;
use hotlap::{Hotlap, InputId};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::sources::{InputSource, Sources};
use hotlap_runtime::{FlussSinkFactory, SourceFactory, SqlSession};
use hotlap_sql::SqlError;

use backend::SharedBackend;

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";

fn session_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// Counts reads so the test can prove a refused retry never opens the source.
struct QuietSource {
    reads: Arc<AtomicU32>,
}

impl Source for QuietSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/sql-session-durable-start/source-store".into())
    }

    fn schema(&self) -> SchemaRef {
        session_schema()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }

    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::empty()))
    }

    fn state(&self) -> SourceState {
        SourceState::default()
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

struct QuietFactory {
    reads: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl SourceFactory for QuietFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        Ok(Box::new(QuietSource {
            reads: Arc::clone(&self.reads),
        }))
    }
}

/// A single-column source used only to seed a checkpoint whose schema differs
/// from the session's, so recovery rejects it before consuming any record.
struct SeedSource;

impl Source for SeedSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/sql-session-durable-start/source-store".into())
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }

    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(stream::empty()))
    }

    fn state(&self) -> SourceState {
        SourceState::default()
    }

    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

/// Seed a real checkpoint whose saved schema cannot match the session's source.
async fn mismatched_backend() -> SharedBackend {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let sources = Sources::new(vec![InputSource {
        id: InputId(0),
        name: "src".into(),
        source: Arc::new(SeedSource),
        watermark: None,
    }])
    .unwrap();
    let mut engine = Hotlap::open_with(Box::new(EngineCore::new()));
    engine.register_input_with_id("src", InputId(0)).unwrap();
    checkpointer.take(&engine, &sources).await.unwrap();
    backend
}

#[tokio::test]
async fn a_failed_durable_start_is_not_retried_without_its_checkpoint() {
    let reads = Arc::new(AtomicU32::new(0));
    let mut session = SqlSession::open_with_factories(
        Arc::new(QuietFactory {
            reads: Arc::clone(&reads),
        }),
        Arc::new(FlussSinkFactory),
    )
    .with_checkpoint(CheckpointConfig {
        interval: Duration::from_secs(3600),
        backend: Box::new(mismatched_backend().await),
        retain: DEFAULT_RETAIN,
    });
    session.sql(SOURCE).await.unwrap();
    assert!(
        session.sql("START;").await.is_err(),
        "an incompatible checkpoint must fail START"
    );
    assert!(
        session.sql("START;").await.is_err(),
        "the retry must not bypass the consumed durable config"
    );
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "neither attempt may open the source"
    );
}
