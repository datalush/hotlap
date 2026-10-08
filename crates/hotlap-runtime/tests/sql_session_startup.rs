//! Startup failure and retry must leave the session on its latest registry.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::stream;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_runtime::{FlussSinkFactory, QueryResult, SourceFactory, SqlSession};
use hotlap_sql::SqlError;

const SOURCE: &str = "CREATE SOURCE %s WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// A source whose first `read` fails and then succeeds with an empty stream.
struct OpenOnce {
    failed: AtomicBool,
}

impl OpenOnce {
    fn new() -> Self {
        Self {
            failed: AtomicBool::new(false),
        }
    }
}

impl Source for OpenOnce {
    fn schema(&self) -> SchemaRef {
        schema()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        if !self.failed.swap(true, Ordering::SeqCst) {
            return Err(ConnectorError::Infrastructure(format!(
                "open failed for split {}",
                split.id
            )));
        }
        Ok(Box::pin(stream::empty()))
    }

    fn state(&self) -> SourceState {
        SourceState::default()
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

/// A source with no splits, so it never produces a batch or a failure.
struct EmptySource;

impl Source for EmptySource {
    fn schema(&self) -> SchemaRef {
        schema()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(Vec::new())
    }

    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(stream::empty()))
    }

    fn state(&self) -> SourceState {
        SourceState::default()
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

/// Fails `bad` once, so the first `START` fails and a retry can succeed.
struct StartupFactory;

#[async_trait::async_trait]
impl SourceFactory for StartupFactory {
    async fn create(
        &self,
        name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        if name == "bad" {
            Ok(Box::new(OpenOnce::new()))
        } else {
            Ok(Box::new(EmptySource))
        }
    }
}

fn source(name: &str) -> String {
    SOURCE.replace("%s", name)
}

fn row_count(result: QueryResult) -> usize {
    match result {
        QueryResult::Rows(batches) => batches.iter().map(|batch| batch.num_rows()).sum(),
        QueryResult::Ack(_) => panic!("expected a result set"),
    }
}

#[tokio::test]
async fn failed_start_does_not_freeze_bindings() {
    let mut session =
        SqlSession::open_with_factories(Arc::new(StartupFactory), Arc::new(FlussSinkFactory));
    session.sql(&source("bad")).await.unwrap();
    assert!(
        session.sql("START;").await.is_err(),
        "startup must fail when a source cannot open"
    );
    // The session never started, so a new source is still allowed and must be
    // visible to a view compiled afterwards (no stale frozen bindings).
    session.sql(&source("good")).await.unwrap();
    let view = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM good GROUP BY k;";
    assert!(
        matches!(session.sql(view).await, Ok(QueryResult::Ack(_))),
        "a view over a source declared after the failed start must compile"
    );
}

#[tokio::test]
async fn failed_start_then_retry_succeeds_with_the_latest_registry() {
    let mut session =
        SqlSession::open_with_factories(Arc::new(StartupFactory), Arc::new(FlussSinkFactory));
    session.sql(&source("bad")).await.unwrap();
    assert!(session.sql("START;").await.is_err(), "first start fails");
    // Declare the second source and its view, then retry: `bad` opens now and
    // the bindings must include `good` for the view to compile.
    session.sql(&source("good")).await.unwrap();
    session
        .sql("CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM good GROUP BY k;")
        .await
        .unwrap();
    assert!(
        matches!(session.sql("START;").await, Ok(QueryResult::Ack(_))),
        "the retry must start with the latest source registry"
    );
    assert_eq!(
        row_count(
            session
                .sql("SELECT k, count FROM mv")
                .await
                .expect("view query")
        ),
        0,
        "no batch was produced, so the view reads empty"
    );
    session.shutdown().await.unwrap();
}
