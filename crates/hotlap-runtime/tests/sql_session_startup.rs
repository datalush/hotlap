//! Startup failure must not leave the session with frozen source bindings.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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

/// A source whose only split fails to open, so `START` cannot succeed.
struct FailingSource;

impl Source for FailingSource {
    fn schema(&self) -> SchemaRef {
        schema()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        Err(ConnectorError::Infrastructure(format!(
            "open failed for split {}",
            split.id
        )))
    }

    fn state(&self) -> SourceState {
        SourceState::default()
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

struct FailingFactory;

#[async_trait::async_trait]
impl SourceFactory for FailingFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        Ok(Box::new(FailingSource))
    }
}

fn source(name: &str) -> String {
    SOURCE.replace("%s", name)
}

#[tokio::test]
async fn failed_start_does_not_freeze_bindings() {
    let mut session =
        SqlSession::open_with_factories(Arc::new(FailingFactory), Arc::new(FlussSinkFactory));
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
