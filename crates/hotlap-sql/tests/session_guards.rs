//! DDL guards enforced by `SqlSession` before `START`.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::stream;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_sql::{FlussSinkFactory, QueryResult, SourceFactory, SqlError, SqlSession};

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";

struct FixedSource {
    schema: SchemaRef,
}

impl Source for FixedSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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

struct FixedFactory {
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl SourceFactory for FixedFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        Ok(Box::new(FixedSource {
            schema: self.schema.clone(),
        }))
    }
}

fn session(schema: SchemaRef) -> SqlSession {
    SqlSession::open_with_factories(
        Arc::new(FixedFactory { schema }),
        Arc::new(FlussSinkFactory),
    )
}

fn kv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

fn float_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("f", DataType::Float64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

#[tokio::test]
async fn second_source_rejected() {
    let mut session = session(kv_schema());
    session.sql(SOURCE).await.unwrap();
    let other = "CREATE SOURCE src2 WITH (connector='inmem') \
         WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';";
    assert!(matches!(
        session.sql(other).await,
        Err(SqlError::Unsupported(_))
    ));
}

#[tokio::test]
async fn unrepresentable_mv_output_rejected() {
    let mut session = session(float_schema());
    session.sql(SOURCE).await.unwrap();
    let view = "CREATE MATERIALIZED VIEW mv AS SELECT f FROM src;";
    assert!(matches!(
        session.sql(view).await,
        Err(SqlError::Unsupported(_))
    ));
}

#[tokio::test]
async fn global_count_rejected_at_planning() {
    let mut session = session(kv_schema());
    session.sql(SOURCE).await.unwrap();
    // No grouping key: must fail while planning the DDL, before any ingest.
    let view = "CREATE MATERIALIZED VIEW mv AS SELECT count(*) FROM src;";
    assert!(matches!(
        session.sql(view).await,
        Err(SqlError::Unsupported(_))
    ));
    // A grouped count stays supported.
    let grouped = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src GROUP BY k;";
    assert!(matches!(
        session.sql(grouped).await,
        Ok(QueryResult::Ack(_))
    ));
}

#[tokio::test]
async fn rejected_view_does_not_poison_its_name() {
    let mut session = session(float_schema());
    session.sql(SOURCE).await.unwrap();
    let bad = "CREATE MATERIALIZED VIEW mv AS SELECT f FROM src;";
    assert!(matches!(
        session.sql(bad).await,
        Err(SqlError::Unsupported(_))
    ));
    // The failed attempt must not have registered `mv`; the same name is free.
    let good = "CREATE MATERIALIZED VIEW mv AS SELECT _event_time, count(*) \
         FROM src GROUP BY _event_time;";
    assert!(matches!(session.sql(good).await, Ok(QueryResult::Ack(_))));
}
