//! DDL guards enforced by `SqlSession` before `START`.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::stream;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_runtime::{FlussSinkFactory, QueryResult, SourceFactory, SqlSession};
use hotlap_sql::SqlError;

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

/// UInt64 is not representable by the kernel, so a view selecting it must be
/// rejected at DDL time.
fn unsupported_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("u", DataType::UInt64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

#[tokio::test]
async fn duplicate_source_rejected_without_replacing_it() {
    let mut session = session(kv_schema());
    session.sql(SOURCE).await.unwrap();
    // The second declaration of `src` must be rejected before it can replace
    // the live source/provider.
    assert!(matches!(
        session.sql(SOURCE).await,
        Err(SqlError::Catalog(_))
    ));
    // The original binding still resolves, so the name was not poisoned.
    let view = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src GROUP BY k;";
    assert!(matches!(session.sql(view).await, Ok(QueryResult::Ack(_))));
}

#[tokio::test]
async fn source_after_start_rejected() {
    let mut session = session(kv_schema());
    session.sql(SOURCE).await.unwrap();
    session.sql("START;").await.unwrap();
    let other = "CREATE SOURCE src2 WITH (connector='inmem') \
         WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';";
    assert!(matches!(
        session.sql(other).await,
        Err(SqlError::Unsupported(_))
    ));
}

#[tokio::test]
async fn quoted_source_name_binds_its_view() {
    let mut session = session(kv_schema());
    // An embedded doubled quote must survive into the DataFusion registration
    // and the canonical binding in the same path.
    session
        .sql(
            "CREATE SOURCE \"Src\"\"X\" WITH (connector='inmem') \
             WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';",
        )
        .await
        .unwrap();
    session
        .sql(
            "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) \
             FROM \"Src\"\"X\" GROUP BY k;",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn quoted_names_keep_case_and_literal_dots() {
    let mut session = session(kv_schema());
    // A double-quoted identifier keeps its case; a dot inside it is literal.
    session
        .sql(
            "CREATE SOURCE \"Src\" WITH (connector='inmem') \
             WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';",
        )
        .await
        .unwrap();
    session
        .sql(
            "CREATE SOURCE \"public.x\" WITH (connector='inmem') \
             WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';",
        )
        .await
        .unwrap();
    session
        .sql("CREATE MATERIALIZED VIEW mv1 AS SELECT k, count(*) FROM \"Src\" GROUP BY k;")
        .await
        .unwrap();
    session
        .sql("CREATE MATERIALIZED VIEW mv2 AS SELECT k, count(*) FROM \"public.x\" GROUP BY k;")
        .await
        .unwrap();
}

#[tokio::test]
async fn unrepresentable_mv_output_rejected() {
    let mut session = session(unsupported_schema());
    session.sql(SOURCE).await.unwrap();
    let view = "CREATE MATERIALIZED VIEW mv AS SELECT u FROM src;";
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
    let mut session = session(unsupported_schema());
    session.sql(SOURCE).await.unwrap();
    let bad = "CREATE MATERIALIZED VIEW mv AS SELECT u FROM src;";
    assert!(matches!(
        session.sql(bad).await,
        Err(SqlError::Unsupported(_))
    ));
    // The failed attempt must not have registered `mv`; the same name is free.
    let good = "CREATE MATERIALIZED VIEW mv AS SELECT _event_time, count(*) \
         FROM src GROUP BY _event_time;";
    assert!(matches!(session.sql(good).await, Ok(QueryResult::Ack(_))));
}
