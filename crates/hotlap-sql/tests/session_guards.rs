//! DDL guards enforced by `SqlSession` before `START`.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::stream;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_sql::{SourceFactory, SqlError, SqlSession};

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
    SqlSession::open_with_factory(Arc::new(FixedFactory { schema }))
}

fn kv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
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
