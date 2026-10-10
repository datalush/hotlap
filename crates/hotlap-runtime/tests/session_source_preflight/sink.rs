use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_runtime::SinkFactory;
use hotlap_sql::SqlError;

#[derive(Clone, Default)]
pub struct SinkCounters {
    pub creates: Arc<AtomicU32>,
    pub writes: Arc<AtomicU32>,
    pub commits: Arc<AtomicU32>,
}

pub struct CountSinkFactory(pub SinkCounters);
struct EmptySink(SinkCounters);

#[async_trait::async_trait]
impl Sink for EmptySink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.0.writes.fetch_add(1, Ordering::SeqCst);
        while changes.next().await.is_some() {}
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.0.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl SinkFactory for CountSinkFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &std::collections::BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        self.0.creates.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(EmptySink(self.0.clone())))
    }

    fn accepts_retractions(&self, _options: &std::collections::BTreeMap<String, String>) -> bool {
        true
    }

    fn may_create_transactional(
        &self,
        _options: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        false
    }
}
