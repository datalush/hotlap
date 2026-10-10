use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_runtime::SinkFactory;
use hotlap_runtime::runtime::pipeline::SinkDescription;
use hotlap_sql::SqlError;

#[derive(Clone, Default)]
pub struct SinkCounters {
    pub creates: Arc<AtomicU32>,
    pub writes: Arc<AtomicU32>,
    pub commits: Arc<AtomicU32>,
}

pub struct CountSinkFactory(pub SinkCounters);
struct EmptySink {
    counters: SinkCounters,
    binding_name: String,
}

#[async_trait::async_trait]
impl Sink for EmptySink {
    fn binding_name(&self) -> Option<&str> {
        Some(&self.binding_name)
    }

    fn physical_identity(&self) -> Option<String> {
        Some("test/session-source-preflight/output".into())
    }

    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.counters.writes.fetch_add(1, Ordering::SeqCst);
        while changes.next().await.is_some() {}
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }

    fn accepts_retractions(&self) -> bool {
        true
    }

    fn commit_redriable(&self) -> bool {
        true
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.counters.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl SinkFactory for CountSinkFactory {
    async fn describe(
        &self,
        binding_name: &str,
        _options: &std::collections::BTreeMap<String, String>,
        _schema: SchemaRef,
        view: &str,
    ) -> Result<Option<SinkDescription>, SqlError> {
        Ok(Some(SinkDescription {
            binding_name: binding_name.to_owned(),
            view: view.to_owned(),
            physical_identity: "test/session-source-preflight/output".into(),
            capabilities: SinkCapabilities::Idempotent,
            accepts_retractions: true,
            commit_redriable: true,
        }))
    }

    async fn create(
        &self,
        name: &str,
        _options: &std::collections::BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        self.0.creates.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(EmptySink {
            counters: self.0.clone(),
            binding_name: name.to_owned(),
        }))
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
