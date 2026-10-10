use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_runtime::SinkFactory;
use hotlap_runtime::runtime::pipeline::SinkDescription;
use hotlap_sql::SqlError;

/// Metadata-only factory with independently mutable declaration and target.
pub struct IdentityFactory {
    pub creates: Arc<AtomicU32>,
    pub writes: Arc<AtomicU32>,
    pub described: Arc<Mutex<Option<String>>>,
    pub actual: Arc<Mutex<String>>,
}

struct IdentitySink {
    name: String,
    physical_identity: String,
    writes: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Sink for IdentitySink {
    fn binding_name(&self) -> Option<&str> {
        Some(&self.name)
    }

    fn physical_identity(&self) -> Option<String> {
        Some(self.physical_identity.clone())
    }

    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        while changes.next().await.is_some() {}
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl SinkFactory for IdentityFactory {
    async fn describe(
        &self,
        binding_name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
        view: &str,
    ) -> Result<Option<SinkDescription>, SqlError> {
        Ok(self
            .described
            .lock()
            .unwrap()
            .as_ref()
            .map(|identity| SinkDescription {
                binding_name: binding_name.to_owned(),
                view: view.to_owned(),
                physical_identity: identity.clone(),
                capabilities: SinkCapabilities::AtLeastOnce,
                accepts_retractions: false,
                commit_redriable: false,
            }))
    }

    async fn create(
        &self,
        name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(IdentitySink {
            name: name.to_owned(),
            physical_identity: self.actual.lock().unwrap().clone(),
            writes: Arc::clone(&self.writes),
        }))
    }

    fn accepts_retractions(&self, _options: &BTreeMap<String, String>) -> bool {
        false
    }

    fn may_create_transactional(&self, _options: &BTreeMap<String, String>) -> bool {
        false
    }
}
