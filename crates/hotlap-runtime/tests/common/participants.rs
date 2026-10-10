//! Explicit identities for test doubles that model named physical stores.

use std::sync::Arc;

use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

#[allow(dead_code)]
pub struct IdentifiedSource {
    identity: String,
    inner: Arc<dyn Source>,
}

#[allow(dead_code)]
impl IdentifiedSource {
    pub fn new(identity: impl Into<String>, inner: Arc<dyn Source>) -> Self {
        Self {
            identity: identity.into(),
            inner,
        }
    }
}

impl Source for IdentifiedSource {
    fn physical_identity(&self) -> Option<String> {
        Some(self.identity.clone())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        self.inner.splits()
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        self.inner.read(split)
    }

    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.inner.commit(split, offset)
    }

    fn state(&self) -> SourceState {
        self.inner.state()
    }

    fn event_time_column(&self) -> Option<usize> {
        self.inner.event_time_column()
    }

    fn is_unbounded(&self) -> bool {
        self.inner.is_unbounded()
    }

    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        self.inner.resume(state)
    }
}

#[allow(dead_code)]
pub struct IdentifiedSink {
    identity: String,
    inner: Arc<dyn Sink>,
}

#[allow(dead_code)]
impl IdentifiedSink {
    pub fn new(identity: impl Into<String>, inner: Arc<dyn Sink>) -> Self {
        Self {
            identity: identity.into(),
            inner,
        }
    }
}

#[async_trait::async_trait]
impl Sink for IdentifiedSink {
    fn physical_identity(&self) -> Option<String> {
        Some(self.identity.clone())
    }

    fn binding_name(&self) -> Option<&str> {
        self.inner.binding_name()
    }

    fn capabilities(&self) -> SinkCapabilities {
        self.inner.capabilities()
    }

    fn accepts_retractions(&self) -> bool {
        self.inner.accepts_retractions()
    }

    fn commit_redriable(&self) -> bool {
        self.inner.commit_redriable()
    }

    async fn write(&self, changes: ChangeStream) -> Result<(), ConnectorError> {
        self.inner.write(changes).await
    }

    async fn prepare(&self) -> Result<(), ConnectorError> {
        self.inner.prepare().await
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.inner.commit().await
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.inner.abort().await
    }
}

#[allow(dead_code)]
pub fn sink_sync(
    identity: impl Into<String>,
    binding_name: impl Into<String>,
    view: impl Into<String>,
    sink: Arc<dyn Sink>,
) -> SinkSync {
    SinkSync::sink_only_named(
        SharedSink::new(Arc::new(IdentifiedSink::new(identity, sink))),
        binding_name.into(),
        view.into(),
    )
}
