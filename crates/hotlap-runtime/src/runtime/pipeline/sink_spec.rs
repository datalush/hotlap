//! Named sink binding and metadata used to preflight durable pipelines.

use std::sync::Arc;

use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};

/// Metadata-only description of the actual target a factory resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkDescription {
    pub binding_name: String,
    pub view: String,
    pub physical_identity: String,
    pub capabilities: SinkCapabilities,
    pub accepts_retractions: bool,
    pub commit_redriable: bool,
}

impl SinkDescription {
    /// Validate that the physical identity and participant bindings are explicit.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.binding_name.trim().is_empty() || self.view.trim().is_empty() {
            return Err(ConnectorError::Unsupported(
                "sink binding name and view must be non-empty".into(),
            ));
        }
        if self.physical_identity.trim().is_empty() {
            return Err(ConnectorError::Unsupported(
                "sink metadata has no physical target identity".into(),
            ));
        }
        Ok(())
    }

    /// Describe a sink that has already been created, for anti-lying verification.
    pub fn from_sink(
        binding_name: &str,
        view: &str,
        sink: &dyn Sink,
    ) -> Result<Self, ConnectorError> {
        let physical_identity = sink
            .physical_identity()
            .filter(|identity| !identity.trim().is_empty())
            .ok_or_else(|| {
                ConnectorError::Unsupported("sink has no physical target identity".into())
            })?;
        let actual_binding = sink.binding_name().unwrap_or(binding_name);
        let description = Self {
            binding_name: actual_binding.to_owned(),
            view: view.to_owned(),
            physical_identity,
            capabilities: sink.capabilities(),
            accepts_retractions: sink.accepts_retractions(),
            commit_redriable: sink.commit_redriable(),
        };
        description.validate()?;
        Ok(description)
    }
}

/// A sink attached to a named binding and a view.
pub struct SinkSpec {
    pub view: String,
    pub sink: Arc<dyn Sink>,
}

impl SinkSpec {
    /// Construct an explicitly named participant; no view-name inference is used.
    pub fn named(
        binding_name: impl Into<String>,
        view: impl Into<String>,
        sink: Arc<dyn Sink>,
    ) -> Self {
        Self {
            view: view.into(),
            sink: Arc::new(NamedSink {
                binding_name: binding_name.into(),
                inner: sink,
            }),
        }
    }

    /// Resolve the actual sink identity together with this binding's view.
    pub fn description(&self) -> Result<SinkDescription, ConnectorError> {
        let binding_name = self.sink.binding_name().ok_or_else(|| {
            ConnectorError::Unsupported("sink binding name must be explicit".into())
        })?;
        SinkDescription::from_sink(binding_name, &self.view, self.sink.as_ref())
    }
}

struct NamedSink {
    binding_name: String,
    inner: Arc<dyn Sink>,
}

#[async_trait::async_trait]
impl Sink for NamedSink {
    fn binding_name(&self) -> Option<&str> {
        Some(&self.binding_name)
    }

    fn physical_identity(&self) -> Option<String> {
        self.inner.physical_identity()
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
