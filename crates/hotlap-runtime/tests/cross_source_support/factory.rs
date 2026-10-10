//! `SourceFactory` selecting a channel-driven source by declared name.
//!
//! The SQL session builds one source per `CREATE SOURCE`, so a test declares
//! each name's schema first and then reads the resulting handle and senders
//! back to drive ingestion after `START`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};
use hotlap_runtime::SourceFactory;
use hotlap_sql::SqlError;

use crate::sql_source::{BatchSender, SqlSource};

/// Declared shape of one source: its Arrow schema and event-time column index.
#[derive(Clone)]
pub struct SourceSpec {
    pub schema: SchemaRef,
    pub event_time: usize,
}

impl SourceSpec {
    /// Declare a schema whose `_event_time` column sits at `event_time`.
    pub fn new(schema: SchemaRef, event_time: usize) -> Self {
        Self { schema, event_time }
    }
}

/// A built source plus the sender that feeds its single split.
struct Built {
    source: Arc<SqlSource>,
    senders: Vec<BatchSender>,
}

/// Builds a controlled source for every name a test declares.
#[derive(Default)]
pub struct CrossSourceFactory {
    specs: Mutex<BTreeMap<String, SourceSpec>>,
    built: Mutex<BTreeMap<String, Built>>,
}

impl CrossSourceFactory {
    /// An empty factory; declare names before `CREATE SOURCE`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Declare the schema and event-time column of source `name`.
    pub fn declare(&self, name: &str, spec: SourceSpec) {
        self.specs.lock().unwrap().insert(name.to_string(), spec);
    }

    /// The senders feeding source `name`, one per split (split 0 here).
    pub fn senders(&self, name: &str) -> Vec<BatchSender> {
        self.built
            .lock()
            .unwrap()
            .get(name)
            .map(|built| built.senders.clone())
            .unwrap_or_default()
    }

    /// The built source `name`, so a test can observe its commits.
    pub fn source(&self, name: &str) -> Arc<SqlSource> {
        self.built
            .lock()
            .unwrap()
            .get(name)
            .expect("source was not built")
            .source
            .clone()
    }
}

#[async_trait::async_trait]
impl SourceFactory for CrossSourceFactory {
    async fn create(
        &self,
        name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        let spec = self
            .specs
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| SqlError::Catalog(format!("no schema declared for source `{name}`")))?;
        let (source, senders) = SqlSource::new(spec.schema, spec.event_time);
        let shared = Arc::clone(&source);
        self.built
            .lock()
            .unwrap()
            .insert(name.to_string(), Built { source, senders });
        Ok(Box::new(SharedSource(shared)))
    }
}

/// Delegates to a shared [`SqlSource`] so the test keeps its handle.
struct SharedSource(Arc<SqlSource>);

impl Source for SharedSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/sql-source-dataset".into())
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        self.0.splits()
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        self.0.read(split)
    }

    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.0.commit(split, offset)
    }

    fn state(&self) -> SourceState {
        self.0.state()
    }

    fn event_time_column(&self) -> Option<usize> {
        self.0.event_time_column()
    }
}
