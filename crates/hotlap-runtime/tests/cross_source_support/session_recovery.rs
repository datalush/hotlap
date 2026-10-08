//! Resumable `SourceFactory` for the SQL session checkpoint/restart test.
//!
//! It reuses the recovery suite's [`ResumableSource`] algorithm and adds only
//! the event-time advertisement the session's `WATERMARK` clause requires.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};
use hotlap_runtime::SourceFactory;
use hotlap_sql::SqlError;

use crate::resumable::{Dataset, ResumableSource};

/// Builds a resumable source for every name a test declares.
#[derive(Default)]
pub struct ResumableSessionFactory {
    datasets: Mutex<BTreeMap<String, Dataset>>,
    built: Mutex<BTreeMap<String, Arc<ResumableSource>>>,
}

impl ResumableSessionFactory {
    /// An empty factory; declare names before `CREATE SOURCE`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Declare the log source `name` resumes from.
    pub fn declare(&self, name: &str, dataset: Dataset) {
        self.datasets
            .lock()
            .unwrap()
            .insert(name.to_string(), dataset);
    }

    /// The built source `name`, so a test can observe its applied offsets.
    pub fn source(&self, name: &str) -> Arc<ResumableSource> {
        self.built
            .lock()
            .unwrap()
            .get(name)
            .expect("source was not built")
            .clone()
    }
}

#[async_trait::async_trait]
impl SourceFactory for ResumableSessionFactory {
    async fn create(
        &self,
        name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        let dataset = self
            .datasets
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| SqlError::Catalog(format!("no dataset declared for `{name}`")))?;
        let source = Arc::new(ResumableSource::new(dataset));
        self.built
            .lock()
            .unwrap()
            .insert(name.to_string(), Arc::clone(&source));
        Ok(Box::new(SessionSource(source)))
    }
}

/// Wraps [`ResumableSource`], advertising column 0 (`k`) as the event time.
struct SessionSource(Arc<ResumableSource>);

impl Source for SessionSource {
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
        Some(0)
    }

    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        self.0.resume(state)
    }

    fn is_unbounded(&self) -> bool {
        self.0.is_unbounded()
    }
}
