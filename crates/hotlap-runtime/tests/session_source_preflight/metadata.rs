use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_runtime::SourceFactory;
use hotlap_sql::SqlError;

#[derive(Clone)]
pub struct Metadata {
    declared: Arc<RwLock<(SchemaRef, Vec<Split>)>>,
    pub reads: Arc<AtomicU32>,
    pub resumes: Arc<AtomicU32>,
}

impl Metadata {
    pub fn new(schema: SchemaRef, splits: Vec<Split>) -> Self {
        Self {
            declared: Arc::new(RwLock::new((schema, splits))),
            reads: Arc::new(AtomicU32::new(0)),
            resumes: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn set(&self, schema: SchemaRef, splits: Vec<Split>) {
        *self.declared.write().unwrap() = (schema, splits);
    }
}

struct MetadataSource(Metadata);

impl Source for MetadataSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/session-source-preflight/metadata-dataset".into())
    }

    fn schema(&self) -> SchemaRef {
        self.0.declared.read().unwrap().0.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(self.0.declared.read().unwrap().1.clone())
    }

    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        self.0.reads.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::empty()))
    }

    fn state(&self) -> SourceState {
        SourceState {
            offsets: std::collections::BTreeMap::from([(0, 1)]),
        }
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }

    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        self.0.resumes.fetch_add(1, Ordering::SeqCst);
        let mut splits = self.splits()?;
        for split in &mut splits {
            if let Some(offset) = state.offsets.get(&split.id) {
                split.start = *offset;
            }
        }
        Ok(splits)
    }
}

pub struct MetadataFactory(pub Metadata);

#[async_trait::async_trait]
impl SourceFactory for MetadataFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        Ok(Box::new(MetadataSource(self.0.clone())))
    }
}

pub fn schema(kind: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", kind, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}
