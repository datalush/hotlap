//! Adapt a shared `Arc<dyn Source>` back into the boxed source trait object
//! the pipeline owns.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};

/// A [`Source`] that delegates to an already-registered, shared source.
///
/// The session keeps the source in an `Arc` so the DataFusion planning table
/// and the runtime pipeline observe the same source; the pipeline still needs
/// an owned `Box<dyn Source>`, which this thin wrapper supplies.
pub(crate) struct SharedSource(pub(crate) Arc<dyn Source>);

impl Source for SharedSource {
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
    fn is_unbounded(&self) -> bool {
        self.0.is_unbounded()
    }
}
