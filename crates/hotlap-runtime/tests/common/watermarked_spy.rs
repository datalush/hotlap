//! Test adapter that marks a single-column source as event time.

use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};

pub struct WatermarkedSpy<S>(pub S);

impl<S: Source> Source for WatermarkedSpy<S> {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
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
}
