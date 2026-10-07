//! Engine-owned source contract: splits, resumable offsets and Arrow batches.

use std::collections::BTreeMap;
use std::pin::Pin;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorError;

/// Log offset within a split.
pub type Offset = i64;
/// Identifies a split (v1: a Fluss bucket id).
pub type SplitId = i32;

/// A read unit with its starting offset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Split {
    pub id: SplitId,
    pub start: Offset,
}

/// Resumable per-split offsets (serializable for future checkpoints).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceState {
    pub offsets: BTreeMap<SplitId, Offset>,
}

/// An ordered run of records from a split (`batch` includes `_event_time`).
#[derive(Clone, Debug)]
pub struct SourceBatch {
    pub batch: RecordBatch,
    pub base_offset: Offset,
}

/// A source's stream of batches.
pub type SourceStream =
    Pin<Box<dyn Stream<Item = Result<SourceBatch, ConnectorError>> + Send>>;

/// A source of Arrow batches with resumable offsets and optional event-time.
pub trait Source: Send + Sync {
    /// Full Arrow schema of produced batches, including `_event_time` when present.
    fn schema(&self) -> SchemaRef;
    /// Enumerate splits and their start offsets.
    fn splits(&self) -> Result<Vec<Split>, ConnectorError>;
    /// Read `split` as an ordered stream of batches.
    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError>;
    /// Current resumable state.
    fn state(&self) -> SourceState;
    /// Column index of the event-time column (ms), if the source has one.
    fn event_time_column(&self) -> Option<usize>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_state_roundtrip() {
        let mut st = SourceState::default();
        st.offsets.insert(0, 42);
        st.offsets.insert(3, 7);
        let json = serde_json::to_string(&st).unwrap();
        let back: SourceState = serde_json::from_str(&json).unwrap();
        assert_eq!(st, back);
    }
}
