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
///
/// The offsets reflect the **read position** of each split: the offset of the
/// next record to be read. They do not track what has been fully consumed by
/// downstream views.
///
/// Replay invariant: a checkpoint taken between source polls contains every
/// record with offset strictly below the captured offset (`records < offset`
/// are already applied) and none at or above it (`records >= offset` are
/// replayed). Reading from `offset` therefore neither loses nor duplicates.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceState {
    pub offsets: BTreeMap<SplitId, Offset>,
}

/// An ordered run of records from a split (`batch` includes `_event_time`).
#[derive(Clone, Debug)]
pub struct SourceBatch {
    pub batch: RecordBatch,
    /// Offset of the **first** record of `batch`; the records are ordered but
    /// their offsets are not assumed to be contiguous.
    pub base_offset: Offset,
    /// Split (bucket) the records were read from, propagated to the engine so
    /// it can track a watermark per split.
    pub split: SplitId,
}

/// A source's stream of batches.
pub type SourceStream = Pin<Box<dyn Stream<Item = Result<SourceBatch, ConnectorError>> + Send>>;

/// A source of Arrow batches with resumable offsets and optional event-time.
pub trait Source: Send + Sync {
    /// Full Arrow schema of produced batches, including `_event_time` when present.
    fn schema(&self) -> SchemaRef;
    /// Enumerate splits and their start offsets.
    fn splits(&self) -> Result<Vec<Split>, ConnectorError>;
    /// Read `split` as an ordered stream of batches.
    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError>;
    /// Current resumable state, reflecting the **read position** (not the
    /// consumption position) of each split.
    ///
    /// A checkpoint may advance the read position when a batch is produced, but
    /// the engine only calls `state()` between polls, after the previous batch
    /// was ingested, so a persisted offset never runs ahead of applied records.
    fn state(&self) -> SourceState;
    /// Column index of the event-time column (ms), if the source has one.
    fn event_time_column(&self) -> Option<usize>;
    /// Whether this source never ends (default: bounded).
    fn is_unbounded(&self) -> bool {
        false
    }
    /// Reopen the source at previously captured offsets, returning the splits
    /// to read from.
    ///
    /// The default reads the current splits and overrides each `start` with the
    /// captured offset when one exists. A source that can no longer serve a
    /// captured offset (data older than its retention) must instead return an
    /// explicit error rather than silently skip records.
    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        let mut splits = self.splits()?;
        for split in &mut splits {
            if let Some(offset) = state.offsets.get(&split.id) {
                split.start = *offset;
            }
        }
        Ok(splits)
    }
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
