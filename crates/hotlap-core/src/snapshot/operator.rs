//! Serialized state of the stateful operators a view graph may contain.

use serde::{Deserialize, Serialize};

use super::SnapshotTable;

/// Retained state of one node, in plan pre-order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperatorState {
    /// A `groupcount` reducer.
    Group(GroupState),
    /// A tumbling-window reducer.
    Window(WindowState),
    /// An inner equi-join (including its buffered side deltas).
    Join(JoinState),
}

/// `groupcount` retained counts: encoded key bytes to count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupState {
    /// Arrow IPC schema bytes of the reducer's input, or `None` before use.
    pub schema: Option<Vec<u8>>,
    /// Live `key bytes -> count` entries, ordered by key bytes.
    pub counts: Vec<(Vec<u8>, i64)>,
}

/// A single open window bucket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowBucket {
    /// Inclusive window start.
    pub start: i64,
    /// Encoded key bytes.
    pub key: Vec<u8>,
    /// Accumulated count for the bucket.
    pub count: i64,
}

/// Tumbling-window retained state: open buckets plus progress counters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowState {
    /// Arrow IPC schema bytes of the reducer's input, or `None` before use.
    pub schema: Option<Vec<u8>>,
    /// Reducer watermark.
    pub watermark: i64,
    /// Rows dropped as late since construction.
    pub dropped_late: u64,
    /// Deltas dropped because their window had already closed.
    pub dropped_closed: u64,
    /// Open buckets.
    pub open: Vec<WindowBucket>,
}

/// Join retained state: both side relations, the cached join, and any deltas
/// buffered while a sibling side's schema was still unknown.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinState {
    /// Materialized left arrangement, or `None` before initialization.
    pub left: Option<SnapshotTable>,
    /// Materialized right arrangement, or `None` before initialization.
    pub right: Option<SnapshotTable>,
    /// Cached per-key join output, ordered by key bytes.
    pub joined: Vec<(Vec<u8>, SnapshotTable)>,
    /// Left delta buffered until the right schema was known.
    pub left_pending: Option<SnapshotTable>,
    /// Right delta buffered until the left schema was known.
    pub right_pending: Option<SnapshotTable>,
}
