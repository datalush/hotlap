//! Serialized state of the stateful operators a view graph may contain.

use serde::{Deserialize, Serialize};

use super::SnapshotTable;
use super::minmax::OrderedMultiset;

/// Retained state of one node, in plan pre-order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperatorState {
    /// A grouped-aggregate reducer.
    Group(GroupState),
    /// A tumbling-window reducer.
    Window(WindowState),
    /// An inner equi-join (including its buffered side deltas).
    Join(JoinState),
}

/// Accumulated value of one aggregate for one group.
///
/// Floats are compared by `f64` equality. `PartialEq` is implemented manually:
/// `min`/`max` compare by their current extreme rather than by their whole
/// multiset, so two states that render the same output row are equal and no
/// redundant upsert is emitted. `Eq` is implemented to keep the snapshot
/// container usable as an `Eq` type, mirroring `Scalar`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggValue {
    /// A `count` accumulator.
    Count(i64),
    /// An integer `sum` accumulator plus its non-null count.
    SumInteger {
        /// Running sum, widened to `i128`.
        sum: i128,
        /// Number of non-null values folded in; zero means the sum is null.
        count: i64,
    },
    /// A float `sum` accumulator plus its non-null count.
    SumFloat {
        /// Running sum.
        sum: f64,
        /// Number of non-null values folded in; zero means the sum is null.
        count: i64,
    },
    /// An `avg` accumulator: the running sum plus its non-null count.
    Avg {
        /// Running sum of the input values.
        sum: f64,
        /// Number of non-null values folded in.
        count: i64,
    },
    /// A retraction-aware `min` over the group's non-null values.
    Min(OrderedMultiset),
    /// A retraction-aware `max` over the group's non-null values.
    Max(OrderedMultiset),
}

impl PartialEq for AggValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (AggValue::Count(a), AggValue::Count(b)) => a == b,
            (
                AggValue::SumInteger { sum: a, count: b },
                AggValue::SumInteger { sum: c, count: d },
            ) => a == c && b == d,
            (AggValue::SumFloat { sum: a, count: b }, AggValue::SumFloat { sum: c, count: d }) => {
                a == c && b == d
            }
            (AggValue::Avg { sum: a, count: b }, AggValue::Avg { sum: c, count: d }) => {
                a == c && b == d
            }
            (AggValue::Min(a), AggValue::Min(b)) => a.min() == b.min(),
            (AggValue::Max(a), AggValue::Max(b)) => a.max() == b.max(),
            _ => false,
        }
    }
}

impl Eq for AggValue {}

/// Retained state of one group: its row multiplicity and one value per agg.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupEntry {
    /// Net row multiplicity; the group exists while this is non-zero.
    pub rows: i64,
    /// One accumulated value per aggregate, in plan order.
    pub values: Vec<AggValue>,
}

impl GroupEntry {
    /// Whether two entries render the same output aggregate row.
    ///
    /// The row multiplicity is internal bookkeeping: `min`/`max` can change it
    /// (retracting a null or a non-extreme value) without changing any emitted
    /// cell, so comparisons that gate emission must ignore it.
    pub fn same_output(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

/// `groupaggregate` retained state: encoded key bytes to accumulator values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupState {
    /// Arrow IPC schema bytes of the reducer's input, or `None` before use.
    pub schema: Option<Vec<u8>>,
    /// Live `key bytes -> entry` groups, ordered by key bytes.
    pub groups: Vec<(Vec<u8>, GroupEntry)>,
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
