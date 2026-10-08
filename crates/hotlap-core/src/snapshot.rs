//! Versioned, serializable snapshot of an engine's durable state.
//!
//! The snapshot is a plain serde container so any format (JSON, binary) can
//! carry it. Rectangular tables are stored as Arrow IPC bytes plus a signed
//! multiplicity column; keyed operator state is stored as ordered
//! `(bytes, value)` maps. The engine encodes and decodes its internals into
//! this representation; [`ENGINE_SNAPSHOT_FORMAT_VERSION`] guards against
//! reading bytes produced by an incompatible layout.

mod operator;

use serde::{Deserialize, Serialize};

use crate::ids::{InputId, SplitId, ViewId};
use crate::plan::Plan;
use crate::watermark::WatermarkSpec;

pub use operator::{
    AggValue, GroupEntry, GroupState, JoinState, OperatorState, WindowBucket, WindowState,
};

/// Current at-rest layout version of [`EngineSnapshot`].
///
/// Bump this whenever the meaning of an existing field changes; readers reject
/// any other version instead of guessing.
pub const ENGINE_SNAPSHOT_FORMAT_VERSION: u32 = 3;

/// An Arrow IPC table plus its signed multiplicity column.
///
/// The IPC stream holds the data columns; `diff` carries one signed
/// multiplicity per row at the same position, mirroring [`crate::ZSetBatch`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTable {
    /// Arrow IPC stream bytes for the table's data columns.
    pub ipc: Vec<u8>,
    /// One signed multiplicity per table row.
    pub diff: Vec<i64>,
}

/// State persisted for one registered source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputSnapshot {
    /// Source handle.
    pub id: InputId,
    /// Arrow IPC schema bytes, or `None` until the first push.
    pub schema: Option<Vec<u8>>,
    /// Declared watermark, if any.
    pub spec: Option<WatermarkSpec>,
    /// Effective (minimum) watermark value across the input's splits.
    pub watermark: i64,
    /// Monotonic watermark per split, ordered by split id. Restored exactly so
    /// a slow split does not restart at zero and re-admit stale records.
    pub splits: Vec<(SplitId, i64)>,
    /// Number of events dropped as late.
    pub late: u64,
}

/// State persisted for one compiled view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewSnapshot {
    /// View handle.
    pub id: ViewId,
    /// The plan the view graph was compiled from (needed to rebuild it).
    pub plan: Plan,
    /// Whether the plan contains a window operator.
    pub windowed: bool,
    /// Whether the view's changelog is tapped.
    pub tapped: bool,
    /// Consolidated output rows, or `None` before the first push.
    pub output: Option<SnapshotTable>,
    /// Buffered tapped changelog.
    pub pending: Option<SnapshotTable>,
    /// One entry per plan node in pre-order; `None` for stateless nodes.
    pub operators: Vec<Option<OperatorState>>,
}

/// The full persisted state of an engine core.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineSnapshot {
    /// Layout version of this snapshot.
    pub format_version: u32,
    /// Logical epoch the state was captured at.
    pub epoch: u64,
    /// Whether the engine had frozen its schema.
    pub frozen: bool,
    /// Per-source state, ordered by input id.
    pub inputs: Vec<InputSnapshot>,
    /// Per-view state, ordered by view id.
    pub views: Vec<ViewSnapshot>,
}
