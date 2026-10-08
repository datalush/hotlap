//! Engine-owned incremental-core boundary.
//!
//! The trait speaks only in engine types ([`Plan`], [`ZSetBatch`], [`InputId`],
//! [`ViewId`]); no third-party dataflow type crosses it.
//!
//! [`Plan`]: crate::plan::Plan
//! [`ZSetBatch`]: crate::batch::ZSetBatch

use crate::batch::ZSetBatch;
use crate::error::CoreError;
use crate::ids::{InputId, ViewId};
use crate::plan::Plan;
use crate::snapshot::EngineSnapshot;
use crate::watermark::WatermarkSpec;

/// The DD-free contract a stateful engine kernel implements.
pub trait IncrementalCore {
    /// Declare a source. Must happen before the first push, and before any
    /// [`build_view`](Self::build_view) that references it.
    fn register_input(&mut self, input: InputId) -> Result<(), CoreError>;

    /// Compile a `Source`-rooted plan into a live view. The inputs named by the
    /// plan must already be registered.
    ///
    /// Before the first push this only records the view. After the first push
    /// it is allowed only when [`set_input_retention`](Self::set_input_retention)
    /// enabled retention that still covers the whole run: the new view is
    /// evaluated over the retained inputs and then joined to the live flow.
    /// Otherwise it is rejected rather than returning a truncated view.
    fn build_view(&mut self, view: ViewId, plan: &Plan) -> Result<(), CoreError>;

    /// Retain the last `events` input deltas (in push order) so a view can be
    /// built after the first push. Only before the first push; `events` must be
    /// at least one. Retention is off by default, which keeps post-start
    /// [`build_view`](Self::build_view) rejected.
    fn set_input_retention(&mut self, events: usize) -> Result<(), CoreError>;

    /// Declare a source's watermark. Only before the first push, and after
    /// [`register_input`](Self::register_input) of that source.
    fn declare_watermark(&mut self, input: InputId, spec: WatermarkSpec) -> Result<(), CoreError>;

    /// Feed a Z-set to the given source, consolidating every consuming view.
    fn push(&mut self, input: InputId, batch: &ZSetBatch) -> Result<(), CoreError>;

    /// Current consolidated output of a view.
    fn snapshot(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError>;

    /// Number of events dropped as late in `input`.
    fn late_dropped(&self, input: InputId) -> Result<u64, CoreError>;

    /// Drain the change deltas accumulated for `view` since the last drain.
    fn take_changes(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError>;

    /// Subscribe `view`'s output changes (`tap`). Pre-freeze: after
    /// [`build_view`](Self::build_view) and before the first push.
    fn tap_view(&mut self, view: ViewId) -> Result<(), CoreError>;

    /// Capture the engine's durable state as a versioned [`EngineSnapshot`].
    fn checkpoint(&self) -> Result<EngineSnapshot, CoreError>;

    /// Rebuild this engine's state from a [`EngineSnapshot`] captured by
    /// [`checkpoint`](Self::checkpoint). Rejects an unknown layout version.
    fn restore(&mut self, snapshot: &EngineSnapshot) -> Result<(), CoreError>;
}
