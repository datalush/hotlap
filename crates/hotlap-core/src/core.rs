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
use crate::watermark::WatermarkSpec;

/// The DD-free contract a stateful engine kernel implements.
pub trait IncrementalCore {
    /// Declare a source. Must happen before the first push, and before any
    /// [`build_view`](Self::build_view) that references it.
    fn register_input(&mut self, input: InputId) -> Result<(), CoreError>;

    /// Compile a `Source`-rooted plan into a live view. Also pre-freeze only.
    /// The inputs named by the plan must already be registered.
    fn build_view(&mut self, view: ViewId, plan: &Plan) -> Result<(), CoreError>;

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
}
