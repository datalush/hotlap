//! Stable handles for sources and views declared to an [`IncrementalCore`].
//!
//! [`IncrementalCore`]: crate::core::IncrementalCore

use serde::{Deserialize, Serialize};

/// Handle for a source declared to the core. One input feeds many views.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InputId(pub u32);

/// Handle for a view compiled into the core from a plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ViewId(pub u32);

/// Identifies a split (bucket) within a source.
///
/// Engines track watermarks per `(InputId, SplitId)` so a fast split cannot
/// advance the watermark of a slower one. `0` is the implicit split used by
/// callers that do not model splits.
pub type SplitId = i32;
