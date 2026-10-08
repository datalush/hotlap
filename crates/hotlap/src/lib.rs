//! Hotlap engine kernel: an engine-owned facade over a stateful core.
//!
//! The shared contract types ([`Plan`], [`ZSetBatch`], ids, watermark spec and
//! the [`IncrementalCore`] trait) live in `hotlap-core` and are re-exported here
//! so callers only depend on this crate.
pub mod engine;
#[cfg(test)]
pub mod harness;
pub mod state;

pub use engine::{Hotlap, HotlapError};
pub use hotlap_core::plan;
pub use hotlap_core::{
    CmpOp, CoreError, IncrementalCore, InputId, Plan, Predicate, Scalar, SplitId, ViewId,
    WatermarkSpec, ZSetBatch,
};
