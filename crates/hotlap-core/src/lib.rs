//! Shared, engine-owned contract for the hotlap dataflow.
//!
//! Defines the plan IR, ids, watermark spec, Z-set batch and the
//! [`IncrementalCore`] trait implemented by concrete engine kernels.
#![forbid(unsafe_code)]

pub mod batch;
pub mod core;
pub mod error;
pub mod ids;
pub mod plan;
pub mod snapshot;
pub mod watermark;

pub use batch::ZSetBatch;
pub use core::IncrementalCore;
pub use error::CoreError;
pub use ids::{InputId, ViewId};
pub use plan::{Plan, Predicate, Scalar};
pub use snapshot::{ENGINE_SNAPSHOT_FORMAT_VERSION, EngineSnapshot};
pub use watermark::WatermarkSpec;
