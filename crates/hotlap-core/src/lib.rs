//! Shared, engine-owned contract for the hotlap dataflow.
//!
//! Defines the plan IR, ids, watermark spec, Z-set batch and the
//! [`IncrementalCore`] trait implemented by concrete engine kernels.
#![forbid(unsafe_code)]

pub mod batch;
pub mod core;
pub mod error;
pub mod ids;
pub mod metrics;
pub mod plan;
pub mod plan_normalize;
pub mod predicate;
pub mod snapshot;
pub mod watermark;

pub use batch::ZSetBatch;
pub use core::IncrementalCore;
pub use error::CoreError;
pub use ids::{InputId, SplitId, ViewId};
pub use metrics::{Metric, MetricsRegistry};
pub use plan::{AggFunc, AggSpec, CmpOp, Plan, Predicate, Scalar};
pub use plan_normalize::{
    ColumnMapping, DenseReason, Recognition, recognize as recognize_factorized_plan,
};
pub use snapshot::{ENGINE_SNAPSHOT_FORMAT_VERSION, EngineSnapshot};
pub use watermark::WatermarkSpec;
