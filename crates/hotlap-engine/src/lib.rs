//! Arrow-native engine kernel for hotlap.
//!
//! Provides an Arrow-native Z-set ([`ZSetBatch`]) and row-format key encoding
//! ([`KeyConverter`]) without depending on differential dataflow. The shared
//! contract types (plan IR, ids, watermark spec) live in `hotlap-core` and are
//! re-exported here for convenience.
#![forbid(unsafe_code)]

pub mod arrange;
pub mod batch;
pub mod core;
pub mod error;
pub mod keys;
pub mod ops;
pub mod time;
pub mod zset;

pub use arrange::KeyedArrangement;
pub use batch::ZSetBatch;
pub use core::EngineCore;
pub use error::EngineError;
pub use keys::KeyConverter;
pub use time::Frontier;
pub use time::Watermark;
pub use zset::{consolidate, sort_rows};

pub use hotlap_core::{
    CoreError, IncrementalCore, InputId, Plan, Predicate, Scalar, ViewId, WatermarkSpec,
};
