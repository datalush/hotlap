//! Arrow-native engine kernel for hotlap.
//!
//! Provides an Arrow-native Z-set ([`ZSetBatch`]) and row-format key encoding
//! ([`KeyConverter`]) without depending on differential dataflow.

pub mod arrange;
pub mod batch;
pub mod error;
pub mod keys;
pub mod ops;
pub mod time;
pub mod zset;

pub use arrange::KeyedArrangement;
pub use batch::ZSetBatch;
pub use error::EngineError;
pub use keys::KeyConverter;
pub use time::Frontier;
pub use time::Watermark;
pub use zset::{consolidate, sort_rows};
