//! Arrow-native engine kernel for hotlap.
//!
//! Provides an Arrow-native Z-set ([`ZSetBatch`]) and row-format key encoding
//! ([`KeyConverter`]) without depending on differential dataflow.

pub mod batch;
pub mod error;
pub mod keys;

pub use batch::ZSetBatch;
pub use error::EngineError;
pub use keys::KeyConverter;
