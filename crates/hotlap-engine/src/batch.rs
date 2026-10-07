//! Re-export of the shared Z-set type from `hotlap-core`.
//!
//! Kept as a module so existing `crate::batch::ZSetBatch` paths keep working
//! after the type moved to the differential-dataflow-free contract crate.

pub use hotlap_core::ZSetBatch;
