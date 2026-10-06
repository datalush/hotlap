//! Hotlap engine kernel: incremental core behind an engine-owned boundary.
pub mod core;
pub mod engine;
#[cfg(test)]
pub mod harness;
pub mod plan;
pub mod row;
pub mod state;

pub use engine::{Hotlap, HotlapError};
pub use plan::Plan;
pub use row::{ChangeBatch, Row, Scalar};
