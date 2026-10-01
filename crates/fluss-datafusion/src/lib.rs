// SPDX-License-Identifier: Apache-2.0
//! Bounded DataFusion source for Fluss append-only log tables.
//!
//! Fluss already decodes log reads into Arrow `RecordBatch` values. This
//! adapter registers a table explicitly and captures end offsets when the
//! physical plan executes; filters and limits stay in DataFusion. KV tables
//! and partitioned tables are rejected rather than misrepresented.

mod catalog;
mod log_table;

pub use catalog::FlussCatalog;
pub use log_table::FlussLogTable;
