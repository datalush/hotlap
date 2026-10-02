// SPDX-License-Identifier: Apache-2.0
//! Bounded DataFusion sources for Fluss logs and current KV state.
//!
//! Fluss already decodes log reads into Arrow `RecordBatch` values. This
//! adapter registers a table explicitly and captures end offsets when the
//! physical plan executes; KV scans use paginated server-side snapshots per
//! bucket. Partitioned logs and KV tables discover partitions per execution.

mod catalog;
mod execution;
mod filter;
mod kv_scan;
mod kv_table;
mod log_table;
mod metrics;
mod offsets;
mod partitions;
mod scan;

pub use catalog::FlussCatalog;
pub use kv_table::FlussKvTable;
pub use log_table::FlussLogTable;
