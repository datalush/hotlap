//! DataFusion integration point for the copied Fluss Rust client.
//!
//! Fluss already decodes scan results into Arrow `RecordBatch` values.
//! A `TableProvider` must be built on a *correct bounded read*: the client's
//! `LimitBatchScanner` returns at most N rows per bucket, not a full KV table.
//! Do not register it as an unrestricted table scan.
