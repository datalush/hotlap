//! Shared helpers for cross-source tests.
//!
//! Each integration test pulls only the harness whose items it fully uses, so
//! the recovery harness is included directly by the recovery test and this
//! module exposes the oracle rows used by the SQL oracle test.

#[path = "rows.rs"]
pub mod rows;
