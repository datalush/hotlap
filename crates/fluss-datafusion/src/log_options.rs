// SPDX-License-Identifier: Apache-2.0
//! Read semantics of a Fluss log source, independent of the Fluss connection.

use std::collections::HashMap;
use std::time::Duration;

use fluss::metadata::TableBucket;

/// Whether an execution ends at captured offsets or follows new log records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogReadMode {
    /// Finite scan of the offsets visible when execution begins.
    Batch,
    /// Keep polling and yield records appended after execution begins.
    Streaming,
}

/// Inclusive starting offsets, resolved independently for every bucket.
#[derive(Clone, Debug, Default)]
pub enum LogStart {
    /// Read the entire range that has not been removed by retention.
    #[default]
    Earliest,
    /// Start at the latest offsets captured when execution begins.
    Latest,
    /// Resume each selected bucket at the specified inclusive offset.
    /// All selected buckets must be present; extra or stale buckets fail.
    Offsets(HashMap<TableBucket, i64>),
}

/// Per-provider options; `fluss::Config` still controls transport and scanner
/// operation deadlines, and DataFusion controls SQL and the memory pool.
#[derive(Clone, Debug)]
pub struct LogReadOptions {
    pub mode: LogReadMode,
    pub start: LogStart,
    /// Whole-query deadline for Batch only. In Streaming, idle periods are
    /// normal; network timeouts remain configured on the Fluss connection.
    pub batch_timeout: Duration,
}

impl Default for LogReadOptions {
    fn default() -> Self {
        Self {
            mode: LogReadMode::Streaming,
            start: LogStart::Earliest,
            batch_timeout: Duration::from_secs(45),
        }
    }
}

impl LogReadOptions {
    pub fn batch(timeout: Duration) -> Self {
        Self {
            mode: LogReadMode::Batch,
            batch_timeout: timeout,
            ..Self::default()
        }
    }
}
