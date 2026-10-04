// SPDX-License-Identifier: Apache-2.0
//! Optional observations of batches offered by the Fluss source to DataFusion.

use fluss::metadata::TableBucket;

/// Source-side delivery, NOT an engine checkpoint or sink acknowledgement.
/// A DataFusion filter may later discard rows in this batch. Gaps between
/// events are possible when server-side batch pruning skips log offsets.
#[derive(Clone, Debug)]
pub struct LogDelivery {
    /// Unique within this process for one execution of a physical scan plan.
    pub execution_id: u64,
    pub bucket: TableBucket,
    pub base_offset: i64,
    pub next_offset: i64,
    pub rows: usize,
}

/// Assigned range at source initialization. Stops are exclusive; streaming has
/// no stop. These positions describe offered/excluded work, never engine state.
#[derive(Clone, Debug)]
pub struct LogReadPosition {
    pub bucket: TableBucket,
    pub start_offset: i64,
    pub stop_offset: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogTermination {
    Completed,
    Failed,
    Cancelled,
}

/// Bounded optional source observations. Subscribe before execution; handle
/// broadcast Lagged as incomplete evidence and never resume from it silently.
/// Initializations for all physical partitions are required for full assignment.
#[derive(Clone, Debug)]
pub enum LogProgress {
    Initialized {
        execution_id: u64,
        table_id: i64,
        schema_id: i32,
        partition: usize,
        partitions: usize,
        positions: Vec<LogReadPosition>,
    },
    Offered(LogDelivery),
    /// Range conclusively excluded by source pruning. Does not pass any batch
    /// still buffered for this bucket; downstream filtering is a separate fact.
    Excluded {
        execution_id: u64,
        bucket: TableBucket,
        base_offset: i64,
        next_offset: i64,
    },
    Terminated {
        execution_id: u64,
        partition: usize,
        status: LogTermination,
    },
}

/// A fetched position can lead batches not yet offered. Clamp it to their first
/// offset and the finite stop before publishing resumable source progress.
pub(crate) fn safe_frontier(consumed: i64, pending: Option<i64>, stop: Option<i64>) -> i64 {
    let next = pending.map_or(consumed, |pending| consumed.min(pending));
    stop.map_or(next, |stop| next.min(stop))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fetch_completion_cannot_skip_pending_batches_or_finite_stop() {
        assert_eq!(safe_frontier(100, Some(20), Some(80)), 20);
        assert_eq!(safe_frontier(100, None, Some(80)), 80);
        assert_eq!(safe_frontier(100, Some(20), None), 20);
        assert_eq!(safe_frontier(100, None, None), 100);
    }
}
