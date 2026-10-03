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
