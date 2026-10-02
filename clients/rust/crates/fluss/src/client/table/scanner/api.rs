// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Public record and Arrow batch scanner operations.

use super::{
    Arc, Duration, Error, HashMap, LogScanner, PartitionId, RecordBatchLogScanner, Result,
    ScanBatch, ScanRecords, SchemaRef, TableBucket, TableId, TablePath, from_ref,
};

impl LogScanner {
    pub async fn poll(&self, timeout: Duration) -> Result<ScanRecords> {
        self.inner.poll_records(timeout).await
    }

    pub async fn subscribe(&self, bucket: i32, offset: i64) -> Result<()> {
        self.inner.subscribe(bucket, offset).await
    }

    pub async fn subscribe_buckets(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()> {
        self.inner.subscribe_buckets(bucket_offsets).await
    }

    pub async fn subscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
        offset: i64,
    ) -> Result<()> {
        self.inner
            .subscribe_partition(partition_id, bucket, offset)
            .await
    }

    pub async fn subscribe_partition_buckets(
        &self,
        partition_bucket_offsets: &HashMap<(PartitionId, i32), i64>,
    ) -> Result<()> {
        self.inner
            .subscribe_partition_buckets(partition_bucket_offsets)
            .await
    }

    pub async fn unsubscribe(&self, bucket: i32) -> Result<()> {
        self.inner.unsubscribe(bucket).await
    }

    pub async fn unsubscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
    ) -> Result<()> {
        self.inner.unsubscribe_partition(partition_id, bucket).await
    }
}

// Implementation for RecordBatchLogScanner (batches mode)
impl RecordBatchLogScanner {
    pub(crate) fn set_partition_bucket_counts(&self, counts: HashMap<PartitionId, i32>) {
        *self.inner.log_fetcher.partition_bucket_counts.write() = counts;
    }

    /// Poll for batches with metadata (bucket and offset information).
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ScanBatch>> {
        self.inner.poll_batches(timeout).await
    }

    pub async fn subscribe(&self, bucket: i32, offset: i64) -> Result<()> {
        self.inner.subscribe(bucket, offset).await
    }

    pub async fn subscribe_buckets(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()> {
        self.inner.subscribe_buckets(bucket_offsets).await
    }

    pub async fn subscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
        offset: i64,
    ) -> Result<()> {
        self.inner
            .subscribe_partition(partition_id, bucket, offset)
            .await
    }

    /// Returns whether the table is partitioned
    pub fn is_partitioned(&self) -> bool {
        self.inner.is_partitioned_table
    }

    /// Returns all subscribed buckets with their current offsets
    pub fn get_subscribed_buckets(&self) -> Vec<(TableBucket, i64)> {
        self.inner.log_scanner_status.get_all_subscriptions()
    }

    pub async fn subscribe_partition_buckets(
        &self,
        partition_bucket_offsets: &HashMap<(PartitionId, i32), i64>,
    ) -> Result<()> {
        self.inner
            .subscribe_partition_buckets(partition_bucket_offsets)
            .await
    }

    pub async fn unsubscribe(&self, bucket: i32) -> Result<()> {
        self.inner.unsubscribe(bucket).await
    }

    pub async fn unsubscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
    ) -> Result<()> {
        self.inner.unsubscribe_partition(partition_id, bucket).await
    }

    /// Returns the Arrow schema for batches produced by this scanner.
    pub fn schema(&self) -> SchemaRef {
        self.inner.arrow_schema.clone()
    }

    pub fn table_path(&self) -> &TablePath {
        &self.inner.table_path
    }

    pub fn table_id(&self) -> TableId {
        self.inner.table_id
    }

    pub(crate) fn num_buckets(&self) -> i32 {
        self.inner.num_buckets
    }

    /// Subscribes non-partitioned ranges while the caller holds the active
    /// reader guard.
    pub(crate) async fn subscribe_buckets_for_reader(
        &self,
        bucket_offsets: &HashMap<i32, i64>,
    ) -> Result<()> {
        self.inner
            .subscribe_buckets_for_reader(bucket_offsets)
            .await
    }

    /// Subscribes partitioned ranges while the caller holds the active reader
    /// guard.
    pub(crate) async fn subscribe_partition_buckets_for_reader(
        &self,
        partition_bucket_offsets: &HashMap<(PartitionId, i32), i64>,
    ) -> Result<()> {
        self.inner
            .subscribe_partition_buckets_for_reader(partition_bucket_offsets)
            .await
    }

    /// Creates a new handle to the same underlying scanner state.
    ///
    /// Binding layers (Python, C++) that hold the scanner behind shared
    /// ownership (`Arc`) cannot move it into a [`crate::client::RecordBatchLogReader`].
    /// This method produces a second handle so the reader can take ownership
    /// while the binding retains its reference for subscription management.
    ///
    /// **Not intended for general use** — prefer moving the scanner directly.
    #[doc(hidden)]
    pub fn new_shared_handle(&self) -> Self {
        RecordBatchLogScanner {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Atomically marks the scanner as having an active reader.
    ///
    /// Returns `Err(IllegalArgument)` if another reader is already active on
    /// this scanner — only one [`crate::client::RecordBatchLogReader`] may
    /// iterate per scanner at a time. This mirrors Java's
    /// `LogScannerImpl.acquire()` single-consumer guard.
    pub(crate) fn try_set_reader_active(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        let _subscription_guard = self.inner.subscription_lock.lock();
        self.inner
            .reader_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| Error::IllegalArgument {
                message: "Another RecordBatchLogReader is already active on this scanner. \
                          Drop the existing reader first."
                    .to_string(),
            })
    }

    /// Clears the active-reader guard, re-enabling subscription changes.
    pub(crate) fn clear_reader_active(&self) {
        let _subscription_guard = self.inner.subscription_lock.lock();
        self.inner
            .reader_active
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Synchronous, infallible counterpart to [`unsubscribe`](Self::unsubscribe).
    ///
    /// Exists so [`crate::client::RecordBatchLogReader`]'s `Drop` impl can
    /// release lingering subscriptions without `.await`. The async version is
    /// also synchronous under the hood (it only acquires a lock and removes
    /// from a map — no IO), so this exposes the same work without the
    /// async wrapper. Silently no-ops on partitioned/non-partitioned mismatch
    /// because `Drop` cannot return errors; callers must pick the correct
    /// variant.
    ///
    /// **Not intended for general use** — prefer the async [`unsubscribe`].
    pub(crate) fn unsubscribe_sync(&self, bucket: i32) {
        let _subscription_guard = self.inner.subscription_lock.lock();
        if self.inner.is_partitioned_table {
            return;
        }
        let table_bucket = TableBucket::new(self.inner.table_id, bucket);
        self.inner
            .log_scanner_status
            .unassign_scan_buckets(from_ref(&table_bucket));
    }

    /// Synchronous, infallible counterpart to
    /// [`unsubscribe_partition`](Self::unsubscribe_partition). See
    /// [`unsubscribe_sync`](Self::unsubscribe_sync) for rationale.
    pub(crate) fn unsubscribe_partition_sync(&self, partition_id: PartitionId, bucket: i32) {
        let _subscription_guard = self.inner.subscription_lock.lock();
        if !self.inner.is_partitioned_table {
            return;
        }
        let table_bucket =
            TableBucket::new_with_partition(self.inner.table_id, Some(partition_id), bucket);
        self.inner
            .log_scanner_status
            .unassign_scan_buckets(from_ref(&table_bucket));
    }
}
