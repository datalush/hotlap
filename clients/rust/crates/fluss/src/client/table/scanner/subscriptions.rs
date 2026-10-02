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

//! Bucket subscription lifecycle and reader exclusivity.

use super::{
    Error, HashMap, LogScannerInner, PartitionId, Result, TableBucket, UnsupportedOperation,
    from_ref,
};

impl LogScannerInner {
    fn check_no_active_reader(&self) -> Result<()> {
        if self
            .reader_active
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::IllegalArgument {
                message: "Cannot modify subscriptions while a RecordBatchLogReader is active. \
                          Drop the reader first."
                    .to_string(),
            });
        }
        Ok(())
    }

    pub(super) async fn subscribe(&self, bucket: i32, offset: i64) -> Result<()> {
        self.check_no_active_reader()?;
        if self.is_partitioned_table {
            return Err(Error::UnsupportedOperation {
                message: "The table is a partitioned table, please use \"subscribe_partition\" to \
                subscribe a partitioned bucket instead."
                    .to_string(),
            });
        }
        let table_bucket = TableBucket::new(self.table_id, bucket);
        self.metadata
            .check_and_update_table_metadata(from_ref(&self.table_path))
            .await?;
        let _subscription_guard = self.subscription_lock.lock();
        self.check_no_active_reader()?;
        self.log_scanner_status
            .assign_scan_bucket(table_bucket, offset);
        Ok(())
    }

    pub(super) async fn subscribe_buckets(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()> {
        self.subscribe_buckets_internal(bucket_offsets, false).await
    }

    pub(super) async fn subscribe_buckets_for_reader(
        &self,
        bucket_offsets: &HashMap<i32, i64>,
    ) -> Result<()> {
        self.subscribe_buckets_internal(bucket_offsets, true).await
    }

    /// `reader_is_active` is `false` for subscriptions initiated through the
    /// scanner API, which must reject an active reader, and `true` during reader
    /// construction, which already holds the active-reader guard.
    async fn subscribe_buckets_internal(
        &self,
        bucket_offsets: &HashMap<i32, i64>,
        reader_is_active: bool,
    ) -> Result<()> {
        if !reader_is_active {
            self.check_no_active_reader()?;
        }
        if self.is_partitioned_table {
            return Err(Error::UnsupportedOperation {
                message:
                    "The table is a partitioned table, please use \"subscribe_partition_buckets\" instead."
                        .to_string(),
            });
        }

        let mut scan_bucket_offsets = HashMap::new();
        for (bucket_id, offset) in bucket_offsets {
            let table_bucket = TableBucket::new(self.table_id, *bucket_id);
            scan_bucket_offsets.insert(table_bucket, *offset);
        }
        self.do_subscribe_buckets(scan_bucket_offsets, reader_is_active)
            .await
    }

    pub(super) async fn subscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
        offset: i64,
    ) -> Result<()> {
        self.check_no_active_reader()?;
        if !self.is_partitioned_table {
            return Err(Error::UnsupportedOperation {
                message: "The table is not a partitioned table, please use \"subscribe\" to \
                subscribe a non-partitioned bucket instead."
                    .to_string(),
            });
        }
        let table_bucket =
            TableBucket::new_with_partition(self.table_id, Some(partition_id), bucket);
        self.metadata
            .check_and_update_partition_metadata_by_ids(&self.table_path, &[partition_id])
            .await?;
        let _subscription_guard = self.subscription_lock.lock();
        self.check_no_active_reader()?;
        self.log_scanner_status
            .assign_scan_bucket(table_bucket, offset);
        Ok(())
    }

    pub(super) async fn subscribe_partition_buckets(
        &self,
        partition_bucket_offsets: &HashMap<(PartitionId, i32), i64>,
    ) -> Result<()> {
        self.subscribe_partition_buckets_internal(partition_bucket_offsets, false)
            .await
    }

    pub(super) async fn subscribe_partition_buckets_for_reader(
        &self,
        partition_bucket_offsets: &HashMap<(PartitionId, i32), i64>,
    ) -> Result<()> {
        self.subscribe_partition_buckets_internal(partition_bucket_offsets, true)
            .await
    }

    /// `reader_is_active` is `false` for subscriptions initiated through the
    /// scanner API, which must reject an active reader, and `true` during reader
    /// construction, which already holds the active-reader guard.
    async fn subscribe_partition_buckets_internal(
        &self,
        partition_bucket_offsets: &HashMap<(PartitionId, i32), i64>,
        reader_is_active: bool,
    ) -> Result<()> {
        if !reader_is_active {
            self.check_no_active_reader()?;
        }
        if !self.is_partitioned_table {
            return Err(UnsupportedOperation {
                message: "The table is not a partitioned table, please use \"subscribe_buckets\" \
                    to subscribe to non-partitioned buckets instead."
                    .to_string(),
            });
        }

        let mut scan_bucket_offsets = HashMap::new();
        for (&(partition_id, bucket_id), &offset) in partition_bucket_offsets {
            let table_bucket =
                TableBucket::new_with_partition(self.table_id, Some(partition_id), bucket_id);
            scan_bucket_offsets.insert(table_bucket, offset);
        }
        self.do_subscribe_buckets(scan_bucket_offsets, reader_is_active)
            .await
    }

    async fn do_subscribe_buckets(
        &self,
        bucket_offsets: HashMap<TableBucket, i64>,
        reader_is_active: bool,
    ) -> Result<()> {
        if bucket_offsets.is_empty() {
            return Err(Error::UnexpectedError {
                message: "Bucket offsets are empty.".to_string(),
                source: None,
            });
        }

        if self.is_partitioned_table {
            let partition_ids: Vec<PartitionId> = bucket_offsets
                .keys()
                .filter_map(TableBucket::partition_id)
                .collect();
            self.metadata
                .check_and_update_partition_metadata_by_ids(&self.table_path, &partition_ids)
                .await?;
        } else {
            self.metadata
                .check_and_update_table_metadata(from_ref(&self.table_path))
                .await?;
        }

        let _subscription_guard = self.subscription_lock.lock();
        if reader_is_active {
            debug_assert!(
                self.reader_active
                    .load(std::sync::atomic::Ordering::Acquire),
                "reader-only subscription helper called without an active reader"
            );
        } else {
            self.check_no_active_reader()?;
        }
        self.log_scanner_status.assign_scan_buckets(bucket_offsets);
        Ok(())
    }

    pub(super) async fn unsubscribe(&self, bucket: i32) -> Result<()> {
        let _subscription_guard = self.subscription_lock.lock();
        self.check_no_active_reader()?;
        if self.is_partitioned_table {
            return Err(Error::UnsupportedOperation {
                message:
                    "The table is a partitioned table, please use \"unsubscribe_partition\" to \
                    unsubscribe a partitioned bucket instead."
                        .to_string(),
            });
        }
        let table_bucket = TableBucket::new(self.table_id, bucket);
        self.log_scanner_status
            .unassign_scan_buckets(from_ref(&table_bucket));
        Ok(())
    }

    pub(super) async fn unsubscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
    ) -> Result<()> {
        let _subscription_guard = self.subscription_lock.lock();
        self.check_no_active_reader()?;
        if !self.is_partitioned_table {
            return Err(Error::UnsupportedOperation {
                message: "Can't unsubscribe a partition for a non-partitioned table.".to_string(),
            });
        }
        let table_bucket =
            TableBucket::new_with_partition(self.table_id, Some(partition_id), bucket);
        self.log_scanner_status
            .unassign_scan_buckets(from_ref(&table_bucket));
        Ok(())
    }
}
