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

//! Log scanner configuration, runtime, fetch and bucket status.

//! Shared scanner lifecycle, subscriptions and polling for both public APIs.

use super::*;

impl Drop for LogScannerInner {
    fn drop(&mut self) {
        self.last_poll_seconds_ago_task.abort();
    }
}

impl LogScannerInner {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        table_info: &TableInfo,
        metadata: Arc<Metadata>,
        connections: Arc<RpcClient>,
        config: &Config,
        projected_fields: Option<Vec<usize>>,
        fixed_schema: bool,
        filter: Option<PbPredicate>,
        admin: Arc<crate::client::admin::FlussAdmin>,
    ) -> Result<Self> {
        let log_scanner_status = Arc::new(LogScannerStatus::new());

        let full_row_type = table_info.get_row_type();
        let arrow_schema = match &projected_fields {
            Some(indices) => {
                let projected_fields_vec: Vec<_> = indices
                    .iter()
                    .map(|&i| full_row_type.fields()[i].clone())
                    .collect();
                let projected_row_type = crate::metadata::RowType::new(projected_fields_vec);
                to_arrow_schema(&projected_row_type)?
            }
            None => to_arrow_schema(full_row_type)?,
        };

        // Create schema getter for schema evolution support
        let latest_schema =
            SchemaInfo::new(table_info.get_schema().clone(), table_info.get_schema_id());
        let schema_getter = Arc::new(ClientSchemaGetter::new(
            table_info.table_path.clone(),
            admin,
            latest_schema,
        ));

        let metrics = Arc::new(ScannerMetrics::new(&table_info.table_path));
        let last_poll_unix_ms = Arc::new(AtomicI64::new(0));
        let last_poll_seconds_ago_task = spawn_last_poll_seconds_ago_ticker(
            Arc::clone(&last_poll_unix_ms),
            Arc::clone(&metrics),
        );
        Ok(Self {
            table_path: table_info.table_path.clone(),
            table_id: table_info.table_id,
            num_buckets: table_info.get_num_buckets(),
            is_partitioned_table: table_info.is_partitioned(),
            metadata: metadata.clone(),
            log_scanner_status: log_scanner_status.clone(),
            log_fetcher: LogFetcher::new(
                table_info.clone(),
                connections,
                metadata,
                log_scanner_status.clone(),
                config,
                projected_fields,
                fixed_schema,
                filter,
                Arc::clone(&metrics),
                schema_getter,
            )?,
            arrow_schema,
            reader_active: std::sync::atomic::AtomicBool::new(false),
            subscription_lock: Mutex::new(()),
            poll_state: Mutex::new(PollState::default()),
            metrics,
            last_poll_unix_ms,
            last_poll_seconds_ago_task,
        })
    }

    pub(super) fn check_no_active_reader(&self) -> Result<()> {
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

    pub(super) async fn poll_records(&self, timeout: Duration) -> Result<ScanRecords> {
        // Pairs record_poll_start (now) with record_poll_end
        // (drop). Runs on every exit, including the cancellation path
        // where the caller drops this future.
        let _poll_guard = PollGuard::new(self);
        let start = Instant::now();
        let deadline = start + timeout;

        loop {
            // Try to collect fetches
            let fetch_result = self.poll_for_fetches().await?;

            if !fetch_result.is_empty() {
                // We have data, send next round of fetches and return
                // This enables pipelining while user processes the data
                self.log_fetcher.send_fetches().await?;
                return Ok(ScanRecords::new(fetch_result));
            }

            // No data available, check if we should wait
            let now = Instant::now();
            if now >= deadline {
                // Timeout reached, return empty result
                return Ok(ScanRecords::new(HashMap::new()));
            }

            // Wait for buffer to become non-empty with remaining time
            let remaining = deadline - now;
            let has_data = self
                .log_fetcher
                .log_fetch_buffer
                .await_not_empty(remaining)
                .await?;

            if !has_data {
                // Timeout while waiting
                return Ok(ScanRecords::new(HashMap::new()));
            }

            // Buffer became non-empty, try again
        }
    }

    /// Records the start of a `poll()` call and emits
    /// `SCANNER_TIME_BETWEEN_POLL_MS`. The first poll emits `0.0`,
    /// matching Java's `ScannerMetricGroup.recordPollStart`
    /// (`timeMsBetweenPoll = lastPollMs != 0L ? pollStartMs - lastPollMs : 0L`).
    ///
    /// Single-consumer contract: a previous poll must have recorded its
    /// end before the next start. Java enforces this with
    /// `LogScannerImpl.acquire()` (throws `ConcurrentModificationException`).
    /// Rust surfaces violations as:
    /// - debug builds: `debug_assert!` panics (caught by tests),
    /// - release builds: `log::warn!` + the in-flight `poll_start_at` is
    ///   overwritten so the metric series keeps moving; the resulting
    ///   `time_between_poll_ms` / `poll_idle_ratio` values for the
    ///   overlapping polls are not meaningful until the overlap clears.
    pub(super) fn record_poll_start(&self) {
        let now = Instant::now();
        // Compute under the lock; emit the metric outside the critical
        // section so a user-installed recorder cannot stall the next poll.
        let (between_ms, overlap) = {
            let mut state = self.poll_state.lock();
            let overlap = state.poll_start_at.is_some();
            debug_assert!(
                !overlap,
                "concurrent poll() detected on the same scanner; \
                 LogScanner / RecordBatchLogScanner are single-consumer \
                 (see LogScannerImpl.acquire() for Java parity)"
            );
            let between_ms = match state.last_poll_at {
                Some(prev) => now.duration_since(prev).as_secs_f64() * 1000.0,
                None => 0.0,
            };
            state.time_between_poll_ms = between_ms;
            state.last_poll_at = Some(now);
            state.poll_start_at = Some(now);
            (between_ms, overlap)
        };
        if overlap {
            warn!(
                "concurrent poll() detected on scanner; single-consumer \
                 contract violated, poll-timing metrics will be inaccurate \
                 until the overlap clears"
            );
        }
        self.metrics.record_time_between_poll_ms(between_ms);

        // Publish the wall-clock timestamp the ticker uses to compute
        // `last_poll_seconds_ago`. Use `SystemTime` rather than `Instant`
        // because the ticker needs an absolute clock to diff against
        // `SystemTime::now()` at arbitrary moments. `Release` pairs with the
        // ticker's `Acquire` load. If the system clock is somehow before
        // `UNIX_EPOCH` (vanishingly rare; pre-1970 wall clock), we keep the
        // existing value so we never publish a negative timestamp that would
        // produce a bogus gauge reading on the next tick.
        if let Ok(unix_ms) = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
        {
            self.last_poll_unix_ms.store(unix_ms, Ordering::Release);
        }
    }

    /// Computes `poll_idle_ratio = poll_time / (poll_time + between_time)`.
    /// On the first poll, `between_time` is 0 so the ratio is 1.0
    /// (poll-bound).
    ///
    /// Orphan call: if no matching `record_poll_start` is in flight,
    /// emits a `log::warn!` (single-consumer contract may have been
    /// violated, e.g. in release builds where the start-side
    /// `debug_assert!` is compiled out) and skips the metric update.
    pub(super) fn record_poll_end(&self) {
        let now = Instant::now();
        // Compute under the lock; emit metric / warn outside the critical
        // section so neither the user-installed recorder nor the logger
        // can stall the next poll.
        let (orphan, ratio) = {
            let mut state = self.poll_state.lock();
            match state.poll_start_at.take() {
                None => (true, None),
                Some(start) => {
                    let poll_time_ms = now.duration_since(start).as_secs_f64() * 1000.0;
                    let total = poll_time_ms + state.time_between_poll_ms;
                    let r = (total > 0.0).then_some(poll_time_ms / total);
                    (false, r)
                }
            }
        };
        if orphan {
            warn!(
                "record_poll_end called without a matching record_poll_start; \
                 single-consumer contract may have been violated, idle ratio \
                 for this poll is not emitted"
            );
            return;
        }
        if let Some(r) = ratio {
            self.metrics.record_poll_idle_ratio(r);
        }
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
    pub(super) async fn subscribe_buckets_internal(
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
    pub(super) async fn subscribe_partition_buckets_internal(
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

    pub(super) async fn do_subscribe_buckets(
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

    pub(super) async fn poll_for_fetches(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        let result = self.log_fetcher.collect_fetches().await?;
        if !result.is_empty() {
            return Ok(result);
        }

        // send any new fetches (won't resend pending fetches).
        self.log_fetcher.send_fetches().await?;

        // Collect completed fetches from buffer
        self.log_fetcher.collect_fetches().await
    }

    pub(super) async fn poll_batches(&self, timeout: Duration) -> Result<Vec<ScanBatch>> {
        let _poll_guard = PollGuard::new(self);
        let start = Instant::now();
        let deadline = start + timeout;

        loop {
            let batches = self.poll_for_batches().await?;

            if !batches.is_empty() {
                self.log_fetcher.send_fetches().await?;
                return Ok(batches);
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(Vec::new());
            }

            let remaining = deadline - now;
            let has_data = self
                .log_fetcher
                .log_fetch_buffer
                .await_not_empty(remaining)
                .await?;

            if !has_data {
                return Ok(Vec::new());
            }
        }
    }

    pub(super) async fn poll_for_batches(&self) -> Result<Vec<ScanBatch>> {
        let result = self.log_fetcher.collect_batches().await?;
        if !result.is_empty() {
            return Ok(result);
        }

        self.log_fetcher.send_fetches().await?;
        self.log_fetcher.collect_batches().await
    }
}

// Implementation for LogScanner (records mode)
