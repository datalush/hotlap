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

//! Poll records or Arrow batches from the fetcher.

use super::{
    Duration, HashMap, Instant, LogScannerInner, PollGuard, Result, ScanBatch, ScanRecord,
    ScanRecords, TableBucket,
};

impl LogScannerInner {
    pub(super) async fn poll_records(&self, timeout: Duration) -> Result<ScanRecords> {
        // Pairs record_poll_start (now) with record_poll_end
        // (drop). Runs on every exit, including the cancellation path
        // where the caller drops this future.
        let _poll_guard = PollGuard::new(self);
        let start = Instant::now();
        let deadline =
            start
                .checked_add(timeout)
                .ok_or_else(|| crate::error::Error::IllegalArgument {
                    message: "Scanner poll timeout exceeds the clock range".into(),
                })?;

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
    async fn poll_for_fetches(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        let result = self.log_fetcher.collect_fetches().await?;
        if !result.is_empty() {
            return Ok(result);
        }

        // send any new fetches (won't resend pending fetches).
        self.log_fetcher.send_fetches().await?;

        // Collect completed fetches from buffer
        self.log_fetcher.collect_fetches().await
    }

    pub(super) async fn poll_batches(
        &self,
        timeout: Duration,
        max_batches: usize,
    ) -> Result<Vec<ScanBatch>> {
        let _poll_guard = PollGuard::new(self);
        let start = Instant::now();
        let deadline =
            start
                .checked_add(timeout)
                .ok_or_else(|| crate::error::Error::IllegalArgument {
                    message: "Scanner poll timeout exceeds the clock range".into(),
                })?;

        loop {
            let batches = self.poll_for_batches(max_batches).await?;

            if !batches.is_empty() {
                // Do not await metadata/network work after consuming batches:
                // cancellation would lose them while offsets already advanced.
                // The next poll sends more fetches when buffered data is drained.
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

    async fn poll_for_batches(&self, max_batches: usize) -> Result<Vec<ScanBatch>> {
        let result = self
            .log_fetcher
            .collect_batches_limited(max_batches)
            .await?;
        if !result.is_empty() {
            return Ok(result);
        }

        self.log_fetcher.send_fetches().await?;
        self.log_fetcher.collect_batches_limited(max_batches).await
    }
}
