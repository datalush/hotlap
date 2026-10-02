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

//! Poll lifetime guard and time-between-polls instrumentation.

use super::*;

/// Snapshot state used to derive the scanner poll-timing metrics.
///
/// The mutex makes the state updates in `record_poll_start` /
/// `record_poll_end` atomic with respect to themselves; metric
/// emission (`metrics::gauge!(...).set(...)`) and `log::warn!` calls
/// happen after the lock is released so a user-installed recorder or
/// logger cannot stall the critical section. The mutex does **not** by
/// itself preserve start↔end pairing across overlapping `poll()` calls
/// — that invariant relies on the single-consumer contract that
/// mirrors Java's `LogScannerImpl.acquire()`. Concurrent polls on the
/// same scanner are detected by a `debug_assert!` in
/// `record_poll_start` (panics in debug / tests) and a `log::warn!` on
/// both anomalous paths (`record_poll_start` sees a stale `Some`;
/// `record_poll_end` sees `None`) for release-build observability.
#[derive(Default, Debug)]
pub(super) struct PollState {
    /// Instant captured at the most recent `record_poll_start()`. `None`
    /// before the first poll.
    pub(super) last_poll_at: Option<Instant>,
    /// Instant captured at the start of the in-flight poll. `None` after
    /// the last `record_poll_end()`.
    pub(super) poll_start_at: Option<Instant>,
    /// Cached ms between the two most recent poll starts, used to compute
    /// `poll_idle_ratio` in `record_poll_end`.
    pub(super) time_between_poll_ms: f64,
}

/// Pairs `record_poll_start` with `record_poll_end`. Created
/// at the top of `poll_records` / `poll_batches`; `record_poll_end` runs on
/// drop, including the cancellation path (caller drops the future).
pub(super) struct PollGuard<'a> {
    inner: &'a LogScannerInner,
}

impl<'a> PollGuard<'a> {
    pub(super) fn new(inner: &'a LogScannerInner) -> Self {
        inner.record_poll_start();
        Self { inner }
    }
}

impl Drop for PollGuard<'_> {
    fn drop(&mut self) {
        self.inner.record_poll_end();
    }
}

/// Single-tick emission for the `last_poll_seconds_ago` gauge. Reads the
/// last-poll timestamp from the shared atomic and pushes the elapsed
/// integer-seconds into the gauge.
///
/// Emission is skipped while the atomic still holds the sentinel `0` (no
/// `record_poll_start` yet) — Java's `(System.currentTimeMillis() - 0) /
/// 1000` startup nonsense (see `ScannerMetricGroup.java:121`) would trip
/// every consumer-liveness alert on startup. Java parity note: Java's
/// expression is integer-truncating (`long / long`); we preserve that with
/// `i64` division before the `f64` cast so dashboards built against Java
/// behave the same.
///
/// Extracted from the ticker loop so unit tests can exercise the emission
/// logic without depending on real-time scheduling.
pub(super) fn emit_last_poll_seconds_ago_once(
    last_poll_unix_ms: &AtomicI64,
    metrics: &ScannerMetrics,
) {
    let stored = last_poll_unix_ms.load(Ordering::Acquire);
    if stored == 0 {
        return;
    }
    let Ok(now_ms) = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
    else {
        return;
    };
    let seconds = ((now_ms - stored).max(0) / 1000) as f64;
    metrics.record_last_poll_seconds_ago(seconds);
}

/// Spawn the 1-second background tokio task that pushes
/// `last_poll_seconds_ago` into the gauge. The task holds only the shared
/// atomic timestamp and the metric handle — never an `Arc<LogScannerInner>`
/// — so it does not create a reference cycle that would block the scanner's
/// `Drop` (and hence the abort that stops this task).
///
/// `MissedTickBehavior::Delay` is used so a stalled runtime (e.g. test
/// pausing/advancing time) does not produce a burst of catch-up ticks when
/// it resumes.
pub(super) fn spawn_last_poll_seconds_ago_ticker(
    last_poll_unix_ms: Arc<AtomicI64>,
    metrics: Arc<ScannerMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            emit_last_poll_seconds_ago_once(&last_poll_unix_ms, &metrics);
        }
    })
}
