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

use super::*;
use prost::Message;

/// Exercises the `PollGuard` lifecycle across two consecutive
/// `record_poll_start` calls. Asserts both poll-timing gauges are
/// emitted at the right moments and `record_poll_end` runs on guard
/// drop (also the cancellation-safety path, since dropping the
/// `poll()` future drops the guard).
#[test]
fn poll_guard_emits_time_between_poll_and_idle_ratio() {
    use crate::metrics::{SCANNER_POLL_IDLE_RATIO, SCANNER_TIME_BETWEEN_POLL_MS};
    use metrics_util::debugging::DebuggingRecorder;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    metrics::with_local_recorder(&recorder, || {
        with_test_log_scanner_inner(|inner| {
            // First poll: emits time_between_poll_ms=0 (Java parity:
            // ScannerMetricGroup.recordPollStart emits 0 when there is
            // no previous poll). Idle ratio is also emitted as 1.0
            // on drop (poll_time / (poll_time + 0) = 1.0).
            {
                let _g = PollGuard::new(inner);
                std::thread::sleep(std::time::Duration::from_millis(5));
            }

            // Brief gap so time_between_poll_ms is observably > 0.
            std::thread::sleep(std::time::Duration::from_millis(5));

            // Second poll: refreshes both time_between_poll_ms (>0)
            // and a fresh idle ratio.
            {
                let _g = PollGuard::new(inner);
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        });
    });

    let between = snapshot_gauge(&snapshotter, SCANNER_TIME_BETWEEN_POLL_MS)
        .expect("time_between_poll_ms must be emitted on every poll");
    assert!(
        between > 0.0,
        "second-poll time_between_poll_ms must be positive, got {between}"
    );

    let ratio = snapshot_gauge(&snapshotter, SCANNER_POLL_IDLE_RATIO)
        .expect("poll_idle_ratio must be emitted on poll end");
    assert!(
        (0.0..=1.0).contains(&ratio),
        "poll_idle_ratio must be in [0, 1], got {ratio}"
    );

    // Both gauges must carry `database=db` / `table=tbl` (the fixture
    // values from `with_test_log_scanner_inner`).
    assert_scanner_entries_labeled(&snapshotter.snapshot().into_vec(), "db", "tbl");
}

/// Java parity: `ScannerMetricGroup.recordPollStart` emits
/// `timeMsBetweenPoll = 0` on the very first poll. The Rust gauge
/// must do the same so dashboards see the metric series from poll #1.
#[test]
fn time_between_poll_ms_emits_zero_on_first_poll() {
    use crate::metrics::SCANNER_TIME_BETWEEN_POLL_MS;
    use metrics_util::debugging::DebuggingRecorder;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    metrics::with_local_recorder(&recorder, || {
        with_test_log_scanner_inner(|inner| {
            let _g = PollGuard::new(inner);
            // Drop at end of scope completes the poll; the value of
            // SCANNER_TIME_BETWEEN_POLL_MS was emitted at start, not end.
        });
    });

    let between = snapshot_gauge(&snapshotter, SCANNER_TIME_BETWEEN_POLL_MS)
        .expect("time_between_poll_ms must be emitted on the first poll");
    assert_eq!(
        between, 0.0,
        "first-poll time_between_poll_ms must be 0.0 (Java parity), got {between}"
    );
    assert_scanner_entries_labeled(&snapshotter.snapshot().into_vec(), "db", "tbl");
}

/// Pins the single-consumer contract: overlapping `PollGuard`s on the
/// same scanner trip the `debug_assert!` in `record_poll_start`.
/// Release builds skip the check, so the test is gated on
/// `debug_assertions`.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "concurrent poll() detected")]
fn overlapping_polls_panic_in_debug_builds() {
    with_test_log_scanner_inner(|inner| {
        let _g1 = PollGuard::new(inner);
        // _g1 has not been dropped → poll_start_at is still Some,
        // so the second start must panic.
        let _g2 = PollGuard::new(inner);
    });
}

/// Drives `handle_fetch_response` against a local metrics recorder and
/// asserts that latency + bytes-per-request histograms are emitted with
/// values that mirror what Java would record. This complements the unit
/// tests in `metrics.rs` (which only verify the facade) by exercising
/// the actual instrumented call path.
///
/// Note: uses a `current_thread` runtime inside `with_local_recorder`
/// (rather than `#[tokio::test]`) because the metrics facade installs a
/// thread-local recorder; running the async work on the same thread is
/// the only way to observe the emitted metrics in the snapshot. Both
/// the fetcher construction and the `handle_fetch_response` call run
/// inside the runtime (the security-token manager and remote-log
/// downloader require a Tokio reactor).
#[test]
fn handle_fetch_response_emits_latency_and_bytes_metrics() {
    use crate::metrics::{SCANNER_BYTES_PER_REQUEST, SCANNER_FETCH_LATENCY_MS};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    let expected_bytes = metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current_thread runtime");

        rt.block_on(async {
            let table_path = TablePath::new("db".to_string(), "tbl".to_string());
            let table_info = build_table_info(table_path.clone(), 1, 1);
            let cluster = build_cluster_arc(&table_path, 1, 1);
            let metadata = Arc::new(Metadata::new_for_test(cluster));
            let status = Arc::new(LogScannerStatus::new());
            status.assign_scan_bucket(TableBucket::new(1, 0), 5);
            let fetcher = LogFetcher::new(
                table_info.clone(),
                Arc::new(RpcClient::new()),
                metadata.clone(),
                status,
                &Config::default(),
                None,
                false,
                None,
                test_scanner_metrics(&table_path),
                test_schema_getter(&table_info, &metadata),
            )
            .expect("build LogFetcher");

            let response = FetchLogResponse {
                tables_resp: vec![PbFetchLogRespForTable {
                    table_id: 1,
                    buckets_resp: vec![PbFetchLogRespForBucket {
                        partition_id: None,
                        bucket_id: 0,
                        error_code: Some(FlussError::None.code()),
                        error_message: None,
                        high_watermark: Some(7),
                        log_start_offset: Some(0),
                        remote_log_fetch_info: None,
                        records: None,
                        filtered_end_offset: None,
                        min_retain_offset: None,
                    }],
                }],
            };
            let expected_bytes = response.encoded_len() as f64;
            let response_context = FetchResponseContext {
                metadata: metadata.clone(),
                log_fetch_buffer: fetcher.log_fetch_buffer.clone(),
                log_scanner_status: fetcher.log_scanner_status.clone(),
                resolver: Arc::clone(&fetcher.resolver),
                remote_log_downloader: fetcher.remote_log_downloader.clone(),
                metrics: Arc::clone(&fetcher.metrics),
                request_start_time: Instant::now(),
            };

            LogFetcher::handle_fetch_response(response, response_context).await;
            expected_bytes
        })
    });

    let entries: Vec<_> = snapshotter.snapshot().into_vec();
    let find_histogram = |name: &str| -> Vec<f64> {
        entries
            .iter()
            .find_map(|(key, _, _, val)| {
                if key.key().name() == name {
                    if let DebugValue::Histogram(v) = val {
                        return Some(v.iter().map(|f| f.into_inner()).collect());
                    }
                }
                None
            })
            .unwrap_or_default()
    };

    let latency_samples = find_histogram(SCANNER_FETCH_LATENCY_MS);
    assert_eq!(latency_samples.len(), 1, "expected one latency sample");
    assert!(
        latency_samples[0] >= 0.0,
        "latency must be non-negative, got {}",
        latency_samples[0]
    );

    let bytes_samples = find_histogram(SCANNER_BYTES_PER_REQUEST);
    assert_eq!(
        bytes_samples,
        vec![expected_bytes],
        "bytes histogram must record encoded_len() for parity with Java fetchLogResponse.totalSize()",
    );

    // Every emitted scanner metric must carry both `database` and `table`
    // labels — that's the whole point of `ScannerMetrics`. If a future
    // contributor adds a new `metrics::*!` macro inline (bypassing
    // `ScannerMetrics`), this assertion catches it.
    assert_scanner_entries_labeled(&entries, "db", "tbl");
}

/// `emit_last_poll_seconds_ago_once` must skip emission while the
/// shared atomic still holds the sentinel `0` — that's the
/// pre-first-poll guard that prevents Java's
/// `(System.currentTimeMillis() - 0) / 1000` startup nonsense from
/// tripping consumer-liveness alerts before any poll happens.
///
/// `ScannerMetrics::new` already registers the gauge with the
/// recorder, so it appears in the snapshot with the default `0.0`
/// even without any emission. The discriminating assertion is that
/// the value stays near zero rather than blowing up to ~1.7 billion
/// (current Unix-epoch seconds), which is what a broken skip would
/// produce.
#[test]
fn emit_last_poll_seconds_ago_skips_sentinel_value() {
    use crate::metrics::SCANNER_LAST_POLL_SECONDS_AGO;
    use metrics_util::debugging::DebuggingRecorder;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    metrics::with_local_recorder(&recorder, || {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let metrics = ScannerMetrics::new(&table_path);
        let last_poll = AtomicI64::new(0);

        for _ in 0..3 {
            emit_last_poll_seconds_ago_once(&last_poll, &metrics);
        }
    });

    let value = snapshot_gauge(&snapshotter, SCANNER_LAST_POLL_SECONDS_AGO)
        .expect("ScannerMetrics::new registers the gauge so it appears in the snapshot");
    assert!(
        value < 1.0,
        "pre-first-poll emission must be skipped; broken skip would push ~unix-epoch \
             seconds (~1.7e9) into the gauge, got {value}"
    );
}

/// Once a real timestamp has been published, the helper must emit
/// `floor((now - stored) / 1000)` matching Java's integer-truncating
/// `(System.currentTimeMillis() - lastPollMs) / 1000`. Tolerance
/// allows for real wall-clock progression between the test setting
/// up `stored` and the helper reading `SystemTime::now()`.
///
/// Also covers the reset-after-fresh-poll case: updating the
/// stored timestamp to "now" must drop the next emission back near
/// zero, matching the property "gauge resets when a new poll
/// happens".
#[test]
fn emit_last_poll_seconds_ago_publishes_integer_truncated_elapsed() {
    use crate::metrics::SCANNER_LAST_POLL_SECONDS_AGO;
    use metrics_util::debugging::DebuggingRecorder;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    metrics::with_local_recorder(&recorder, || {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let metrics = ScannerMetrics::new(&table_path);

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .expect("wall clock after UNIX_EPOCH");
        let last_poll = AtomicI64::new(now_ms - 5_500);

        emit_last_poll_seconds_ago_once(&last_poll, &metrics);

        let value = snapshot_gauge(&snapshotter, SCANNER_LAST_POLL_SECONDS_AGO)
            .expect("gauge must emit once a real timestamp is published");
        assert!(
            (5.0..=6.0).contains(&value),
            "gauge must be ~5 (5500ms truncated to 5s, plus test scheduling slack), got {value}"
        );

        // Simulate a fresh poll: update the shared atomic to "right
        // now". The next emission must collapse the gauge back near
        // zero.
        let fresh_now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .expect("wall clock after UNIX_EPOCH");
        last_poll.store(fresh_now_ms, Ordering::Release);

        emit_last_poll_seconds_ago_once(&last_poll, &metrics);

        let reset = snapshot_gauge(&snapshotter, SCANNER_LAST_POLL_SECONDS_AGO)
            .expect("gauge must still be present after the second emission");
        assert!(
            (0.0..=1.0).contains(&reset),
            "fresh poll must reset gauge near zero, got {reset}"
        );
    });

    assert_scanner_entries_labeled(&snapshotter.snapshot().into_vec(), "db", "tbl");
}

/// Negative `now - stored` (e.g. wall-clock jumps backwards via NTP)
/// must clamp to 0, not produce a negative gauge reading.
#[test]
fn emit_last_poll_seconds_ago_clamps_negative_delta_to_zero() {
    use crate::metrics::SCANNER_LAST_POLL_SECONDS_AGO;
    use metrics_util::debugging::DebuggingRecorder;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    metrics::with_local_recorder(&recorder, || {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let metrics = ScannerMetrics::new(&table_path);

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .expect("wall clock after UNIX_EPOCH");
        // Stored timestamp in the future → negative delta.
        let last_poll = AtomicI64::new(now_ms + 60_000);

        emit_last_poll_seconds_ago_once(&last_poll, &metrics);
    });

    let value = snapshot_gauge(&snapshotter, SCANNER_LAST_POLL_SECONDS_AGO)
        .expect("gauge must emit even when delta is clamped to 0");
    assert_eq!(value, 0.0, "negative delta must clamp to 0, got {value}");
}

/// `spawn_last_poll_seconds_ago_ticker` returns a `JoinHandle` whose
/// `abort()` cleanly terminates the loop. Pins the lifecycle pattern
/// that `impl Drop for LogScannerInner` relies on.
#[tokio::test(flavor = "current_thread")]
async fn spawn_last_poll_seconds_ago_ticker_aborts_cleanly() {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let metrics = Arc::new(ScannerMetrics::new(&table_path));
    let last_poll_unix_ms = Arc::new(AtomicI64::new(0));

    let handle =
        spawn_last_poll_seconds_ago_ticker(Arc::clone(&last_poll_unix_ms), Arc::clone(&metrics));
    assert!(
        !handle.is_finished(),
        "freshly spawned ticker must be alive"
    );

    handle.abort();
    let join_result = handle.await;
    assert!(
        join_result.is_err() && join_result.unwrap_err().is_cancelled(),
        "abort must cancel the ticker, not let it complete normally"
    );
}

/// End-to-end test of the *spawned* ticker (not just the extracted
/// `emit_last_poll_seconds_ago_once` helper): the interval loop must
/// emit on its first tick and keep emitting on subsequent ticks,
/// reflecting the latest published timestamp each time.
///
/// Uses a paused-clock `current_thread` runtime so the second tick can
/// be driven deterministically with `tokio::time::advance` instead of
/// sleeping a real second. Note the elapsed value is derived from
/// wall-clock `SystemTime`, which `advance` does *not* move — so the
/// gauge value is controlled by what we store in the atomic (a known
/// past / present wall-clock timestamp), and `advance` is used only to
/// fire the parked 1-second interval timer.
///
/// Built inside `with_local_recorder` (rather than `#[tokio::test]`)
/// because the metrics facade installs a thread-local recorder; the
/// spawned task is polled on the same thread during `block_on`, so its
/// `gauge!` calls resolve to this local recorder.
#[test]
fn spawned_ticker_emits_on_first_and_subsequent_ticks() {
    use crate::metrics::SCANNER_LAST_POLL_SECONDS_AGO;
    use metrics_util::debugging::DebuggingRecorder;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("build paused current_thread runtime");

        rt.block_on(async {
            let table_path = TablePath::new("db".to_string(), "tbl".to_string());
            let metrics = Arc::new(ScannerMetrics::new(&table_path));
            let last_poll_unix_ms = Arc::new(AtomicI64::new(0));

            // Simulate a poll that started ~5s ago (wall clock) before
            // the ticker runs, so the first (immediate) tick emits ~5.
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .expect("wall clock after UNIX_EPOCH");
            last_poll_unix_ms.store(now_ms - 5_000, Ordering::Release);

            let handle = spawn_last_poll_seconds_ago_ticker(
                Arc::clone(&last_poll_unix_ms),
                Arc::clone(&metrics),
            );

            // `tokio::time::interval` fires its first tick immediately,
            // so a few yields let the spawned task run that first
            // emission without advancing the clock.
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }

            let first = snapshot_gauge(&snapshotter, SCANNER_LAST_POLL_SECONDS_AGO)
                .expect("spawned ticker must emit on its first (immediate) tick");
            assert!(
                (5.0..=6.0).contains(&first),
                "first tick must reflect ~5s elapsed, got {first}"
            );

            // Simulate a fresh poll "now", then advance the paused clock
            // by 1s to fire the parked second interval tick. The loop
            // must emit again, this time near zero.
            let fresh_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .expect("wall clock after UNIX_EPOCH");
            last_poll_unix_ms.store(fresh_ms, Ordering::Release);

            tokio::time::advance(Duration::from_secs(1)).await;
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }

            let second = snapshot_gauge(&snapshotter, SCANNER_LAST_POLL_SECONDS_AGO)
                .expect("spawned ticker must keep emitting on subsequent ticks");
            assert!(
                (0.0..=1.0).contains(&second),
                "second tick after a fresh poll must reset gauge near zero, got {second}"
            );

            handle.abort();
        });
    });

    assert_scanner_entries_labeled(&snapshotter.snapshot().into_vec(), "db", "tbl");
}

/// `LogScannerInner::drop` must abort the ticker task so the gauge
/// stops emitting once the scanner is closed — mirrors Java's
/// `ScannerMetricGroup.close()`. The atomic is shared with the task,
/// so we use its `Arc::strong_count` as an indirect liveness probe:
/// once the runtime processes the abort and drops the task's future,
/// the task's clone of the `Arc` is released, leaving only the one
/// we hold here.
#[test]
fn log_scanner_inner_drop_aborts_ticker_task() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current_thread runtime");
    rt.block_on(async {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = build_table_info(table_path.clone(), 1, 1);
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let rpc_client = Arc::new(RpcClient::new());
        let admin = Arc::new(crate::client::admin::FlussAdmin::new(
            rpc_client.clone(),
            metadata.clone(),
        ));
        let inner = LogScannerInner::new(
            &table_info,
            metadata,
            rpc_client,
            &Config::default(),
            None,
            false,
            None,
            admin,
        )
        .expect("build LogScannerInner");

        let abort_handle = inner.last_poll_seconds_ago_task.abort_handle();
        assert!(
            !abort_handle.is_finished(),
            "ticker must be alive before scanner drop"
        );

        drop(inner);

        // Yield repeatedly so the runtime can process the abort.
        // Cap at a generous iteration count to avoid hanging the test
        // if Drop ever stops calling `abort()`.
        for _ in 0..32 {
            tokio::task::yield_now().await;
            if abort_handle.is_finished() {
                break;
            }
        }
        assert!(
            abort_handle.is_finished(),
            "Drop for LogScannerInner must abort the last_poll_seconds_ago ticker"
        );
    });
}
