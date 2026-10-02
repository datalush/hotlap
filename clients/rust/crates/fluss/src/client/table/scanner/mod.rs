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

//! Log scanners: [`TableScan`] configures them, [`LogScanner`] reads records and
//! [`RecordBatchLogScanner`] reads Arrow batches. Implementations live in
//! `builder`, `api`/`runtime`, `requests`/`responses`/`records`/`batches`,
//! `subscriptions`/`polling`, `poll_timing`, and `status` so each read path can
//! be followed independently.

use crate::client::ClientSchemaGetter;
use crate::client::connection::FlussConnection;
use crate::client::credentials::SecurityTokenManager;
use crate::client::metadata::Metadata;
use crate::client::table::batch_scanner::LimitBatchScanner;
use crate::client::table::kv_scanner::KvBatchScanner;
use crate::client::table::log_fetch_buffer::{
    CompletedFetch, DefaultCompletedFetch, FetchErrorAction, FetchErrorContext, FetchErrorLogLevel,
    FetchResult, LogFetchBuffer, NO_FILTERED_END_OFFSET, RemotePendingFetch,
};
use crate::client::table::read_context_resolver::ReadContextResolver;
use crate::client::table::remote_log::{RemoteLogDownloader, RemoteLogFetchInfo};
use crate::config::Config;
use crate::error::Error::UnsupportedOperation;
use crate::error::{ApiError, Error, FlussError, Result};
use crate::metadata::{
    LogFormat, PhysicalTablePath, RowType, SchemaInfo, TableBucket, TableInfo, TablePath,
};
use crate::metrics::ScannerMetrics;
use crate::predicate::{Predicate, to_pb_predicate};
use crate::proto::{
    ErrorResponse, FetchLogRequest, FetchLogResponse, PbFetchLogReqForBucket,
    PbFetchLogReqForTable, PbPredicate,
};
use crate::record::{
    LogRecordsBatches, ReadContext, ScanBatch, ScanRecord, ScanRecords, to_arrow_schema,
};
use crate::rpc::{RpcClient, RpcError, message};
use crate::util::FairBucketStatusMap;
use crate::{PartitionId, TableId};
use arrow_schema::SchemaRef;
use log::{debug, warn};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{HashMap, HashSet},
    slice::from_ref,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
mod api;
mod batches;
mod builder;
mod fetch;
mod poll_timing;
mod polling;
mod records;
mod requests;
mod responses;
mod runtime;
mod status;
mod subscriptions;
#[cfg(test)]
mod tests;

pub use builder::TableScan;
#[cfg(test)]
use builder::{
    validate_limit_scan_fixed_schema, validate_scan_support, validate_scan_support_inner,
};
#[cfg(test)]
use poll_timing::emit_last_poll_seconds_ago_once;
use poll_timing::{PollGuard, PollState, spawn_last_poll_seconds_ago_ticker};
use status::LogScannerStatus;

pub struct LogScanner {
    inner: Arc<LogScannerInner>,
}

/// Scanner for reading log data as Arrow RecordBatches.
///
/// More efficient than [`LogScanner`] for batch-level analytics where per-record
/// metadata (offsets, timestamps) is not needed.
///
/// This type is intentionally **not** `Clone`. To perform a bounded read, move
/// the scanner into a [`crate::client::RecordBatchLogReader`] — the compiler
/// then prevents concurrent polls by construction.
pub struct RecordBatchLogScanner {
    inner: Arc<LogScannerInner>,
}

/// Private shared implementation for both scanner types
struct LogScannerInner {
    table_path: TablePath,
    table_id: TableId,
    num_buckets: i32,
    metadata: Arc<Metadata>,
    log_scanner_status: Arc<LogScannerStatus>,
    log_fetcher: LogFetcher,
    is_partitioned_table: bool,
    arrow_schema: SchemaRef,
    /// Guards against subscription changes while a
    /// [`crate::client::RecordBatchLogReader`] is iterating.
    reader_active: std::sync::atomic::AtomicBool,
    /// Serializes the active-reader transition with subscription mutations.
    ///
    /// Public subscribe methods await metadata before changing the status map.
    /// Without this lock, a subscribe call that passed the initial
    /// `reader_active` check could finish after a bounded reader became active.
    subscription_lock: Mutex<()>,
    /// Holds the snapshot fields used by [`PollGuard`] to derive the
    /// scanner poll-timing metrics. The mutex makes the state updates
    /// in `record_poll_start` / `record_poll_end` atomic; metric
    /// emission and `log::warn!` calls happen after the lock is
    /// released. The start↔end pairing depends on the single-consumer
    /// contract documented on [`LogScanner::poll`] and
    /// [`RecordBatchLogScanner::poll`] (mirrors Java's
    /// `LogScannerImpl.acquire()`). Overlapping polls on the same
    /// scanner trip a `debug_assert!` in `record_poll_start` (debug
    /// builds) or emit a `log::warn!` (release builds).
    poll_state: Mutex<PollState>,
    /// Per-table scanner metric handles, pre-bound with `database`/`table`
    /// labels.
    metrics: Arc<ScannerMetrics>,
    /// Wall-clock millis (since `UNIX_EPOCH`) of the most recent
    /// `record_poll_start`. Sentinel `0` means "no poll yet" — the
    /// `last_poll_seconds_ago` ticker skips emission while this is `0`,
    /// deviating from Java's unguarded `(now - 0)/1000` startup value.
    ///
    /// Written by `record_poll_start` with `Release` ordering, read by
    /// the ticker task with `Acquire` ordering. Cloned (`Arc`) into the
    /// ticker so the task does not hold a back-reference to
    /// `LogScannerInner` (avoids a reference cycle that would block
    /// `Drop`, and hence the ticker abort, until tokio runtime
    /// shutdown).
    last_poll_unix_ms: Arc<AtomicI64>,
    /// Handle to the 1-second background tokio task that pushes
    /// `last_poll_seconds_ago` into the gauge. Aborted from
    /// `impl Drop for LogScannerInner` so the gauge stops emitting once
    /// the scanner is closed.
    last_poll_seconds_ago_task: JoinHandle<()>,
}

struct LogFetcher {
    conns: Arc<RpcClient>,
    metadata: Arc<Metadata>,
    table_path: TablePath,
    is_partitioned: bool,
    log_scanner_status: Arc<LogScannerStatus>,
    resolver: Arc<ReadContextResolver>,
    remote_log_downloader: Arc<RemoteLogDownloader>,
    /// Background security token manager for remote filesystem access.
    /// Kept alive to run the background refresh task; stopped on drop.
    #[allow(dead_code)]
    security_token_manager: Arc<SecurityTokenManager>,
    log_fetch_buffer: Arc<LogFetchBuffer>,
    nodes_with_pending_fetch_requests: Arc<Mutex<HashSet<i32>>>,
    /// Per-table scanner metric handles shared with the owning
    /// `LogScannerInner` and `RemoteLogDownloader`.
    metrics: Arc<ScannerMetrics>,
    /// Encoded filter sent on every fetch request, paired with the schema id it
    /// was compiled against so the server can resolve its field ids.
    filter: Option<(PbPredicate, i32)>,
    /// Routing counts captured alongside the partition IDs for a bounded scan.
    partition_bucket_counts: RwLock<HashMap<PartitionId, i32>>,
    max_poll_records: usize,
    fetch_max_bytes: i32,
    fetch_min_bytes: i32,
    fetch_wait_max_time_ms: i32,
    fetch_max_bytes_for_bucket: i32,
}

struct FetchResponseContext {
    metadata: Arc<Metadata>,
    log_fetch_buffer: Arc<LogFetchBuffer>,
    log_scanner_status: Arc<LogScannerStatus>,
    resolver: Arc<ReadContextResolver>,
    remote_log_downloader: Arc<RemoteLogDownloader>,
    /// Per-table scanner metric handles for `scanner.fetch_*` recording.
    metrics: Arc<ScannerMetrics>,
    /// `Instant` captured immediately before the FetchLog RPC; used to compute
    /// `scanner.fetch_latency_ms` on a successful response.
    request_start_time: Instant,
}
