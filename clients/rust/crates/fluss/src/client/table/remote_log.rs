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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::client::credentials::CredentialsReceiver;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::io::{FileIO, Storage};
use crate::metadata::TableBucket;
use crate::metrics::ScannerMetrics;
use crate::proto::{PbRemoteLogFetchInfo, PbRemoteLogSegment};
use futures::TryStreamExt;
use parking_lot::Mutex;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    future::Future,
    io, mem,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
    time::Duration,
};

#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;

/// Default maximum number of remote log segments to prefetch
/// Matches Java's CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM (default: 4)
pub const DEFAULT_SCANNER_REMOTE_LOG_PREFETCH_NUM: usize = 4;

/// Default maximum concurrent remote log downloads
/// Matches Java's REMOTE_FILE_DOWNLOAD_THREAD_NUM (default: 3)
pub const DEFAULT_REMOTE_FILE_DOWNLOAD_THREAD_NUM: usize = 3;

#[derive(Clone, Copy, Debug)]
struct RemoteRetryPolicy {
    max_retries: u32,
    backoff_base_ms: u64,
    backoff_max_ms: u64,
}

#[derive(Clone, Copy)]
struct DownloadOptions {
    max_prefetch_bytes: usize,
    retry_policy: RemoteRetryPolicy,
}

#[derive(Clone, Copy)]
struct StreamingReadOptions {
    chunk_size: usize,
    concurrency: usize,
    timeout: Duration,
}

impl From<&Config> for RemoteRetryPolicy {
    fn from(config: &Config) -> Self {
        Self {
            max_retries: config.scanner_remote_log_max_retries,
            backoff_base_ms: config.scanner_remote_log_retry_backoff_base_ms,
            backoff_max_ms: config.scanner_remote_log_retry_backoff_max_ms,
        }
    }
}

/// Calculate exponential backoff delay with jitter for retries
fn calculate_backoff_delay(retry_count: u32, policy: RemoteRetryPolicy) -> Duration {
    use rand::Rng;

    // First retry uses the configured base; later retries grow exponentially.
    let exponential_ms = policy
        .backoff_base_ms
        .saturating_mul(1_u64 << retry_count.saturating_sub(1).min(63));

    // Cap at maximum
    let capped_ms = exponential_ms.min(policy.backoff_max_ms);

    // Add jitter (±25% randomness) to avoid thundering herd
    let mut rng = rand::rng();
    let jitter = rng.random_range(0.75..=1.25);
    let final_ms = (((capped_ms as f64) * jitter) as u64)
        .max(1)
        .min(policy.backoff_max_ms);

    Duration::from_millis(final_ms)
}

fn remote_failure_can_retry(error: &Error) -> bool {
    match error {
        Error::RemoteStorageUnexpectedError { source, .. } => source.is_temporary(),
        Error::IoUnexpectedError { source, .. } => matches!(
            source.kind(),
            io::ErrorKind::TimedOut
                | io::ErrorKind::Interrupted
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionRefused
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::WouldBlock
        ),
        // UnexpectedError may wrap a transient error from a custom fetcher.
        Error::UnexpectedError { .. } => true,
        _ => false,
    }
}

fn remote_failure_for_scan(error: Error, segment_id: &str, attempts: u32) -> Error {
    let missing = matches!(
        &error,
        Error::RemoteStorageUnexpectedError { source, .. }
            if source.kind() == opendal::ErrorKind::NotFound
    ) || matches!(&error, Error::IoUnexpectedError { source, .. } if source.kind() == io::ErrorKind::NotFound);
    let message = if missing {
        format!(
            "Required remote log segment {segment_id} is missing; this scan cannot complete (the object may have expired or been removed). Replan to read the currently retained offsets. Download attempts: {attempts}"
        )
    } else {
        format!("Failed to download remote log segment after {attempts} attempt(s): {error}")
    };
    Error::UnexpectedError {
        message,
        source: Some(Box::new(error)),
    }
}

/// Result of a fetch operation containing file path and size
#[derive(Debug)]
pub struct FetchResult {
    pub file_path: PathBuf,
    pub file_size: usize,
}

/// Trait for fetching remote log segments (allows dependency injection for testing)
pub trait RemoteLogFetcher: Send + Sync {
    fn fetch(
        &self,
        request: &RemoteLogDownloadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<FetchResult>> + Send>>;

    /// Production fetchers reserve real bytes before writing each chunk.
    /// Test fetchers that only return a completed file may use the default;
    /// the coordinator still checks their final size before delivering it.
    fn fetch_bounded<'a>(
        &'a self,
        request: &'a RemoteLogDownloadRequest,
        _reserve: &'a mut (dyn FnMut(usize) -> Result<()> + Send),
    ) -> Pin<Box<dyn Future<Output = Result<FetchResult>> + Send + 'a>> {
        self.fetch(request)
    }
}

/// Represents a remote log segment that needs to be downloaded
#[derive(Debug, Clone)]
pub struct RemoteLogSegment {
    pub segment_id: String,
    pub start_offset: i64,
    #[allow(dead_code)]
    pub end_offset: i64,
    #[allow(dead_code)]
    pub size_in_bytes: i32,
    pub table_bucket: TableBucket,
    pub max_timestamp: i64,
}

impl RemoteLogSegment {
    pub fn from_proto(segment: &PbRemoteLogSegment, table_bucket: TableBucket) -> Self {
        Self {
            segment_id: segment.remote_log_segment_id.clone(),
            start_offset: segment.remote_log_start_offset,
            end_offset: segment.remote_log_end_offset,
            size_in_bytes: segment.segment_size_in_bytes,
            table_bucket,
            // Match Java's behavior: use -1 for missing timestamp
            // (Java: CommonRpcMessageUtils.java:171-174)
            max_timestamp: segment.max_timestamp.unwrap_or(-1),
        }
    }

    /// Get the local file name for this remote log segment
    pub fn local_file_name(&self) -> String {
        // Format: ${remote_segment_id}_${offset_prefix}.log
        let offset_prefix = format!("{:020}", self.start_offset);
        format!("{}_{}.log", self.segment_id, offset_prefix)
    }
}

/// Represents remote log fetch information
#[derive(Debug, Clone)]
pub struct RemoteLogFetchInfo {
    pub remote_log_tablet_dir: String,
    #[allow(dead_code)]
    pub partition_name: Option<String>,
    pub remote_log_segments: Vec<RemoteLogSegment>,
    pub first_start_pos: i32,
}

impl RemoteLogFetchInfo {
    pub fn from_proto(info: &PbRemoteLogFetchInfo, table_bucket: TableBucket) -> Self {
        let segments = info
            .remote_log_segments
            .iter()
            .map(|s| RemoteLogSegment::from_proto(s, table_bucket.clone()))
            .collect();

        Self {
            remote_log_tablet_dir: info.remote_log_tablet_dir.clone(),
            partition_name: info.partition_name.clone(),
            remote_log_segments: segments,
            first_start_pos: info.first_start_pos.unwrap_or(0),
        }
    }
}

/// RAII guard for prefetch permit that notifies coordinator on drop
///
/// NOTE: File deletion is now handled by FileSource::drop(), not here.
/// This ensures the file is closed before deletion
#[derive(Debug)]
pub struct PrefetchPermit {
    permit: Option<OwnedSemaphorePermit>,
    recycle_notify: Arc<Notify>,
}

impl PrefetchPermit {
    fn new(permit: OwnedSemaphorePermit, recycle_notify: Arc<Notify>) -> Self {
        Self {
            permit: Some(permit),
            recycle_notify,
        }
    }
}

impl Drop for PrefetchPermit {
    fn drop(&mut self) {
        // Release capacity (critical: permit must be dropped before notify)
        let _ = self.permit.take(); // drops permit here

        // Then wake coordinator so it can acquire the now-available permit
        self.recycle_notify.notify_one();
    }
}

/// Reserves remote-file disk bytes while a download or prefetched file is
/// alive. A cancelled scanner releases the reservation via ordinary Drop.
#[derive(Debug)]
pub(crate) struct PrefetchBytesPermit {
    reserved: Arc<AtomicUsize>,
    bytes: usize,
    recycle_notify: Arc<Notify>,
}

impl PrefetchBytesPermit {
    fn adjust_to(&mut self, actual: usize, limit: usize) -> bool {
        if actual > self.bytes {
            let extra = actual - self.bytes;
            if self
                .reserved
                .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |used| {
                    used.checked_add(extra).filter(|&next| next <= limit)
                })
                .is_err()
            {
                return false;
            }
        } else if actual < self.bytes {
            self.reserved
                .fetch_sub(self.bytes - actual, AtomicOrdering::AcqRel);
            self.recycle_notify.notify_one();
        }
        self.bytes = actual;
        true
    }
}

impl Drop for PrefetchBytesPermit {
    fn drop(&mut self) {
        self.reserved.fetch_sub(self.bytes, AtomicOrdering::AcqRel);
        self.recycle_notify.notify_one();
    }
}

/// Downloaded remote log file with prefetch permit
/// File remains on disk for memory efficiency; file deletion is handled by FileCleanupGuard in FileSource
#[derive(Debug)]
pub struct RemoteLogFile {
    /// Path to the downloaded file on local disk
    pub file_path: PathBuf,
    /// Size of the file in bytes
    /// Currently unused but kept for potential future use (logging, metrics, etc.)
    #[allow(dead_code)]
    pub file_size: usize,
    /// RAII permit that releases prefetch semaphore slot and notifies coordinator when dropped
    pub permit: PrefetchPermit,
    pub(crate) bytes_permit: PrefetchBytesPermit,
}

/// Represents a request to download a remote log segment with priority ordering
#[derive(Debug)]
pub struct RemoteLogDownloadRequest {
    segment: RemoteLogSegment,
    remote_log_tablet_dir: String,
    result_sender: oneshot::Sender<Result<RemoteLogFile>>,
    retry_count: u32,
    next_retry_at: Option<tokio::time::Instant>,
    cancel_notify: Arc<Notify>,
    /// Keep the queue slot until the coordinator has discarded or finished
    /// this request, even if the caller dropped its future earlier.
    _pending_slot: Option<PendingSlot>,
}

#[derive(Debug)]
struct PendingSlot(Arc<AtomicUsize>);

impl Drop for PendingSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, AtomicOrdering::AcqRel);
    }
}

impl RemoteLogDownloadRequest {
    /// Get the segment (used by test fetcher implementations)
    #[cfg(test)]
    pub fn segment(&self) -> &RemoteLogSegment {
        &self.segment
    }
}

// Total ordering for priority queue (Rust requirement: cmp==Equal implies Eq)
// Primary: Java semantics (timestamp cross-bucket, offset within-bucket)
// Tie-breakers: table_bucket fields (table_id, partition_id, bucket_id), then segment_id
impl Ord for RemoteLogDownloadRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.segment.table_bucket == other.segment.table_bucket {
            // Same bucket: order by start_offset (ascending - earlier segments first)
            self.segment
                .start_offset
                .cmp(&other.segment.start_offset)
                .then_with(|| self.segment.segment_id.cmp(&other.segment.segment_id))
        } else {
            // Different buckets: order by max_timestamp (ascending - older segments first)
            // Then by table_bucket fields for true total ordering
            self.segment
                .max_timestamp
                .cmp(&other.segment.max_timestamp)
                .then_with(|| {
                    self.segment
                        .table_bucket
                        .table_id()
                        .cmp(&other.segment.table_bucket.table_id())
                })
                .then_with(|| {
                    self.segment
                        .table_bucket
                        .partition_id()
                        .cmp(&other.segment.table_bucket.partition_id())
                })
                .then_with(|| {
                    self.segment
                        .table_bucket
                        .bucket_id()
                        .cmp(&other.segment.table_bucket.bucket_id())
                })
                .then_with(|| self.segment.segment_id.cmp(&other.segment.segment_id))
        }
    }
}

impl PartialOrd for RemoteLogDownloadRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RemoteLogDownloadRequest {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RemoteLogDownloadRequest {}

/// Result of a download task
enum DownloadResult {
    /// Successful download - deliver result to future
    Success {
        result: RemoteLogFile,
        result_sender: oneshot::Sender<Result<RemoteLogFile>>,
    },
    /// Download failed - re-queue request for retry (Java pattern)
    FailedRetry { request: RemoteLogDownloadRequest },
    /// Download failed permanently after max retries - fail the future
    FailedPermanently {
        error: Error,
        result_sender: oneshot::Sender<Result<RemoteLogFile>>,
    },
    /// Cancelled - don't deliver, don't re-queue
    Cancelled,
}

/// Production implementation of RemoteLogFetcher that downloads from actual storage
struct ProductionFetcher {
    credentials_rx: CredentialsReceiver,
    local_log_dir: Arc<TempDir>,
    remote_log_read_concurrency: usize,
}

impl ProductionFetcher {
    fn fetch_impl<'a>(
        &self,
        request: &RemoteLogDownloadRequest,
        budget: Option<&'a mut (dyn FnMut(usize) -> Result<()> + Send)>,
    ) -> Pin<Box<dyn Future<Output = Result<FetchResult>> + Send + 'a>> {
        let mut credentials_rx = self.credentials_rx.clone();
        let local_log_dir = self.local_log_dir.clone();
        let remote_log_read_concurrency = self.remote_log_read_concurrency;

        // Clone data needed for async operation to avoid lifetime issues
        let segment = request.segment.clone();
        let remote_log_tablet_dir = request.remote_log_tablet_dir.to_string();

        Box::pin(async move {
            let local_file_name = segment.local_file_name();
            let local_file_path = local_log_dir.path().join(&local_file_name);

            // Build remote path
            let offset_prefix = format!("{:020}", segment.start_offset);
            let remote_path = format!(
                "{}/{}/{}.log",
                remote_log_tablet_dir, segment.segment_id, offset_prefix
            );

            // Get credentials from watch channel, waiting if not yet fetched
            // - None = not yet fetched, wait
            // - Some(props) = fetched (may be empty if no auth needed)
            let remote_fs_props = {
                let maybe_props = credentials_rx.borrow().clone();
                match maybe_props {
                    Some(props) => props,
                    None => {
                        // Credentials not yet fetched, wait for first update
                        log::info!("Waiting for credentials to be available...");
                        // If the sender side has been dropped (e.g. during shutdown),
                        // this will return an error. Surface that as a proper error
                        // instead of silently falling back to empty credentials.
                        if let Err(e) = credentials_rx.changed().await {
                            let io_err = io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                format!(
                                    "credentials manager shut down before credentials were obtained: {e}"
                                ),
                            );
                            return Err(io_err.into());
                        }
                        // After a successful change notification, credentials should be set.
                        // If they are still missing, treat this as an error instead of
                        // defaulting to an empty map (which could break auth flows).
                        credentials_rx
                            .borrow()
                            .clone()
                            .ok_or_else(|| Error::UnexpectedError {
                                message: "credentials not available after watch notification"
                                    .to_string(),
                                source: None,
                            })?
                    }
                }
            };

            // Download file to disk (streaming, no memory spike)
            let file_path = RemoteLogDownloader::download_file(
                &remote_log_tablet_dir,
                &remote_path,
                &local_file_path,
                &remote_fs_props,
                remote_log_read_concurrency,
                budget,
            )
            .await?;

            // Get file size
            let metadata = tokio::fs::metadata(&file_path).await?;
            let file_size = metadata.len() as usize;

            // Return file path - file stays on disk until PrefetchPermit is dropped
            Ok(FetchResult {
                file_path,
                file_size,
            })
        })
    }
}

impl RemoteLogFetcher for ProductionFetcher {
    fn fetch(
        &self,
        request: &RemoteLogDownloadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<FetchResult>> + Send>> {
        self.fetch_impl(request, None)
    }

    fn fetch_bounded<'a>(
        &'a self,
        request: &'a RemoteLogDownloadRequest,
        budget: &'a mut (dyn FnMut(usize) -> Result<()> + Send),
    ) -> Pin<Box<dyn Future<Output = Result<FetchResult>> + Send + 'a>> {
        self.fetch_impl(request, Some(budget))
    }
}

/// Coordinator that owns all download state and orchestrates downloads
struct DownloadCoordinator {
    download_queue: BinaryHeap<Reverse<RemoteLogDownloadRequest>>,
    active_downloads: JoinSet<DownloadResult>,
    in_flight: usize,
    prefetch_semaphore: Arc<Semaphore>,
    prefetch_bytes: Arc<AtomicUsize>,
    max_prefetch_bytes: usize,
    max_concurrent_downloads: usize,
    retry_policy: RemoteRetryPolicy,
    recycle_notify: Arc<Notify>,
    fetcher: Arc<dyn RemoteLogFetcher>,
    /// Per-table scanner metric handles cloned by every spawned download
    /// task to attribute remote-fetch metrics to the owning scanner's
    /// `(database, table)`.
    metrics: Arc<ScannerMetrics>,
}

impl DownloadCoordinator {
    /// Check if we should wait for recycle notification
    /// Only wait if we're blocked on permits AND have pending work
    fn should_wait_for_recycle(&self) -> bool {
        !self.download_queue.is_empty() && self.in_flight < self.max_concurrent_downloads
    }

    fn reserve_bytes(&self, size: usize) -> Option<PrefetchBytesPermit> {
        self.prefetch_bytes
            .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |used| {
                used.checked_add(size)
                    .filter(|&next| next <= self.max_prefetch_bytes)
            })
            .ok()?;
        Some(PrefetchBytesPermit {
            reserved: Arc::clone(&self.prefetch_bytes),
            bytes: size,
            recycle_notify: Arc::clone(&self.recycle_notify),
        })
    }

    /// Find the earliest retry deadline among pending requests
    fn next_retry_deadline(&self) -> Option<tokio::time::Instant> {
        self.download_queue
            .iter()
            .filter_map(|Reverse(req)| req.next_retry_at)
            .min()
    }
}

impl DownloadCoordinator {
    /// Try to start as many downloads as possible (event-driven drain)
    fn drain(&mut self) {
        // Collect deferred requests (backoff not ready) to push back later
        let mut deferred = Vec::new();
        // Scan entire queue once to find ready requests (prevents head-of-line blocking)
        // Bound to reasonable max to avoid excessive work if queue is huge
        let max_scan = self.download_queue.len().min(100);
        let mut scanned = 0;

        while !self.download_queue.is_empty()
            && self.in_flight < self.max_concurrent_downloads
            && scanned < max_scan
        {
            // Try acquire prefetch permit (non-blocking)
            let permit = match self.prefetch_semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => break, // No permits available
            };

            // Pop highest priority request
            let Some(Reverse(request)) = self.download_queue.pop() else {
                drop(permit);
                break;
            };

            scanned += 1;

            // A closed receiver must be discarded even during a long retry
            // backoff, otherwise cancellation keeps its queue slot reserved.
            if request.result_sender.is_closed() {
                drop(permit);
                continue;
            }

            // Retry backoff check: defer if retry time hasn't arrived yet
            if let Some(next_retry_at) = request.next_retry_at {
                let now = tokio::time::Instant::now();
                if next_retry_at > now {
                    // Not ready for retry yet - defer and continue looking for ready requests
                    drop(permit);
                    deferred.push(request);
                    continue; // Don't block - keep looking for ready requests
                }
            }

            let bytes = request.segment.size_in_bytes.max(1) as usize;
            if bytes > self.max_prefetch_bytes {
                drop(permit);
                let _ = request.result_sender.send(Err(Error::BufferExhausted {
                    message: format!(
                        "Remote segment requires {bytes} bytes, above the {}-byte prefetch limit",
                        self.max_prefetch_bytes
                    ),
                }));
                continue;
            }
            let Some(bytes_permit) = self.reserve_bytes(bytes) else {
                drop(permit);
                deferred.push(request);
                continue;
            };

            // Clone data for the spawned task
            let fetcher = self.fetcher.clone();
            let recycle_notify = self.recycle_notify.clone();
            let metrics = Arc::clone(&self.metrics);
            let options = DownloadOptions {
                max_prefetch_bytes: self.max_prefetch_bytes,
                retry_policy: self.retry_policy,
            };

            // Spawn download task
            self.active_downloads.spawn(async move {
                spawn_download_task(
                    request,
                    permit,
                    bytes_permit,
                    options,
                    fetcher,
                    recycle_notify,
                    metrics,
                )
                .await
            });
            self.in_flight += 1;
        }

        // Push deferred requests back to queue (maintains priority order)
        if !deferred.is_empty() {
            for req in deferred {
                self.download_queue.push(Reverse(req));
            }
        }
    }
}

/// Spawn a download task that attempts download once
/// Matches Java's RemoteLogDownloader.java
///
/// Benefits over infinite in-place retry:
/// - Failed downloads don't block prefetch slots
/// - Other segments can make progress while one is failing
/// - Natural retry through coordinator re-picking from queue
async fn spawn_download_task(
    request: RemoteLogDownloadRequest,
    permit: tokio::sync::OwnedSemaphorePermit,
    mut bytes_permit: PrefetchBytesPermit,
    options: DownloadOptions,
    fetcher: Arc<dyn RemoteLogFetcher>,
    recycle_notify: Arc<Notify>,
    metrics: Arc<ScannerMetrics>,
) -> DownloadResult {
    // Check if receiver still alive (early cancellation check)
    if request.result_sender.is_closed() {
        drop(permit);
        return DownloadResult::Cancelled;
    }

    // Java reference: RemoteLogDownloader.java increments `remoteFetchRequestCount`
    // immediately before initiating the download. Each retry of the same segment
    // counts as a separate request (matches Java behavior).
    metrics.record_remote_fetch_request();

    // Try download ONCE
    // Cancellation closes the one-shot receiver. Drop the underlying download
    // future immediately instead of waiting for a blocked object store to
    // respond while it still owns a prefetch slot and the scanner's temp dir.
    let download_result = {
        let mut reserve_written_bytes = |actual| {
            if bytes_permit.adjust_to(actual, options.max_prefetch_bytes) {
                Ok(())
            } else {
                Err(Error::BufferExhausted {
                    message: format!(
                        "Remote segment needs {actual} bytes while downloading, above the {}-byte prefetch budget",
                        options.max_prefetch_bytes
                    ),
                })
            }
        };
        tokio::select! {
            result = fetcher.fetch_bounded(&request, &mut reserve_written_bytes) => result,
            _ = request.cancel_notify.notified() => {
                drop(permit);
                return DownloadResult::Cancelled;
            }
        }
    };

    match download_result {
        Ok(fetch_result) => {
            if !bytes_permit.adjust_to(fetch_result.file_size, options.max_prefetch_bytes) {
                let _ = tokio::fs::remove_file(&fetch_result.file_path).await;
                return DownloadResult::FailedPermanently {
                    error: Error::BufferExhausted {
                        message: format!(
                            "Downloaded remote segment requires {} bytes, above the prefetch budget",
                            fetch_result.file_size
                        ),
                    },
                    result_sender: request.result_sender,
                };
            }
            // Success - permit will be released on drop (FileSource handles file deletion)
            metrics.record_remote_fetch_bytes(fetch_result.file_size as u64);
            DownloadResult::Success {
                result: RemoteLogFile {
                    file_path: fetch_result.file_path,
                    file_size: fetch_result.file_size,
                    permit: PrefetchPermit::new(permit, recycle_notify.clone()),
                    bytes_permit,
                },
                result_sender: request.result_sender,
            }
        }
        Err(_e) if request.result_sender.is_closed() => {
            // Receiver dropped (cancelled) - release permit, don't re-queue
            drop(permit);
            DownloadResult::Cancelled
        }
        Err(e) => {
            // Download failed - check if we should retry or give up
            // Counted per attempt, so retries each contribute one error.
            metrics.record_remote_fetch_error();
            let attempts = request.retry_count.saturating_add(1);

            if request.retry_count >= options.retry_policy.max_retries
                || !remote_failure_can_retry(&e)
            {
                log::error!(
                    "Failed to download remote log segment {} after {} attempt(s): {}. Giving up.",
                    request.segment.segment_id,
                    attempts,
                    e
                );
                drop(permit); // Release immediately

                DownloadResult::FailedPermanently {
                    error: remote_failure_for_scan(e, &request.segment.segment_id, attempts),
                    result_sender: request.result_sender,
                }
            } else {
                // Retry with exponential backoff
                let retry_count = attempts;
                let backoff_delay = calculate_backoff_delay(retry_count, options.retry_policy);
                let next_retry_at = tokio::time::Instant::now() + backoff_delay;

                log::warn!(
                    "Failed to download remote log segment {}: {}. Retry {}/{} after {:?}",
                    request.segment.segment_id,
                    e,
                    retry_count,
                    options.retry_policy.max_retries,
                    backoff_delay
                );
                drop(permit); // Release immediately - critical!

                // Update retry state
                let mut retry_request = request;
                retry_request.retry_count = retry_count;
                retry_request.next_retry_at = Some(next_retry_at);

                // Re-queue request to same priority queue
                // Future stays with request, NOT completed - will complete on successful retry
                DownloadResult::FailedRetry {
                    request: retry_request,
                }
            }
        }
    }
}

/// Coordinator event loop - owns all download state and reacts to events
async fn coordinator_loop(
    mut coordinator: DownloadCoordinator,
    mut request_receiver: mpsc::UnboundedReceiver<RemoteLogDownloadRequest>,
) {
    loop {
        // Drain once at start of iteration to process ready work
        coordinator.drain();

        // Calculate sleep duration until next retry (if any deferred requests)
        let next_retry_sleep = coordinator.next_retry_deadline().map(|deadline| {
            let now = tokio::time::Instant::now();
            if deadline > now {
                deadline - now
            } else {
                tokio::time::Duration::from_millis(0) // Ready now
            }
        });

        tokio::select! {
            // Event 1: NewRequest
            Some(request) = request_receiver.recv() => {
                coordinator.download_queue.push(Reverse(request));
                // Immediately try to start this download
                continue;
            }

            // Event 2: DownloadFinished
            Some(result) = coordinator.active_downloads.join_next() => {
                coordinator.in_flight -= 1;

                match result {
                    Ok(DownloadResult::Success { result, result_sender }) => {
                        // Success - deliver result to future
                        if !result_sender.is_closed() {
                            let _ = result_sender.send(Ok(result));
                        }
                        // Permit held in RemoteLogFile until consumed
                    }
                    Ok(DownloadResult::FailedRetry { request }) => {
                        // Re-queue immediately (don't block coordinator with sleep)
                        // The retry time will be checked in drain() before processing
                        // (Java line 177: segmentsToFetch.add(request))
                        // Permit already released (Java line 174)
                        coordinator.download_queue.push(Reverse(request));
                    }
                    Ok(DownloadResult::FailedPermanently { error, result_sender }) => {
                        // Permanent failure - deliver error to future
                        if !result_sender.is_closed() {
                            let _ = result_sender.send(Err(error));
                        }
                        // Permit already released
                    }
                    Ok(DownloadResult::Cancelled) => {
                        // Cancelled - permit already released, nothing to do
                    }
                    Err(e) => {
                        log::error!("Download task panicked: {e:?}");
                        // Permit already released via RAII
                    }
                }
                // Immediately try to start another download
                continue;
            }

            // Event 3: Recycled (only wait when blocked on permits with pending work)
            _ = coordinator.recycle_notify.notified(),
                if coordinator.should_wait_for_recycle() => {
                // Wake up to try draining
                continue;
            }

            // Event 4: Retry timer - wake up when next retry is ready
            _ = tokio::time::sleep(next_retry_sleep.unwrap_or(tokio::time::Duration::from_secs(3600))),
                if next_retry_sleep.is_some() => {
                // Wake up to retry deferred requests
                continue;
            }

            else => break,  // All channels closed AND no work pending
        }
    }
}

type CompletionCallback = Box<dyn Fn() + Send + Sync>;

/// Future for a remote log download request
pub struct RemoteLogDownloadFuture {
    result: Arc<Mutex<Option<Result<RemoteLogFile>>>>,
    completion_callbacks: Arc<Mutex<Vec<CompletionCallback>>>,
    worker: tokio::task::AbortHandle,
    recycle_notify: Option<Arc<Notify>>,
    cancel_notify: Option<Arc<Notify>>,
}

impl RemoteLogDownloadFuture {
    pub fn new(receiver: oneshot::Receiver<Result<RemoteLogFile>>) -> Self {
        let result = Arc::new(Mutex::new(None));
        let result_clone = Arc::clone(&result);
        let completion_callbacks: Arc<Mutex<Vec<CompletionCallback>>> =
            Arc::new(Mutex::new(Vec::new()));
        let callbacks_clone = Arc::clone(&completion_callbacks);

        // Spawn a task to wait for the download and update result, then call callbacks
        let worker = tokio::spawn(async move {
            let download_result = match receiver.await {
                Ok(Ok(path)) => Ok(path),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(Error::UnexpectedError {
                    message: format!("Download & Read future cancelled: {e:?}"),
                    source: None,
                }),
            };

            *result_clone.lock() = Some(download_result);

            // Call all registered callbacks
            // We need to take the callbacks to avoid holding the lock while calling them
            // This also ensures that any callbacks registered after this point will be called immediately
            let callbacks: Vec<CompletionCallback> = {
                let mut callbacks_guard = callbacks_clone.lock();
                mem::take(&mut *callbacks_guard)
            };
            for callback in callbacks {
                callback();
            }

            // After calling callbacks, any new callbacks registered will see is_done() == true
            // and will be called immediately in on_complete()
        });

        Self {
            result,
            completion_callbacks,
            worker: worker.abort_handle(),
            recycle_notify: None,
            cancel_notify: None,
        }
    }

    fn with_recycle_notify(mut self, recycle_notify: Arc<Notify>) -> Self {
        self.recycle_notify = Some(recycle_notify);
        self
    }

    fn with_cancel_notify(mut self, cancel_notify: Arc<Notify>) -> Self {
        self.cancel_notify = Some(cancel_notify);
        self
    }

    /// Register a callback to be called when download completes (similar to Java's onComplete)
    pub fn on_complete<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        // Acquire callbacks lock first to ensure atomicity of the check-and-register operation
        let mut callbacks_guard = self.completion_callbacks.lock();

        // Check completion status while holding the callbacks lock.
        // This ensures that:
        // 1. If the task completes between checking is_done() and registering the callback,
        //    we'll see the completion state correctly
        // 2. The background task cannot clear the callbacks list while we're checking/registering
        let is_done = self.result.lock().is_some();

        if is_done {
            // If already completed, call immediately (drop lock first to avoid deadlock)
            drop(callbacks_guard);
            callback();
        } else {
            // Register the callback while holding the callbacks lock.
            // This ensures that even if the background task completes right after we check
            // is_done(), it will wait for us to release the lock before taking callbacks.
            // When it does take callbacks, it will see our callback in the list and execute it.
            callbacks_guard.push(Box::new(callback));
            // Lock is automatically released here
        }
    }

    pub fn is_done(&self) -> bool {
        self.result.lock().is_some()
    }

    /// Take the RemoteLogFile (including the permit) from this future
    /// This should only be called when the download is complete
    /// This is the correct way to consume the download - it transfers permit ownership
    pub fn take_remote_log_file(&self) -> Result<RemoteLogFile> {
        let mut guard = self.result.lock();
        match guard.take() {
            Some(Ok(remote_log_file)) => Ok(remote_log_file),
            Some(Err(e)) => Err(e),
            None => Err(Error::IoUnexpectedError {
                message: "Remote log file already taken or not ready".to_string(),
                source: io::Error::other("Remote log file already taken or not ready"),
            }),
        }
    }
}

impl Drop for RemoteLogDownloadFuture {
    fn drop(&mut self) {
        // A cancelled scanner must close the one-shot receiver. The download
        // coordinator can then skip queued segments and release permits/files.
        self.worker.abort();
        if let Some(notify) = &self.cancel_notify {
            notify.notify_one();
        }
        // A cancelled request may be parked in backoff for several seconds.
        // Wake the coordinator to discard it and release its pending slot now.
        if let Some(notify) = &self.recycle_notify {
            notify.notify_one();
        }
    }
}

/// Downloader for remote log segment files.
///
/// # Shutdown behavior
///
/// When the downloader is dropped, the request channel closes, signaling the coordinator
/// to stop accepting new work. The coordinator will finish any in-flight downloads but
/// won't wait for completion. Pending futures will fail.
pub struct RemoteLogDownloader {
    request_sender: Option<mpsc::UnboundedSender<RemoteLogDownloadRequest>>,
    recycle_notify: Arc<Notify>,
    pending_slots: Arc<AtomicUsize>,
    max_pending_segments: usize,
    #[cfg(test)]
    prefetch_bytes: Arc<AtomicUsize>,
}

pub(crate) struct RemoteDownloadLimits {
    prefetch_segments: usize,
    pending_segments: usize,
    prefetch_bytes: usize,
    concurrent_downloads: usize,
    read_concurrency: usize,
    retry_policy: RemoteRetryPolicy,
}

impl RemoteDownloadLimits {
    pub(crate) fn from_config(config: &Config) -> Self {
        Self {
            prefetch_segments: config.scanner_remote_log_prefetch_num,
            pending_segments: config.scanner_remote_log_max_pending_segments,
            prefetch_bytes: config.scanner_remote_log_max_prefetch_bytes,
            concurrent_downloads: config.remote_file_download_thread_num,
            read_concurrency: config.scanner_remote_log_read_concurrency,
            retry_policy: RemoteRetryPolicy::from(config),
        }
    }
}

impl RemoteLogDownloader {
    pub(crate) fn new(
        local_log_dir: TempDir,
        limits: RemoteDownloadLimits,
        credentials_rx: CredentialsReceiver,
        metrics: Arc<ScannerMetrics>,
    ) -> Result<Self> {
        let fetcher = Arc::new(ProductionFetcher {
            credentials_rx,
            local_log_dir: Arc::new(local_log_dir),
            remote_log_read_concurrency: limits.read_concurrency,
        });

        Self::new_with_fetcher_and_retry(
            fetcher,
            limits.prefetch_segments,
            limits.pending_segments,
            limits.prefetch_bytes,
            limits.concurrent_downloads,
            limits.retry_policy,
            metrics,
        )
    }

    /// Create a RemoteLogDownloader with a custom fetcher (for testing).
    #[cfg(test)]
    pub(crate) fn new_with_fetcher(
        fetcher: Arc<dyn RemoteLogFetcher>,
        max_prefetch_segments: usize,
        max_concurrent_downloads: usize,
        metrics: Arc<ScannerMetrics>,
    ) -> Result<Self> {
        Self::new_with_fetcher_and_limit(
            fetcher,
            max_prefetch_segments,
            8_192,
            64 * 1024 * 1024,
            max_concurrent_downloads,
            metrics,
        )
    }

    #[cfg(test)]
    fn new_with_fetcher_and_limit(
        fetcher: Arc<dyn RemoteLogFetcher>,
        max_prefetch_segments: usize,
        max_pending_segments: usize,
        max_prefetch_bytes: usize,
        max_concurrent_downloads: usize,
        metrics: Arc<ScannerMetrics>,
    ) -> Result<Self> {
        Self::new_with_fetcher_and_retry(
            fetcher,
            max_prefetch_segments,
            max_pending_segments,
            max_prefetch_bytes,
            max_concurrent_downloads,
            RemoteRetryPolicy::from(&Config::default()),
            metrics,
        )
    }

    fn new_with_fetcher_and_retry(
        fetcher: Arc<dyn RemoteLogFetcher>,
        max_prefetch_segments: usize,
        max_pending_segments: usize,
        max_prefetch_bytes: usize,
        max_concurrent_downloads: usize,
        retry_policy: RemoteRetryPolicy,
        metrics: Arc<ScannerMetrics>,
    ) -> Result<Self> {
        let (request_sender, request_receiver) = mpsc::unbounded_channel();

        let prefetch_bytes = Arc::new(AtomicUsize::new(0));
        let recycle_notify = Arc::new(Notify::new());
        let coordinator = DownloadCoordinator {
            download_queue: BinaryHeap::new(),
            active_downloads: JoinSet::new(),
            in_flight: 0,
            prefetch_semaphore: Arc::new(Semaphore::new(max_prefetch_segments)),
            prefetch_bytes: Arc::clone(&prefetch_bytes),
            max_prefetch_bytes,
            max_concurrent_downloads,
            retry_policy,
            recycle_notify: Arc::clone(&recycle_notify),
            fetcher,
            metrics,
        };

        // Spawn coordinator task - it will exit when request_sender is dropped
        tokio::spawn(coordinator_loop(coordinator, request_receiver));

        Ok(Self {
            request_sender: Some(request_sender),
            recycle_notify,
            pending_slots: Arc::new(AtomicUsize::new(0)),
            max_pending_segments,
            #[cfg(test)]
            prefetch_bytes,
        })
    }

    /// Request to fetch a remote log segment to local. This method is non-blocking.
    pub fn request_remote_log(
        &self,
        remote_log_tablet_dir: &str,
        segment: &RemoteLogSegment,
    ) -> RemoteLogDownloadFuture {
        let (result_sender, result_receiver) = oneshot::channel();

        if self
            .pending_slots
            .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |pending| {
                (pending < self.max_pending_segments).then_some(pending + 1)
            })
            .is_err()
        {
            let _ = result_sender.send(Err(Error::BufferExhausted {
                message: format!(
                    "Remote log scanner has {} pending segments (configured limit)",
                    self.max_pending_segments
                ),
            }));
            return RemoteLogDownloadFuture::new(result_receiver);
        }

        let cancel_notify = Arc::new(Notify::new());
        let request = RemoteLogDownloadRequest {
            segment: segment.clone(),
            remote_log_tablet_dir: remote_log_tablet_dir.to_string(),
            result_sender,
            retry_count: 0,
            next_retry_at: None,
            cancel_notify: Arc::clone(&cancel_notify),
            _pending_slot: Some(PendingSlot(Arc::clone(&self.pending_slots))),
        };

        // Send to coordinator (non-blocking)
        if let Some(ref sender) = self.request_sender {
            if sender.send(request).is_err() {
                // The failed send returns and drops the request and its slot.
                // Coordinator is gone - immediately fail the future
                let (error_sender, error_receiver) = oneshot::channel();
                let _ = error_sender.send(Err(Error::UnexpectedError {
                    message: "RemoteLogDownloader coordinator has shut down".to_string(),
                    source: None,
                }));
                return RemoteLogDownloadFuture::new(error_receiver);
            }
        }

        RemoteLogDownloadFuture::new(result_receiver)
            .with_recycle_notify(Arc::clone(&self.recycle_notify))
            .with_cancel_notify(cancel_notify)
    }
}

impl Drop for RemoteLogDownloader {
    fn drop(&mut self) {
        // Drop the request sender to signal coordinator shutdown.
        // This causes request_receiver.recv() to return None, allowing the
        // coordinator to exit gracefully after processing pending work.
        // The coordinator task will finish on its own when it sees the channel closed.
        drop(self.request_sender.take());
    }
}

/// A partial download must not remain on disk after a failed or cancelled
/// attempt. On success the file passes to FileSource, which owns its cleanup.
struct IncompleteDownload {
    path: PathBuf,
    finished: bool,
}

impl Drop for IncompleteDownload {
    fn drop(&mut self) {
        if !self.finished {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl RemoteLogDownloader {
    /// Download a file from remote storage to local using streaming read/write.
    async fn download_file(
        remote_log_tablet_dir: &str,
        remote_path: &str,
        local_path: &Path,
        remote_fs_props: &HashMap<String, String>,
        remote_log_read_concurrency: usize,
        budget: Option<&mut (dyn FnMut(usize) -> Result<()> + Send)>,
    ) -> Result<PathBuf> {
        // Handle both URL (e.g., "s3://bucket/path") and local file paths
        // If the path doesn't contain "://", treat it as a local file path
        let remote_log_tablet_dir_url = if remote_log_tablet_dir.contains("://") {
            remote_log_tablet_dir.to_string()
        } else {
            format!("file://{remote_log_tablet_dir}")
        };

        // Create FileIO from the remote log tablet dir URL to get the storage
        let file_io_builder = FileIO::from_url(&remote_log_tablet_dir_url)?;

        // For S3/S3A URLs, inject S3 credentials from props
        let file_io_builder = if remote_log_tablet_dir.starts_with("s3://")
            || remote_log_tablet_dir.starts_with("s3a://")
            || remote_log_tablet_dir.starts_with("oss://")
        {
            file_io_builder.with_props(
                remote_fs_props
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str())),
            )
        } else {
            file_io_builder
        };

        // Build storage and create operator directly
        let storage = Storage::build(file_io_builder)?;
        let (op, relative_path) = storage.create(remote_path)?;

        // Timeout for remote storage operations (30 seconds)
        const REMOTE_OP_TIMEOUT: Duration = Duration::from_secs(30);
        const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8MiB

        Self::download_file_streaming(
            &op,
            relative_path,
            remote_path,
            local_path,
            StreamingReadOptions {
                chunk_size: CHUNK_SIZE,
                concurrency: remote_log_read_concurrency,
                timeout: REMOTE_OP_TIMEOUT,
            },
            budget,
        )
        .await?;

        Ok(local_path.to_path_buf())
    }

    async fn download_file_streaming(
        op: &opendal::Operator,
        relative_path: &str,
        remote_path: &str,
        local_path: &Path,
        options: StreamingReadOptions,
        mut budget: Option<&mut (dyn FnMut(usize) -> Result<()> + Send)>,
    ) -> Result<()> {
        let mut cleanup = IncompleteDownload {
            path: local_path.to_path_buf(),
            finished: false,
        };
        let mut local_file = tokio::fs::File::create(local_path).await?;

        let reader_future = op
            .reader_with(relative_path)
            .chunk(options.chunk_size)
            .concurrent(options.concurrency);
        let reader = tokio::time::timeout(options.timeout, reader_future)
            .await
            .map_err(|e| Error::IoUnexpectedError {
                message: format!("Timeout creating streaming reader for {remote_path}: {e}."),
                source: io::ErrorKind::TimedOut.into(),
            })??;

        let mut stream = tokio::time::timeout(options.timeout, reader.into_bytes_stream(..))
            .await
            .map_err(|e| Error::IoUnexpectedError {
                message: format!("Timeout creating streaming bytes stream for {remote_path}: {e}."),
                source: io::ErrorKind::TimedOut.into(),
            })??;

        let mut chunk_count = 0u64;
        let mut written = 0usize;
        while let Some(chunk) = tokio::time::timeout(options.timeout, stream.try_next())
            .await
            .map_err(|e| Error::IoUnexpectedError {
                message: format!(
                    "Timeout streaming chunk from remote storage: {remote_path}, exception: {e}."
                ),
                source: io::ErrorKind::TimedOut.into(),
            })??
        {
            chunk_count += 1;
            if chunk_count <= 3 || chunk_count % 10 == 0 {
                log::debug!("Remote log streaming download: chunk #{chunk_count} ({remote_path})");
            }
            let next = written
                .checked_add(chunk.len())
                .ok_or(Error::BufferExhausted {
                    message: "Remote segment byte count overflowed during download".into(),
                })?;
            // Charge this chunk before touching disk. The metadata size is
            // only a hint, and concurrent downloads share the same budget.
            if let Some(reserve) = budget.as_deref_mut() {
                reserve(next)?;
            }
            local_file.write_all(&chunk).await?;
            written = next;
        }

        local_file.sync_all().await?;
        cleanup.finished = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::TablePath;
    use crate::test_utils::test_scanner_metrics;
    use crate::{BucketId, TableId};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn create_table_bucket(table_id: TableId, bucket_id: BucketId) -> TableBucket {
        TableBucket::new(table_id, bucket_id)
    }

    /// `ScannerMetrics` instance shared across the local test fixtures. The
    /// labels are arbitrary because none of the tests in this module install
    /// a metrics recorder; the metrics just need to exist for the API
    /// surface.
    fn metrics() -> Arc<ScannerMetrics> {
        test_scanner_metrics(&TablePath::new("db", "tbl"))
    }

    /// Simplified fake fetcher for testing
    struct FakeFetcher {
        directory: Arc<TempDir>,
        completion_gate: Arc<Notify>,
        in_flight: Arc<AtomicUsize>,
        max_seen_in_flight: Arc<AtomicUsize>,
        fail_count: Arc<Mutex<usize>>,
        auto_complete: bool,
    }

    struct InFlightGuard(Arc<AtomicUsize>);

    impl Drop for InFlightGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl FakeFetcher {
        fn new(fail_count: usize, auto_complete: bool) -> Self {
            Self {
                directory: Arc::new(TempDir::with_prefix("fluss-test-remote-").unwrap()),
                completion_gate: Arc::new(Notify::new()),
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_seen_in_flight: Arc::new(AtomicUsize::new(0)),
                fail_count: Arc::new(Mutex::new(fail_count)),
                auto_complete,
            }
        }

        fn max_seen_in_flight(&self) -> usize {
            self.max_seen_in_flight.load(Ordering::SeqCst)
        }

        fn in_flight(&self) -> usize {
            self.in_flight.load(Ordering::SeqCst)
        }

        fn release_one(&self) {
            self.completion_gate.notify_one();
        }

        fn release_all(&self) {
            self.completion_gate.notify_waiters();
        }
    }

    impl RemoteLogFetcher for FakeFetcher {
        fn fetch(
            &self,
            request: &RemoteLogDownloadRequest,
        ) -> Pin<Box<dyn Future<Output = Result<FetchResult>> + Send>> {
            let gate = self.completion_gate.clone();
            let in_flight = self.in_flight.clone();
            let max_seen = self.max_seen_in_flight.clone();
            let fail_count = self.fail_count.clone();
            let segment_id = request.segment().segment_id.clone();
            let auto_complete = self.auto_complete;
            let directory = Arc::clone(&self.directory);

            Box::pin(async move {
                // Track in-flight
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                let _in_flight = InFlightGuard(Arc::clone(&in_flight));
                max_seen.fetch_max(current, Ordering::SeqCst);

                // Wait for gate (or auto-complete)
                if !auto_complete {
                    gate.notified().await;
                } else {
                    tokio::task::yield_now().await;
                }

                // Check if should fail
                let should_fail = {
                    let mut count = fail_count.lock();
                    if *count > 0 {
                        *count -= 1;
                        true
                    } else {
                        false
                    }
                };

                if should_fail {
                    Err(Error::UnexpectedError {
                        message: format!("Fake fetch failed for {segment_id}"),
                        source: None,
                    })
                } else {
                    let fake_data = vec![1, 2, 3, 4];
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos();
                    let file_path = directory
                        .path()
                        .join(format!("{segment_id}_{timestamp}.log"));
                    tokio::fs::write(&file_path, &fake_data).await?;

                    Ok(FetchResult {
                        file_path,
                        file_size: fake_data.len(),
                    })
                }
            })
        }
    }

    struct StorageFailureFetcher {
        kind: opendal::ErrorKind,
        temporary: bool,
        attempts: Arc<AtomicUsize>,
    }

    impl RemoteLogFetcher for StorageFailureFetcher {
        fn fetch(
            &self,
            _request: &RemoteLogDownloadRequest,
        ) -> Pin<Box<dyn Future<Output = Result<FetchResult>> + Send>> {
            let kind = self.kind;
            let temporary = self.temporary;
            let attempts = Arc::clone(&self.attempts);
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(opendal::Error::new(kind, "remote stat failed")
                    .with_temporary(temporary)
                    .into())
            })
        }
    }

    async fn check_storage_retry_policy(
        kind: opendal::ErrorKind,
        temporary: bool,
        policy: RemoteRetryPolicy,
        expected_attempts: usize,
    ) -> Error {
        let attempts = Arc::new(AtomicUsize::new(0));
        let fetcher = Arc::new(StorageFailureFetcher {
            kind,
            temporary,
            attempts: Arc::clone(&attempts),
        });
        let downloader = RemoteLogDownloader::new_with_fetcher_and_retry(
            fetcher,
            1,
            2,
            64 * 1024,
            1,
            policy,
            metrics(),
        )
        .unwrap();
        let segment = create_segment("lost", 0, 0, create_table_bucket(1, 0));
        let future = downloader.request_remote_log("dir", &segment);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !future.is_done() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let error = future.take_remote_log_file().unwrap_err();
        assert_eq!(attempts.load(Ordering::SeqCst), expected_attempts);
        error
    }

    #[tokio::test]
    async fn remote_retries_are_configurable_and_count_attempts_correctly() {
        let config = Config {
            scanner_remote_log_max_retries: 2,
            scanner_remote_log_retry_backoff_base_ms: 1,
            scanner_remote_log_retry_backoff_max_ms: 1,
            ..Config::default()
        };
        let error = check_storage_retry_policy(
            opendal::ErrorKind::Unexpected,
            true,
            RemoteRetryPolicy::from(&config),
            3,
        )
        .await;
        assert!(error.to_string().contains("after 3 attempt(s)"));

        let no_retries = Config {
            scanner_remote_log_max_retries: 0,
            ..config
        };
        let error = check_storage_retry_policy(
            opendal::ErrorKind::Unexpected,
            true,
            RemoteRetryPolicy::from(&no_retries),
            1,
        )
        .await;
        assert!(error.to_string().contains("after 1 attempt(s)"));
    }

    #[tokio::test]
    async fn permanent_remote_errors_do_not_retry_and_missing_segment_retains_cause() {
        let policy = RemoteRetryPolicy::from(&Config::default());
        let error =
            check_storage_retry_policy(opendal::ErrorKind::NotFound, false, policy, 1).await;
        match error {
            Error::UnexpectedError { message, source } => {
                assert!(message.contains("scan cannot complete"));
                assert!(message.contains("may have expired or been removed"));
                assert!(source.is_some());
            }
            other => panic!("expected missing segment error, got {other}"),
        }
        let error =
            check_storage_retry_policy(opendal::ErrorKind::PermissionDenied, false, policy, 1)
                .await;
        assert!(error.to_string().contains("after 1 attempt(s)"));
    }

    #[tokio::test]
    async fn cancelling_during_long_retry_backoff_releases_pending_slot_promptly() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let downloader = RemoteLogDownloader::new_with_fetcher_and_retry(
            Arc::new(StorageFailureFetcher {
                kind: opendal::ErrorKind::Unexpected,
                temporary: true,
                attempts: Arc::clone(&attempts),
            }),
            1,
            1,
            64 * 1024,
            1,
            RemoteRetryPolicy {
                max_retries: 2,
                backoff_base_ms: 10_000,
                backoff_max_ms: 10_000,
            },
            metrics(),
        )
        .unwrap();
        let segment = create_segment("retry", 0, 0, create_table_bucket(1, 0));
        let future = downloader.request_remote_log("dir", &segment);
        tokio::time::timeout(Duration::from_secs(1), async {
            while attempts.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), async {
                while !future.is_done() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "the retry should be waiting in backoff"
        );
        drop(future);
        tokio::time::timeout(Duration::from_secs(1), async {
            while downloader.pending_slots.load(AtomicOrdering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled backoff should not hold a slot for ten seconds");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    /// Helper function to create a RemoteLogSegment for testing
    fn create_segment(
        segment_id: &str,
        start_offset: i64,
        max_timestamp: i64,
        table_bucket: TableBucket,
    ) -> RemoteLogSegment {
        RemoteLogSegment {
            segment_id: segment_id.to_string(),
            start_offset,
            end_offset: start_offset + 1000,
            size_in_bytes: 1024,
            table_bucket,
            max_timestamp,
        }
    }

    /// Helper function to create a RemoteLogDownloadRequest for testing
    fn create_request(segment: RemoteLogSegment) -> RemoteLogDownloadRequest {
        let (result_sender, _) = oneshot::channel();
        RemoteLogDownloadRequest {
            remote_log_tablet_dir: "test_dir".to_string(),
            segment,
            result_sender,
            retry_count: 0,
            next_retry_at: None,
            cancel_notify: Arc::new(Notify::new()),
            _pending_slot: None,
        }
    }

    #[test]
    fn test_priority_ordering_matching_java_test_case() {
        // Test priority ordering: timestamp across buckets, offset within bucket
        // Does NOT test tie-breakers (segment_id) - those are implementation details

        let bucket1 = create_table_bucket(1, 0);
        let bucket2 = create_table_bucket(1, 1);
        let bucket3 = create_table_bucket(1, 2);
        let bucket4 = create_table_bucket(1, 3);

        // Create segments with distinct timestamps/offsets (no ties)
        let seg_negative = create_segment("seg_neg", 0, -1, bucket1.clone());
        let seg_zero = create_segment("seg_zero", 0, 0, bucket2.clone());
        let seg_1000 = create_segment("seg_1000", 0, 1000, bucket3.clone());
        let seg_2000 = create_segment("seg_2000", 0, 2000, bucket4.clone());
        let seg_same_bucket_100 = create_segment("seg_sb_100", 100, 5000, bucket1.clone());
        let seg_same_bucket_50 = create_segment("seg_sb_50", 50, 5000, bucket1.clone());

        let mut heap = BinaryHeap::new();
        heap.push(Reverse(create_request(seg_2000)));
        heap.push(Reverse(create_request(seg_same_bucket_100)));
        heap.push(Reverse(create_request(seg_1000)));
        heap.push(Reverse(create_request(seg_zero)));
        heap.push(Reverse(create_request(seg_negative)));
        heap.push(Reverse(create_request(seg_same_bucket_50)));

        // Verify ordering by timestamp/offset, not segment_id
        let first = heap.pop().unwrap().0;
        assert_eq!(first.segment.max_timestamp, -1, "Lowest timestamp first");

        let second = heap.pop().unwrap().0;
        assert_eq!(second.segment.max_timestamp, 0);

        let third = heap.pop().unwrap().0;
        assert_eq!(third.segment.max_timestamp, 1000);

        let fourth = heap.pop().unwrap().0;
        assert_eq!(fourth.segment.max_timestamp, 2000);

        // Last two are same bucket (ts=5000), ordered by offset
        let fifth = heap.pop().unwrap().0;
        assert_eq!(fifth.segment.max_timestamp, 5000);
        assert_eq!(
            fifth.segment.start_offset, 50,
            "Lower offset first within bucket"
        );

        let sixth = heap.pop().unwrap().0;
        assert_eq!(sixth.segment.max_timestamp, 5000);
        assert_eq!(sixth.segment.start_offset, 100);
    }

    #[tokio::test]
    async fn test_concurrency_and_priority() {
        // Test concurrency limiting and priority-based scheduling together
        let fake_fetcher = Arc::new(FakeFetcher::new(0, false)); // Manual control

        let downloader = RemoteLogDownloader::new_with_fetcher(
            fake_fetcher.clone(),
            10, // High prefetch limit
            2,  // Max concurrent downloads = 2
            metrics(),
        )
        .unwrap();

        let bucket = create_table_bucket(1, 0);

        // Request 4 segments with same priority (to isolate concurrency limiting from priority)
        let segs: Vec<_> = (0..4)
            .map(|i| create_segment(&format!("seg{i}"), i * 100, 1000, bucket.clone()))
            .collect();

        let _futures: Vec<_> = segs
            .iter()
            .map(|seg| downloader.request_remote_log("dir", seg))
            .collect();

        // Wait for exactly 2 to start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            fake_fetcher.in_flight(),
            2,
            "Concurrency limit: exactly 2 should be in-flight"
        );

        // Release one
        fake_fetcher.release_one();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Max should never exceed 2
        assert_eq!(
            fake_fetcher.max_seen_in_flight(),
            2,
            "Max concurrent should not exceed 2"
        );

        // Release all
        fake_fetcher.release_all();
    }

    #[tokio::test]
    async fn test_prefetch_limit() {
        // Test that prefetch semaphore limits outstanding downloads
        let fake_fetcher = Arc::new(FakeFetcher::new(0, true)); // Auto-complete

        let downloader = RemoteLogDownloader::new_with_fetcher(
            fake_fetcher,
            2,  // Max prefetch = 2
            10, // High concurrent limit
            metrics(),
        )
        .unwrap();

        let bucket = create_table_bucket(1, 0);

        // Request 4 downloads
        let segs: Vec<_> = (0..4)
            .map(|i| create_segment(&format!("seg{i}"), i * 100, 1000, bucket.clone()))
            .collect();

        let mut futures: Vec<_> = segs
            .iter()
            .map(|seg| downloader.request_remote_log("dir", seg))
            .collect();

        // Wait for first 2 to complete
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if futures.iter().filter(|f| f.is_done()).count() >= 2 {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("Timeout waiting for first 2 downloads");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Verify 3rd and 4th are blocked (prefetch limit)
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            futures.iter().filter(|f| f.is_done()).count(),
            2,
            "Prefetch limit: only 2 should complete"
        );

        // Drop first 2 (releases permits)
        let f4 = futures.pop().unwrap();
        let f3 = futures.pop().unwrap();
        drop(futures);

        // 3rd and 4th should now complete
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if f3.is_done() && f4.is_done() {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("Timeout after permit release");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn pending_segment_budget_releases_on_cancellation() {
        let fake_fetcher = Arc::new(FakeFetcher::new(100, false));
        let downloader = RemoteLogDownloader::new_with_fetcher_and_limit(
            fake_fetcher.clone(),
            1,
            1,
            64 * 1024 * 1024,
            1,
            metrics(),
        )
        .unwrap();
        let bucket = create_table_bucket(1, 0);
        let first_seg = create_segment("first", 0, 1000, bucket.clone());
        let second_seg = create_segment("second", 100, 1000, bucket.clone());
        let first = downloader.request_remote_log("dir", &first_seg);
        tokio::time::timeout(Duration::from_secs(1), async {
            while fake_fetcher.in_flight() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(first);
        // Cancelling the future must not allow new work to bypass the still
        // blocked in-flight download's admission slot.
        assert_eq!(downloader.pending_slots.load(AtomicOrdering::SeqCst), 1);
        let second = downloader.request_remote_log("dir", &second_seg);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !second.is_done() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            second
                .take_remote_log_file()
                .unwrap_err()
                .to_string()
                .contains("pending segments")
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while downloader.pending_slots.load(AtomicOrdering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            fake_fetcher.in_flight(),
            0,
            "cancellation must abort the blocked download"
        );
        let third = downloader.request_remote_log("dir", &second_seg);
        assert_eq!(downloader.pending_slots.load(AtomicOrdering::SeqCst), 1);
        drop(third);
        tokio::time::timeout(Duration::from_secs(2), async {
            while downloader.pending_slots.load(AtomicOrdering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn remote_prefetch_byte_budget_cancels_blocked_downloads() {
        let fetcher = Arc::new(FakeFetcher::new(100, false));
        let downloader = RemoteLogDownloader::new_with_fetcher_and_limit(
            fetcher.clone(),
            2,
            4,
            1_536,
            2,
            metrics(),
        )
        .unwrap();
        let bucket = create_table_bucket(1, 0);
        let first =
            downloader.request_remote_log("dir", &create_segment("one", 0, 0, bucket.clone()));
        let second =
            downloader.request_remote_log("dir", &create_segment("two", 1, 0, bucket.clone()));
        tokio::time::timeout(Duration::from_secs(1), async {
            while fetcher.in_flight() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            downloader.prefetch_bytes.load(AtomicOrdering::SeqCst),
            1_024
        );
        assert!(
            !second.is_done(),
            "second download must wait for the byte budget"
        );
        drop(first);
        tokio::time::timeout(Duration::from_secs(2), async {
            while fetcher.in_flight() != 1
                || downloader.pending_slots.load(AtomicOrdering::SeqCst) != 1
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            downloader.prefetch_bytes.load(AtomicOrdering::SeqCst),
            1_024
        );
        drop(second);
        tokio::time::timeout(Duration::from_secs(2), async {
            while downloader.prefetch_bytes.load(AtomicOrdering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(fetcher.in_flight(), 0);

        let mut oversized = create_segment("too-big", 2, 0, bucket);
        oversized.size_in_bytes = 2_048;
        let rejected = downloader.request_remote_log("dir", &oversized);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !rejected.is_done() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            rejected
                .take_remote_log_file()
                .unwrap_err()
                .to_string()
                .contains("prefetch limit")
        );
        assert_eq!(downloader.prefetch_bytes.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn streaming_download_rejects_underreported_bytes_before_writing_them() -> Result<()> {
        let remote = TempDir::new()?;
        let output = TempDir::new()?;
        let remote_path = remote.path().join("segment.log");
        let output_path = output.path().join("segment.log");
        tokio::fs::write(&remote_path, vec![17_u8; 20]).await?;
        let op = opendal::Operator::new(
            opendal::services::Fs::default().root(remote.path().to_str().unwrap()),
        )?
        .finish();
        let reserved = Arc::new(AtomicUsize::new(1));
        let mut permit = PrefetchBytesPermit {
            reserved: Arc::clone(&reserved),
            bytes: 1,
            recycle_notify: Arc::new(Notify::new()),
        };
        let error = {
            let mut check = |actual| {
                if permit.adjust_to(actual, 12) {
                    Ok(())
                } else {
                    Err(Error::BufferExhausted {
                        message: "stream exceeded configured disk budget".into(),
                    })
                }
            };
            RemoteLogDownloader::download_file_streaming(
                &op,
                "segment.log",
                "segment.log",
                &output_path,
                StreamingReadOptions {
                    chunk_size: 8,
                    concurrency: 1,
                    timeout: Duration::from_secs(3),
                },
                Some(&mut check),
            )
            .await
            .expect_err("the last chunk must exceed the budget before being written")
        };
        assert!(matches!(error, Error::BufferExhausted { .. }));
        assert!(!output_path.exists(), "partial download must be removed");
        assert!(reserved.load(Ordering::SeqCst) <= 12);
        drop(permit);
        assert_eq!(reserved.load(Ordering::SeqCst), 0);

        let mut check = |_actual| Ok(());
        RemoteLogDownloader::download_file_streaming(
            &op,
            "segment.log",
            "segment.log",
            &output_path,
            StreamingReadOptions {
                chunk_size: 8,
                concurrency: 1,
                timeout: Duration::from_secs(3),
            },
            Some(&mut check),
        )
        .await?;
        assert_eq!(tokio::fs::read(&output_path).await?, vec![17_u8; 20]);
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_a_partial_stream_removes_file_and_byte_reservation() -> Result<()> {
        let remote = TempDir::new()?;
        let output = TempDir::new()?;
        tokio::fs::write(
            remote.path().join("segment.log"),
            vec![17_u8; 16 * 1024 * 1024],
        )
        .await?;
        let output_path = output.path().join("segment.log");
        let op = opendal::Operator::new(
            opendal::services::Fs::default().root(remote.path().to_str().unwrap()),
        )?
        .finish();
        let reserved = Arc::new(AtomicUsize::new(1));
        let tracked = Arc::clone(&reserved);
        let task_path = output_path.clone();
        let task = tokio::spawn(async move {
            let mut permit = PrefetchBytesPermit {
                reserved: tracked,
                bytes: 1,
                recycle_notify: Arc::new(Notify::new()),
            };
            let mut check = |actual| {
                if permit.adjust_to(actual, 32 * 1024 * 1024) {
                    Ok(())
                } else {
                    Err(Error::BufferExhausted {
                        message: "unexpected test budget exhaustion".into(),
                    })
                }
            };
            RemoteLogDownloader::download_file_streaming(
                &op,
                "segment.log",
                "segment.log",
                &task_path,
                StreamingReadOptions {
                    chunk_size: 1_024,
                    concurrency: 1,
                    timeout: Duration::from_secs(5),
                },
                Some(&mut check),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if tokio::fs::metadata(&output_path)
                    .await
                    .is_ok_and(|meta| meta.len() > 0)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("the partial file must be written before cancellation");
        assert!(
            !task.is_finished(),
            "the stream must be aborted mid-download"
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            !output_path.exists(),
            "cancelled download left a partial file"
        );
        assert_eq!(reserved.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_and_cancellation() {
        // Test retry with exponential backoff
        let fake_fetcher = Arc::new(FakeFetcher::new(2, true)); // Fail twice, succeed third time

        let downloader =
            RemoteLogDownloader::new_with_fetcher(fake_fetcher.clone(), 10, 1, metrics()).unwrap();

        let bucket = create_table_bucket(1, 0);
        let seg = create_segment("seg1", 0, 1000, bucket);

        let future = downloader.request_remote_log("dir", &seg);

        // Should succeed after retries
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if future.is_done() {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("Timeout waiting for retry to succeed");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(future.is_done(), "Should succeed after retries");

        // Test cancellation
        let seg2 = create_segment("seg2", 100, 1000, create_table_bucket(1, 0));
        let fake_fetcher2 = Arc::new(FakeFetcher::new(100, true)); // Fail forever
        let downloader2 =
            RemoteLogDownloader::new_with_fetcher(fake_fetcher2.clone(), 10, 1, metrics()).unwrap();

        let future2 = downloader2.request_remote_log("dir", &seg2);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Drop to cancel
        drop(future2);
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            fake_fetcher2.in_flight(),
            0,
            "Cancellation should release resources"
        );
    }
}
