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

//! Process fetch responses, including errors, remote segments and pruned ranges.

use super::{
    ApiError, Arc, DefaultCompletedFetch, ErrorResponse, FetchErrorLogLevel, FetchLogRequest,
    FetchLogResponse, FetchResponseContext, FlussError, HashSet, LogFetchBuffer, LogFetcher,
    LogRecordsBatches, Metadata, NO_FILTERED_END_OFFSET, PhysicalTablePath, ReadContextResolver,
    RemoteLogDownloader, RemoteLogFetchInfo, RemotePendingFetch, TableBucket, debug, warn,
};
use prost::Message;

impl LogFetcher {
    pub(super) async fn handle_fetch_failure(
        metadata: Arc<Metadata>,
        server_id: &i32,
        request: &FetchLogRequest,
    ) {
        let table_ids = request.tables_req.iter().map(|r| r.table_id).collect();
        metadata.invalidate_server(server_id, table_ids);
    }

    /// Handle fetch response and add completed fetches to buffer
    pub(super) async fn handle_fetch_response(
        fetch_response: FetchLogResponse,
        context: FetchResponseContext,
    ) {
        let FetchResponseContext {
            metadata,
            log_fetch_buffer,
            log_scanner_status,
            resolver,
            remote_log_downloader,
            metrics,
            request_start_time,
        } = context;

        // `encoded_len()` mirrors Java's `fetchLogResponse.totalSize()`:
        // both report the serialized API message body size, excluding protocol
        // headers and framing. Recorded unconditionally (including zero-record
        // responses) to match Java's histogram semantics.
        metrics.record_fetch_latency_ms(request_start_time.elapsed().as_secs_f64() * 1000.0);
        metrics.record_bytes_per_request(fetch_response.encoded_len() as f64);

        for pb_fetch_log_resp in fetch_response.tables_resp {
            let table_id = pb_fetch_log_resp.table_id;
            let fetch_log_for_buckets = pb_fetch_log_resp.buckets_resp;

            for fetch_log_for_bucket in fetch_log_for_buckets {
                let bucket: i32 = fetch_log_for_bucket.bucket_id;
                let table_bucket = TableBucket::new_with_partition(
                    table_id,
                    fetch_log_for_bucket.partition_id,
                    bucket,
                );

                // todo: check fetch result code for per-bucket
                let Some(fetch_offset) = log_scanner_status.get_bucket_offset(&table_bucket) else {
                    debug!(
                        "Ignoring fetch log response for bucket {table_bucket} because the bucket has been unsubscribed."
                    );
                    continue;
                };

                if let Some(error_code) = fetch_log_for_bucket.error_code
                    && error_code != FlussError::None.code()
                {
                    let api_error: ApiError = ErrorResponse {
                        error_code,
                        error_message: fetch_log_for_bucket.error_message.clone(),
                    }
                    .into();

                    let error = FlussError::for_code(error_code);
                    if Self::should_invalidate_table_meta(error) {
                        let table_id = table_bucket.table_id();
                        let cluster = metadata.get_cluster();
                        if let Some(table_path) = cluster.get_table_path_by_id(table_id) {
                            let physical_table_path = match table_bucket.partition_id() {
                                Some(partition_id) => {
                                    match cluster.get_partition_name(partition_id) {
                                        Some(partition_name) => {
                                            Some(PhysicalTablePath::of_partitioned(
                                                Arc::new(table_path.clone()),
                                                Some(partition_name.clone()),
                                            ))
                                        }
                                        None => {
                                            warn!(
                                                "Partition id {partition_id} is missing from partition_name_by_id while invalidating metadata for table {table_path}"
                                            );
                                            None
                                        }
                                    }
                                }
                                None => Some(PhysicalTablePath::of(Arc::new(table_path.clone()))),
                            };
                            if let Some(physical_table_path) = physical_table_path {
                                metadata.invalidate_physical_table_meta(&HashSet::from([
                                    physical_table_path,
                                ]));
                            }
                        } else {
                            warn!(
                                "Table id {table_id} is missing from table_path_by_id while invalidating table metadata"
                            );
                        }
                    }
                    let error_context = Self::describe_fetch_error(
                        error,
                        &table_bucket,
                        fetch_offset,
                        api_error.message.as_str(),
                    );
                    log_scanner_status.move_bucket_to_end(table_bucket.clone());
                    match error_context.log_level {
                        FetchErrorLogLevel::Debug => {
                            debug!("{}", error_context.log_message);
                        }
                        FetchErrorLogLevel::Warn => {
                            warn!("{}", error_context.log_message);
                        }
                    }
                    log_fetch_buffer.add_api_error(
                        table_bucket.clone(),
                        api_error,
                        error_context,
                        fetch_offset,
                    );
                    continue;
                }

                // Check if this is a remote log fetch
                if let Some(ref remote_log_fetch_info) = fetch_log_for_bucket.remote_log_fetch_info
                {
                    // Remote fs props are already set by the background SecurityTokenManager
                    let remote_fetch_info =
                        RemoteLogFetchInfo::from_proto(remote_log_fetch_info, table_bucket.clone());

                    let high_watermark = fetch_log_for_bucket.high_watermark.unwrap_or(-1);
                    Self::pending_remote_fetches(
                        remote_log_downloader.clone(),
                        log_fetch_buffer.clone(),
                        Arc::clone(&resolver),
                        &table_bucket,
                        remote_fetch_info,
                        fetch_offset,
                        high_watermark,
                    );
                } else if fetch_log_for_bucket.records.is_some()
                    || fetch_log_for_bucket.filtered_end_offset.is_some()
                {
                    // Handle regular in-memory records - create completed fetch directly.
                    // A filtered response may arrive empty, or carry records with a
                    // pruned tail; either way the end offset is how far the server
                    // scanned, so the client skips that range instead of re-fetching it.
                    let high_watermark = fetch_log_for_bucket.high_watermark.unwrap_or(-1);
                    let filtered_end_offset = Self::validate_filtered_end_offset(
                        fetch_log_for_bucket.filtered_end_offset,
                        fetch_offset,
                        &table_bucket,
                    );
                    let records = fetch_log_for_bucket.records.unwrap_or(vec![]);
                    let size_in_bytes = records.len();

                    let log_record_batch = LogRecordsBatches::new(records);
                    let completed_fetch = DefaultCompletedFetch::new(
                        table_bucket.clone(),
                        log_record_batch,
                        size_in_bytes,
                        Arc::clone(&resolver),
                        false, // is_remote
                        fetch_offset,
                        high_watermark,
                    )
                    .with_filtered_end_offset(filtered_end_offset);
                    log_fetch_buffer.add(Box::new(completed_fetch));
                }
            }
        }
    }

    /// Drops a filtered end offset that would move the bucket backwards, since
    /// the server is only ever meant to report a range it has already scanned.
    fn validate_filtered_end_offset(
        filtered_end_offset: Option<i64>,
        fetch_offset: i64,
        table_bucket: &TableBucket,
    ) -> i64 {
        match filtered_end_offset {
            Some(end) if end >= fetch_offset => end,
            Some(end) => {
                warn!(
                    "Ignoring filtered end offset {end} for bucket {table_bucket} because it precedes the fetch offset {fetch_offset}"
                );
                NO_FILTERED_END_OFFSET
            }
            None => NO_FILTERED_END_OFFSET,
        }
    }

    fn pending_remote_fetches(
        remote_log_downloader: Arc<RemoteLogDownloader>,
        log_fetch_buffer: Arc<LogFetchBuffer>,
        resolver: Arc<ReadContextResolver>,
        table_bucket: &TableBucket,
        remote_fetch_info: RemoteLogFetchInfo,
        fetch_offset: i64,
        high_watermark: i64,
    ) {
        // Download and process remote log segments
        let mut pos_in_log_segment = remote_fetch_info.first_start_pos;
        let mut current_fetch_offset = fetch_offset;
        for (i, segment) in remote_fetch_info.remote_log_segments.iter().enumerate() {
            if i > 0 {
                pos_in_log_segment = 0;
                current_fetch_offset = segment.start_offset;
            }

            // todo:
            // 1: control the max threads to download remote segment
            // 2: introduce priority queue to priority highest for earliest segment
            let download_future = remote_log_downloader
                .request_remote_log(&remote_fetch_info.remote_log_tablet_dir, segment);

            // Register callback to be called when download completes
            // (similar to Java's downloadFuture.onComplete)
            // This must be done before creating RemotePendingFetch to avoid move issues
            let table_bucket = table_bucket.clone();
            let weak_buffer = Arc::downgrade(&log_fetch_buffer);
            download_future.on_complete(move || {
                if let Some(buffer) = weak_buffer.upgrade() {
                    buffer.try_complete(&table_bucket);
                }
            });

            let pending_fetch = RemotePendingFetch::new(
                segment.clone(),
                download_future,
                pos_in_log_segment,
                current_fetch_offset,
                high_watermark,
                Arc::clone(&resolver),
            );
            // Add to pending fetches in buffer (similar to Java's logFetchBuffer.pend)
            log_fetch_buffer.pend(Box::new(pending_fetch));
        }
    }
}
