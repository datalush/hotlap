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

#[tokio::test]
async fn collect_fetches_updates_offset() -> Result<()> {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster));
    let status = Arc::new(LogScannerStatus::new());
    let fetcher = LogFetcher::new(
        table_info.clone(),
        Arc::new(RpcClient::new()),
        metadata.clone(),
        status.clone(),
        &Config::default(),
        None,
        false,
        None,
        test_scanner_metrics(&table_path),
        test_schema_getter(&table_info, &metadata),
    )?;

    let bucket = TableBucket::new(1, 0);
    status.assign_scan_bucket(bucket.clone(), 0);

    let data = build_records(&table_info, Arc::new(table_path))?;
    let log_records = LogRecordsBatches::new(data.clone());
    let resolver = test_resolver(&table_info);
    let completed = DefaultCompletedFetch::new(
        bucket.clone(),
        log_records,
        data.len(),
        resolver,
        false,
        0,
        0,
    );
    fetcher.log_fetch_buffer.add(Box::new(completed));

    let fetched = fetcher.collect_fetches().await?;
    assert_eq!(fetched.get(&bucket).unwrap().len(), 1);
    assert_eq!(status.get_bucket_offset(&bucket), Some(1));
    Ok(())
}

#[tokio::test]
async fn fetch_records_from_fetch_drains_unassigned_bucket() -> Result<()> {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster));
    let status = Arc::new(LogScannerStatus::new());
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
    )?;

    let bucket = TableBucket::new(1, 0);
    let data = build_records(&table_info, Arc::new(table_path))?;
    let log_records = LogRecordsBatches::new(data.clone());
    let resolver = test_resolver(&table_info);
    let mut completed: Box<dyn CompletedFetch> = Box::new(DefaultCompletedFetch::new(
        bucket,
        log_records,
        data.len(),
        resolver,
        false,
        0,
        0,
    ));

    let records = fetcher.fetch_records_from_fetch(&mut completed, 10)?;
    assert!(matches!(records, FetchResult::Data(records) if records.is_empty()));
    assert!(completed.is_consumed());
    Ok(())
}

#[tokio::test]
async fn prepare_fetch_log_requests_skips_pending() -> Result<()> {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster));
    let status = Arc::new(LogScannerStatus::new());
    status.assign_scan_bucket(TableBucket::new(1, 0), 0);
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
    )?;

    fetcher.nodes_with_pending_fetch_requests.lock().insert(1);

    let requests = fetcher.prepare_fetch_log_requests().await;
    assert!(requests.is_empty());
    Ok(())
}

#[tokio::test]
async fn prepare_fetch_log_requests_carries_the_filter() -> Result<()> {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster));
    let status = Arc::new(LogScannerStatus::new());
    status.assign_scan_bucket(TableBucket::new(1, 0), 0);
    let fetcher = filtering_fetcher(
        &table_info,
        &metadata,
        status,
        Some(crate::predicate::col("id").gt(5i32)),
    )?;

    let requests = fetcher.prepare_fetch_log_requests().await;
    let table_req = &requests.get(&1).expect("request for leader").tables_req[0];
    let predicate = table_req
        .filter_predicate
        .as_ref()
        .expect("filter predicate");
    assert_eq!(predicate.r#type, 0);
    assert_eq!(predicate.leaf.as_ref().expect("leaf").field_id, 0);
    // Both fields must travel together, and the id pins the field ids.
    assert_eq!(table_req.filter_schema_id, Some(table_info.get_schema_id()));
    Ok(())
}

#[tokio::test]
async fn prepare_fetch_log_requests_omits_an_absent_filter() -> Result<()> {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster));
    let status = Arc::new(LogScannerStatus::new());
    status.assign_scan_bucket(TableBucket::new(1, 0), 0);
    let fetcher = filtering_fetcher(&table_info, &metadata, status, None)?;

    let requests = fetcher.prepare_fetch_log_requests().await;
    let table_req = &requests.get(&1).expect("request for leader").tables_req[0];
    assert!(table_req.filter_predicate.is_none());
    assert!(table_req.filter_schema_id.is_none());
    Ok(())
}

/// Without this the bucket offset never advances and the scanner re-requests
/// the same range forever whenever a filter prunes a whole fetch.
#[tokio::test]
async fn handle_fetch_response_advances_past_a_fully_filtered_range() -> Result<()> {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster));
    let status = Arc::new(LogScannerStatus::new());
    let bucket = TableBucket::new(1, 0);
    status.assign_scan_bucket(bucket.clone(), 2);
    let fetcher = filtering_fetcher(
        &table_info,
        &metadata,
        status.clone(),
        Some(crate::predicate::col("id").gt(5i32)),
    )?;

    LogFetcher::handle_fetch_response(
        filtered_response(Some(11), Some(9)),
        test_response_context(&fetcher, &metadata),
    )
    .await;

    let fetched = fetcher.collect_fetches().await?;
    assert!(fetched.is_empty());
    assert_eq!(status.get_bucket_offset(&bucket), Some(11));
    Ok(())
}

#[tokio::test]
async fn handle_fetch_response_ignores_a_filtered_range_behind_the_fetch_offset() -> Result<()> {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster));
    let status = Arc::new(LogScannerStatus::new());
    let bucket = TableBucket::new(1, 0);
    status.assign_scan_bucket(bucket.clone(), 5);
    let fetcher = filtering_fetcher(
        &table_info,
        &metadata,
        status.clone(),
        Some(crate::predicate::col("id").gt(5i32)),
    )?;

    LogFetcher::handle_fetch_response(
        filtered_response(Some(3), None),
        test_response_context(&fetcher, &metadata),
    )
    .await;

    let fetched = fetcher.collect_fetches().await?;
    assert!(fetched.is_empty());
    assert_eq!(status.get_bucket_offset(&bucket), Some(5));
    Ok(())
}

/// The server reports a filtered range alongside records when it prunes only
/// the tail of what it scanned, so the offset must clear the whole range.
#[tokio::test]
async fn handle_fetch_response_skips_a_pruned_tail_after_its_records() -> Result<()> {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster));
    let status = Arc::new(LogScannerStatus::new());
    let bucket = TableBucket::new(1, 0);
    status.assign_scan_bucket(bucket.clone(), 0);
    let fetcher = filtering_fetcher(
        &table_info,
        &metadata,
        status.clone(),
        Some(crate::predicate::col("id").gt(5i32)),
    )?;

    let mut response = filtered_response(Some(8), Some(9));
    response.tables_resp[0].buckets_resp[0].records =
        Some(build_records(&table_info, Arc::new(table_path))?);
    LogFetcher::handle_fetch_response(response, test_response_context(&fetcher, &metadata)).await;

    let fetched = fetcher.collect_fetches().await?;
    assert_eq!(fetched.get(&bucket).expect("records").len(), 1);
    // The single record ends at offset 1, but the server scanned through 8.
    assert_eq!(status.get_bucket_offset(&bucket), Some(8));
    Ok(())
}

#[tokio::test]
async fn handle_fetch_response_sets_error() -> Result<()> {
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
        status.clone(),
        &Config::default(),
        None,
        false,
        None,
        test_scanner_metrics(&table_path),
        test_schema_getter(&table_info, &metadata),
    )?;

    let response = FetchLogResponse {
        tables_resp: vec![PbFetchLogRespForTable {
            table_id: 1,
            buckets_resp: vec![PbFetchLogRespForBucket {
                partition_id: None,
                bucket_id: 0,
                error_code: Some(FlussError::AuthorizationException.code()),
                error_message: Some("denied".to_string()),
                high_watermark: None,
                log_start_offset: None,
                remote_log_fetch_info: None,
                records: None,
                filtered_end_offset: None,
                min_retain_offset: None,
            }],
        }],
    };

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

    let completed = fetcher.log_fetch_buffer.poll().expect("completed fetch");
    let api_error = completed.api_error().expect("api error");
    assert_eq!(api_error.code, FlussError::AuthorizationException.code());
    Ok(())
}

#[tokio::test]
async fn handle_fetch_response_invalidates_table_meta() -> Result<()> {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
    let status = Arc::new(LogScannerStatus::new());
    status.assign_scan_bucket(TableBucket::new(1, 0), 5);
    let fetcher = LogFetcher::new(
        table_info.clone(),
        Arc::new(RpcClient::new()),
        metadata.clone(),
        status.clone(),
        &Config::default(),
        None,
        false,
        None,
        test_scanner_metrics(&table_path),
        test_schema_getter(&table_info, &metadata),
    )?;

    let bucket = TableBucket::new(1, 0);
    assert!(metadata.leader_for(&table_path, &bucket).await?.is_some());

    let response = FetchLogResponse {
        tables_resp: vec![PbFetchLogRespForTable {
            table_id: 1,
            buckets_resp: vec![PbFetchLogRespForBucket {
                partition_id: None,
                bucket_id: 0,
                error_code: Some(FlussError::NotLeaderOrFollower.code()),
                error_message: Some("not leader".to_string()),
                high_watermark: None,
                log_start_offset: None,
                remote_log_fetch_info: None,
                records: None,
                filtered_end_offset: None,
                min_retain_offset: None,
            }],
        }],
    };

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

    assert!(metadata.get_cluster().leader_for(&bucket).is_none());
    Ok(())
}

#[tokio::test]
async fn prepare_fetch_log_requests_uses_configured_fetch_params() -> Result<()> {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster));
    let status = Arc::new(LogScannerStatus::new());
    status.assign_scan_bucket(TableBucket::new(1, 0), 0);

    let config = Config {
        scanner_log_fetch_max_bytes: 1234,
        scanner_log_fetch_min_bytes: 7,
        scanner_log_fetch_wait_max_time_ms: 89,
        scanner_log_fetch_max_bytes_for_bucket: 512,
        ..Config::default()
    };

    let fetcher = LogFetcher::new(
        table_info.clone(),
        Arc::new(RpcClient::new()),
        metadata.clone(),
        status,
        &config,
        None,
        false,
        None,
        test_scanner_metrics(&table_path),
        test_schema_getter(&table_info, &metadata),
    )?;

    let requests = fetcher.prepare_fetch_log_requests().await;
    // In this test cluster, leader id should exist; but even if it changes,
    // assert over all built requests.
    assert!(!requests.is_empty());
    for req in requests.values() {
        assert_eq!(req.max_bytes, 1234);
        assert_eq!(req.min_bytes, Some(7));
        assert_eq!(req.max_wait_ms, Some(89));

        for table_req in &req.tables_req {
            for bucket_req in &table_req.buckets_req {
                assert_eq!(bucket_req.max_fetch_bytes, 512);
            }
        }
    }
    Ok(())
}
