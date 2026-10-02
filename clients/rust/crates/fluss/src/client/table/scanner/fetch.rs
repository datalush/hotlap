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

//! Log fetcher construction, schema contexts and error classification.

use super::{
    Arc, ClientSchemaGetter, Config, FetchErrorAction, FetchErrorContext, FetchErrorLogLevel,
    FlussError, HashMap, HashSet, LogFetchBuffer, LogFetcher, LogScannerStatus, Metadata, Mutex,
    PbPredicate, ReadContext, ReadContextResolver, RemoteLogDownloader, Result, RowType, RpcClient,
    RwLock, ScannerMetrics, SchemaRef, SecurityTokenManager, TableBucket, TableInfo, TempDir,
    to_arrow_schema,
};
use crate::client::table::remote_log::RemoteDownloadLimits;

impl LogFetcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_info: TableInfo,
        conns: Arc<RpcClient>,
        metadata: Arc<Metadata>,
        log_scanner_status: Arc<LogScannerStatus>,
        config: &Config,
        projected_fields: Option<Vec<usize>>,
        fixed_schema: bool,
        filter: Option<PbPredicate>,
        metrics: Arc<ScannerMetrics>,
        schema_getter: Arc<ClientSchemaGetter>,
    ) -> Result<Self> {
        let full_row_type = table_info.get_row_type();
        let full_arrow_schema = to_arrow_schema(full_row_type)?;
        let projected_row_type = match &projected_fields {
            None => Arc::new(full_row_type.clone()),
            Some(fields) => Arc::new(RowType::new(
                fields
                    .iter()
                    .map(|&i| full_row_type.fields()[i].clone())
                    .collect(),
            )),
        };
        let read_context = Arc::new(
            Self::create_read_context(
                full_arrow_schema.clone(),
                projected_row_type.clone(),
                projected_fields.clone(),
                false,
            )?
            .with_fluss_row_type(projected_row_type.clone()),
        );
        let remote_read_context = Arc::new(
            Self::create_read_context(
                full_arrow_schema,
                projected_row_type.clone(),
                projected_fields.clone(),
                true,
            )?
            .with_fluss_row_type(projected_row_type),
        );

        let initial_schema_id = table_info.get_schema_id() as i16;
        let mut resolver = ReadContextResolver::new(
            initial_schema_id,
            read_context,
            remote_read_context,
            projected_fields,
        )
        .with_schema_getter(Arc::clone(&schema_getter));
        if fixed_schema {
            resolver = resolver.with_fixed_schema(table_info.get_schema());
        }
        let resolver = Arc::new(resolver);

        let tmp_dir = TempDir::with_prefix("fluss-remote-logs")?;
        let log_fetch_buffer = Arc::new(LogFetchBuffer::new(Arc::clone(&resolver)));

        // Create security token manager for background token refresh
        let security_token_manager = Arc::new(SecurityTokenManager::new(
            conns.clone(),
            metadata.clone(),
            std::time::Duration::from_millis(config.scanner_remote_log_operation_timeout_ms),
        ));

        // Subscribe to credentials updates and pass to remote log downloader
        let credentials_rx = security_token_manager.subscribe();

        let remote_log_downloader = Arc::new(RemoteLogDownloader::new(
            tmp_dir,
            RemoteDownloadLimits::from_config(config),
            credentials_rx,
            Arc::clone(&metrics),
        )?);

        // Start the background token refresh task
        security_token_manager.start();

        Ok(LogFetcher {
            conns: conns.clone(),
            metadata: metadata.clone(),
            table_path: table_info.table_path.clone(),
            is_partitioned: table_info.is_partitioned(),
            log_scanner_status,
            resolver,
            remote_log_downloader,
            security_token_manager,
            log_fetch_buffer,
            nodes_with_pending_fetch_requests: Arc::new(Mutex::new(HashSet::new())),
            metrics,
            filter: filter.map(|predicate| (predicate, table_info.get_schema_id())),
            partition_bucket_counts: RwLock::new(HashMap::new()),
            max_poll_records: config.scanner_log_max_poll_records,
            fetch_max_bytes: config.scanner_log_fetch_max_bytes,
            fetch_min_bytes: config.scanner_log_fetch_min_bytes,
            fetch_wait_max_time_ms: config.scanner_log_fetch_wait_max_time_ms,
            fetch_max_bytes_for_bucket: config.scanner_log_fetch_max_bytes_for_bucket,
        })
    }

    fn create_read_context(
        full_arrow_schema: SchemaRef,
        row_type: Arc<RowType>,
        projected_fields: Option<Vec<usize>>,
        is_from_remote: bool,
    ) -> Result<ReadContext> {
        match projected_fields {
            None => Ok(ReadContext::new(
                full_arrow_schema,
                row_type,
                is_from_remote,
            )),
            Some(fields) => ReadContext::with_projection_pushdown(
                full_arrow_schema,
                row_type,
                fields,
                is_from_remote,
            ),
        }
    }

    pub(super) fn describe_fetch_error(
        error: FlussError,
        table_bucket: &TableBucket,
        fetch_offset: i64,
        error_message: &str,
    ) -> FetchErrorContext {
        match error {
            FlussError::NotLeaderOrFollower
            | FlussError::LogStorageException
            | FlussError::KvStorageException
            | FlussError::StorageException
            | FlussError::FencedLeaderEpochException
            | FlussError::LeaderNotAvailableException => FetchErrorContext {
                action: FetchErrorAction::Ignore,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Error in fetch for bucket {table_bucket}: {error:?}: {error_message}"
                ),
            },
            FlussError::UnknownTableOrBucketException => FetchErrorContext {
                action: FetchErrorAction::Ignore,
                log_level: FetchErrorLogLevel::Warn,
                log_message: format!(
                    "Received unknown table or bucket error in fetch for bucket {table_bucket}"
                ),
            },
            FlussError::LogOffsetOutOfRangeException => FetchErrorContext {
                action: FetchErrorAction::LogOffsetOutOfRange,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "The fetching offset {fetch_offset} is out of range for bucket {table_bucket}: {error_message}"
                ),
            },
            FlussError::AuthorizationException => FetchErrorContext {
                action: FetchErrorAction::Authorization,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Authorization error while fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                ),
            },
            FlussError::UnknownServerError => FetchErrorContext {
                action: FetchErrorAction::Ignore,
                log_level: FetchErrorLogLevel::Warn,
                log_message: format!(
                    "Unknown server error while fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                ),
            },
            FlussError::CorruptMessage => FetchErrorContext {
                action: FetchErrorAction::CorruptMessage,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Encountered corrupt message when fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                ),
            },
            _ => FetchErrorContext {
                action: FetchErrorAction::Unexpected,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Unexpected error code {error:?} while fetching at offset {fetch_offset} from bucket {table_bucket}: {error_message}"
                ),
            },
        }
    }

    pub(super) fn should_invalidate_table_meta(error: FlussError) -> bool {
        matches!(
            error,
            FlussError::NotLeaderOrFollower
                | FlussError::LeaderNotAvailableException
                | FlussError::FencedLeaderEpochException
                | FlussError::UnknownTableOrBucketException
                | FlussError::InvalidCoordinatorException
        )
    }
}
