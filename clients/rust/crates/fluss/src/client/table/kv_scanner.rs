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

//! One server-side RocksDB snapshot session per KV bucket. Continuations use
//! the same session; failures never restart against a different snapshot.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::client::ClientSchemaGetter;
use crate::client::metadata::Metadata;
use crate::client::table::batch_scanner::decode_kv_batch;
use crate::error::{ApiError, Error, FlussError, Result};
use crate::metadata::{TableBucket, TableInfo};
use crate::proto::{ErrorResponse, PbScanReqForBucket};
use crate::rpc::message::ScanKvRequest;
use crate::rpc::{RpcClient, ServerConnection};

const BATCH_SIZE_BYTES: i32 = 1024 * 1024;

/// Streams the live rows of a KV bucket as Arrow batches. The server fixes a
/// RocksDB snapshot on the first request. Abandoning or failing a scan closes
/// its session (or lets the server's TTL reclaim an in-flight open request).
pub struct KvBatchScanner {
    rpc: Arc<RpcClient>,
    metadata: Arc<Metadata>,
    info: TableInfo,
    schema_getter: Arc<ClientSchemaGetter>,
    projection: Option<Vec<usize>>,
    bucket: TableBucket,
    connection: Option<ServerConnection>,
    scanner_id: Option<Vec<u8>>,
    sequence: i32,
    finished: bool,
}

impl KvBatchScanner {
    pub(super) fn new(
        rpc: Arc<RpcClient>,
        metadata: Arc<Metadata>,
        info: TableInfo,
        schema_getter: Arc<ClientSchemaGetter>,
        projection: Option<Vec<usize>>,
        bucket: TableBucket,
    ) -> Self {
        Self {
            rpc,
            metadata,
            info,
            schema_getter,
            projection,
            bucket,
            connection: None,
            scanner_id: None,
            sequence: 0,
            finished: false,
        }
    }

    /// Fetches one nonempty batch or the end of this bucket's snapshot. An
    /// error invalidates the session; do not resume this scanner after failure.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        let result = self.read_next().await;
        if result.is_err() {
            self.finished = true;
        }
        result
    }

    async fn read_next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            let connection = match &self.connection {
                Some(connection) => connection.clone(),
                None => {
                    let leader = self
                        .metadata
                        .leader_for(&self.info.table_path, &self.bucket)
                        .await?
                        .ok_or_else(|| {
                            Error::leader_not_available(format!(
                                "No leader found for table bucket: {}",
                                self.bucket
                            ))
                        })?;
                    let connection = self.rpc.get_connection(&leader).await?;
                    self.connection = Some(connection.clone());
                    connection
                }
            };
            let request = match &self.scanner_id {
                Some(id) => ScanKvRequest::new(
                    Some(id.clone()),
                    None,
                    Some(self.sequence),
                    Some(BATCH_SIZE_BYTES),
                    None,
                ),
                None => ScanKvRequest::new(
                    None,
                    Some(PbScanReqForBucket {
                        table_id: self.bucket.table_id(),
                        partition_id: self.bucket.partition_id(),
                        bucket_id: self.bucket.bucket_id(),
                        limit: None,
                        routing_bucket_count: Some(self.info.get_num_buckets()),
                    }),
                    Some(0),
                    Some(BATCH_SIZE_BYTES),
                    None,
                ),
            };
            let response = connection.request(request).await?;
            if let Some(code) = response.error_code
                && code != FlussError::None.code()
            {
                let error: ApiError = ErrorResponse {
                    error_code: code,
                    error_message: response.error_message,
                }
                .into();
                return Err(Error::FlussAPIError { api_error: error });
            }
            let more = response.has_more_results.unwrap_or(false);
            if more {
                self.scanner_id =
                    Some(response.scanner_id.ok_or_else(|| Error::UnexpectedError {
                        message: "KV scan continuation is missing its scanner ID".into(),
                        source: None,
                    })?);
                self.sequence =
                    self.sequence
                        .checked_add(1)
                        .ok_or_else(|| Error::UnexpectedError {
                            message: "KV scan sequence exhausted".into(),
                            source: None,
                        })?;
            } else {
                self.scanner_id = None;
                self.finished = true;
            }
            let records = response.records.unwrap_or_default();
            if !records.is_empty() {
                let batch = decode_kv_batch(
                    &self.info,
                    &self.schema_getter,
                    self.projection.as_deref(),
                    records,
                    usize::MAX,
                )
                .await?;
                if batch.num_rows() > 0 {
                    return Ok(Some(batch));
                }
            }
            if self.finished {
                return Ok(None);
            }
        }
    }
}

impl Drop for KvBatchScanner {
    fn drop(&mut self) {
        let (Some(connection), Some(id)) = (self.connection.take(), self.scanner_id.take()) else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = connection
                    .request(ScanKvRequest::new(Some(id), None, None, None, Some(true)))
                    .await;
            });
        }
    }
}
