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
use crate::proto::{ErrorResponse, PbScanReqForBucket, ScanKvResponse};
use crate::rpc::message::ScanKvRequest;
use crate::rpc::{RpcClient, ServerConnection};

const BATCH_SIZE_BYTES: i32 = 1024 * 1024;

/// Successful server responses and snapshot sessions observed by this reader.
/// One response can contain zero rows, and Arrow decoding can yield multiple
/// batches from other sources, so these are counted at the RPC boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KvScanStats {
    pub pages_received: usize,
    pub sessions_opened: usize,
}

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
    in_flight: bool,
    failed: bool,
    stats: KvScanStats,
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
            in_flight: false,
            failed: false,
            stats: KvScanStats::default(),
        }
    }

    /// Cumulative counters for completed RPCs, including empty responses.
    pub fn stats(&self) -> KvScanStats {
        self.stats
    }

    /// Fetches one nonempty batch or the end of this bucket's snapshot. An
    /// error invalidates the session; do not resume this scanner after failure.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Dropping a future while an RPC or its schema decode is in flight
        // leaves it unclear whether the server advanced the cursor. Resuming
        // could silently omit a page, even if the next RPC itself succeeds.
        if self.in_flight || self.failed {
            self.failed = true;
            return Err(Error::UnexpectedError {
                message: "KV snapshot scan was interrupted; open a new scanner".into(),
                source: None,
            });
        }
        if self.finished {
            return Ok(None);
        }
        let result = self.read_next().await;
        if result.is_err() {
            self.failed = true;
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
            self.in_flight = true;
            let response = connection.request(request).await?;
            check_response(&response)?;
            self.stats.pages_received += 1;
            if self.sequence == 0 && response.scanner_id.is_some() {
                self.stats.sessions_opened += 1;
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
                    self.in_flight = false;
                    return Ok(Some(batch));
                }
            }
            if self.finished {
                self.in_flight = false;
                return Ok(None);
            }
        }
    }
}

fn check_response(response: &ScanKvResponse) -> Result<()> {
    if let Some(code) = response.error_code
        && code != FlussError::None.code()
    {
        let error: ApiError = ErrorResponse {
            error_code: code,
            error_message: response.error_message.clone(),
        }
        .into();
        return Err(Error::FlussAPIError { api_error: error });
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::FlussConnection;
    use crate::config::Config;
    use crate::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use crate::row::GenericRow;
    use std::time::Duration;

    #[test]
    fn expired_or_moved_session_fails_instead_of_mixing_snapshots() {
        for code in [FlussError::ScannerExpired, FlussError::NotLeaderOrFollower] {
            let response = ScanKvResponse {
                error_code: Some(code.code()),
                error_message: Some("session is no longer valid".into()),
                ..Default::default()
            };
            let error = check_response(&response).expect_err("failed session must fail the scan");
            assert!(matches!(error, Error::FlussAPIError { .. }));
            assert!(error.to_string().contains("session is no longer valid"));
        }
    }

    #[tokio::test]
    #[ignore = "requires native-sni and FLUSS_* credentials; run with --ignored"]
    async fn server_closed_session_fails_without_restarting_snapshot()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let connection = FlussConnection::new(Config {
            bootstrap_servers: std::env::var("FLUSS_BOOTSTRAP")?,
            security_ssl_enabled: true,
            security_ssl_ca_file: Some(std::env::var("FLUSS_CA_FILE")?),
            security_protocol: "sasl".into(),
            security_sasl_username: std::env::var("FLUSS_USER")?,
            security_sasl_password: std::env::var("FLUSS_PASSWORD")?,
            ..Config::default()
        })
        .await?;
        let admin = connection.get_admin()?;
        admin
            .create_database("datafusion_tests", None, true)
            .await?;
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let path = TablePath::new("datafusion_tests", format!("kv_loss_{suffix}"));
        admin
            .create_table(
                &path,
                &TableDescriptor::builder()
                    .schema(
                        Schema::builder()
                            .column("id", DataTypes::int())
                            .column("value", DataTypes::string())
                            .primary_key(vec!["id"])?
                            .build()?,
                    )
                    .distributed_by(Some(1), vec!["id".into()])
                    .build()?,
                false,
            )
            .await?;

        let check: std::result::Result<(), Box<dyn std::error::Error>> = async {
            let table = connection.get_table(&path).await?;
            let writer = table.new_upsert()?.create_writer()?;
            let payload = "x".repeat(350_000);
            for id in 0..8 {
                let mut row = GenericRow::new(2);
                row.set_field(0, id);
                row.set_field(1, format!("{id}-{payload}"));
                writer.upsert(&row)?;
            }
            writer.flush().await?;
            let bucket = TableBucket::new(table.get_table_info().table_id, 0);
            let mut scanner = table.new_scan().create_kv_batch_scanner(bucket.clone())?;
            let first = scanner.next_batch().await?.expect("first page");
            assert!(first.num_rows() < 8);
            let pages = scanner.stats().pages_received;
            let id = scanner.scanner_id.clone().expect("server session is open");
            let server = scanner.connection.as_ref().unwrap();
            server.request(ScanKvRequest::new(Some(id), None, None, None, Some(true))).await?;
            let error = scanner.next_batch().await.expect_err("the server removed the session");
            assert!(matches!(error, Error::FlussAPIError { ref api_error } if api_error.code == FlussError::UnknownScannerId.code()));
            assert_eq!(scanner.stats().pages_received, pages);
            assert!(scanner.next_batch().await.is_err(), "must never restart silently");
            drop(scanner);

            let mut fresh = table.new_scan().create_kv_batch_scanner(bucket)?;
            let mut count = 0;
            while let Some(batch) = fresh.next_batch().await? {
                count += batch.num_rows();
            }
            assert_eq!(count, 8);
            Ok(())
        }.await;
        let cleanup = admin.drop_table(&path, true).await;
        check?;
        cleanup?;
        connection.close(Duration::from_secs(5)).await?;
        Ok(())
    }
}
