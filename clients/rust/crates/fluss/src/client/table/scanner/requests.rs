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

//! Resolve bucket leaders, build fetch requests and dispatch their RPCs.

use super::{
    Arc, Error, FetchLogRequest, FetchResponseContext, FlussError, HashMap, HashSet, Instant,
    LogFetcher, PbFetchLogReqForBucket, PbFetchLogReqForTable, Result, RpcError, TableBucket,
    debug, message, warn,
};

impl LogFetcher {
    async fn check_and_update_metadata(&self, table_buckets: &[TableBucket]) -> Result<()> {
        let mut partition_ids = Vec::new();
        let mut need_update = false;

        for tb in table_buckets {
            if self.get_table_bucket_leader(tb).is_some() {
                continue;
            }

            if self.is_partitioned {
                partition_ids.push(tb.partition_id().unwrap());
            } else {
                need_update = true;
                break;
            }
        }

        let update_result = if self.is_partitioned && !partition_ids.is_empty() {
            self.metadata
                .update_tables_metadata(
                    &HashSet::from([&self.table_path]),
                    &HashSet::new(),
                    partition_ids,
                )
                .await
        } else if need_update {
            self.metadata.update_table_metadata(&self.table_path).await
        } else {
            Ok(())
        };

        update_result.or_else(|error| {
            if matches!(error.api_error(), Some(FlussError::PartitionNotExists)) {
                warn!(
                    "Received PartitionNotExists while updating scanner metadata; ignoring it: {error}"
                );
                Ok(())
            } else if let Error::RpcError { source, .. } = &error
                && matches!(source, RpcError::ConnectionError(_) | RpcError::Poisoned(_))
            {
                warn!(
                    "Retrying after encountering error while updating table metadata: {error}"
                );
                Ok(())
            } else {
                Err(error)
            }
        })?;
        Ok(())
    }

    /// Send fetch requests asynchronously without waiting for responses
    pub(super) async fn send_fetches(&self) -> Result<()> {
        self.check_and_update_metadata(self.fetchable_buckets().as_slice())
            .await?;
        let fetch_request = self.prepare_fetch_log_requests().await;

        for (leader, fetch_request) in fetch_request {
            debug!("Adding pending request for node id {leader}");
            // Check if we already have a pending request for this node
            {
                self.nodes_with_pending_fetch_requests.lock().insert(leader);
            }

            let cluster = self.metadata.get_cluster().clone();

            let conns = Arc::clone(&self.conns);
            let log_fetch_buffer = self.log_fetch_buffer.clone();
            let log_scanner_status = self.log_scanner_status.clone();
            let resolver = Arc::clone(&self.resolver);
            let remote_log_downloader = Arc::clone(&self.remote_log_downloader);
            let nodes_with_pending = self.nodes_with_pending_fetch_requests.clone();
            let metadata = self.metadata.clone();
            let metrics = Arc::clone(&self.metrics);
            // Spawn async task to handle the fetch request
            // Note: These tasks are not explicitly tracked or cancelled when LogFetcher is dropped.
            // This is acceptable because:
            // 1. Tasks will naturally complete (network requests will return or timeout)
            // 2. Tasks use Arc references, so resources are properly shared
            // 3. When the program exits, tokio runtime will clean up all tasks
            // 4. Tasks are short-lived (network I/O operations)
            tokio::spawn(async move {
                // make sure it will always remove leader from pending nodes
                let _guard = scopeguard::guard((), |_| {
                    nodes_with_pending.lock().remove(&leader);
                });

                let server_node = match cluster.get_tablet_server(leader) {
                    Some(node) => node,
                    None => {
                        warn!("No server node found for leader {leader}, retrying");
                        Self::handle_fetch_failure(metadata, &leader, &fetch_request).await;
                        return;
                    }
                };

                let con = match conns.get_connection(server_node).await {
                    Ok(con) => con,
                    Err(e) => {
                        warn!("Retrying after error getting connection to destination node: {e:?}");
                        Self::handle_fetch_failure(metadata, &leader, &fetch_request).await;
                        return;
                    }
                };

                // Java increment the fetch counter and capture `requestStartTime` immediately
                // before the RPC. Failed connection acquisition above is not counted.
                let request_start_time = Instant::now();
                metrics.record_fetch_request();

                let fetch_response = match con
                    .request(message::FetchLogRequest::new(fetch_request.clone()))
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        warn!(
                            "Retrying after error fetching log from destination node {server_node:?}: {e:?}"
                        );
                        Self::handle_fetch_failure(metadata, &leader, &fetch_request).await;
                        return;
                    }
                };

                // Build the context after the RPC so `request_start_time` measures only RPC wall-clock
                // — not tablet-server lookup or connection acquisition, which is matching Java's bebaviour
                // Building it here also skips the allocation on the early-return error paths above.
                let response_context = FetchResponseContext {
                    metadata: metadata.clone(),
                    log_fetch_buffer,
                    log_scanner_status,
                    resolver,
                    remote_log_downloader,
                    metrics,
                    request_start_time,
                };
                Self::handle_fetch_response(fetch_response, response_context).await;
            });
        }

        Ok(())
    }

    pub(super) async fn prepare_fetch_log_requests(&self) -> HashMap<i32, FetchLogRequest> {
        let mut fetch_log_req_for_buckets = HashMap::new();
        let mut table_id = None;
        let mut ready_for_fetch_count = 0;
        for bucket in self.fetchable_buckets() {
            if table_id.is_none() {
                table_id = Some(bucket.table_id());
            }

            let offset = match self.log_scanner_status.get_bucket_offset(&bucket) {
                Some(offset) => offset,
                None => {
                    debug!(
                        "Skipping fetch request for bucket {bucket} because the bucket has been unsubscribed."
                    );
                    continue;
                }
            };

            match self.get_table_bucket_leader(&bucket) {
                None => {
                    log::trace!(
                        "Skipping fetch request for bucket {bucket} because leader is not available."
                    )
                }
                Some(leader) => {
                    if self
                        .nodes_with_pending_fetch_requests
                        .lock()
                        .contains(&leader)
                    {
                        log::trace!(
                            "Skipping fetch request for bucket {bucket} because previous request to server {leader} has not been processed."
                        )
                    } else {
                        let fetch_log_req_for_bucket = PbFetchLogReqForBucket {
                            partition_id: bucket.partition_id(),
                            bucket_id: bucket.bucket_id(),
                            fetch_offset: offset,
                            max_fetch_bytes: self.fetch_max_bytes_for_bucket,
                            routing_bucket_count: bucket.partition_id().and_then(|id| {
                                self.partition_bucket_counts.read().get(&id).copied()
                            }),
                        };

                        fetch_log_req_for_buckets
                            .entry(leader)
                            .or_insert_with(Vec::new)
                            .push(fetch_log_req_for_bucket);
                        ready_for_fetch_count += 1;
                    }
                }
            }
        }

        if ready_for_fetch_count == 0 {
            HashMap::new()
        } else {
            let initial_ctx = self
                .resolver
                .resolve(self.resolver.initial_schema_id(), false)
                .expect("initial ReadContext must exist");
            let (projection_enabled, projected_fields) = match initial_ctx.project_fields_in_order()
            {
                None => (false, vec![]),
                Some(fields) => (true, fields.iter().map(|&i| i as i32).collect()),
            };

            fetch_log_req_for_buckets
                .into_iter()
                .map(|(leader_id, feq_for_buckets)| {
                    let req_for_table = PbFetchLogReqForTable {
                        table_id: table_id.unwrap(),
                        projection_pushdown_enabled: projection_enabled,
                        projected_fields: projected_fields.clone(),
                        buckets_req: feq_for_buckets,
                        // The proto requires both filter fields to be set together.
                        filter_predicate: self.filter.as_ref().map(|(p, _)| p.clone()),
                        filter_schema_id: self.filter.as_ref().map(|&(_, id)| id),
                    };

                    let fetch_log_request = FetchLogRequest {
                        follower_server_id: -1,
                        max_bytes: self.fetch_max_bytes,
                        tables_req: vec![req_for_table],
                        max_wait_ms: Some(self.fetch_wait_max_time_ms),
                        min_bytes: Some(self.fetch_min_bytes),
                        read_preference: None,
                    };
                    (leader_id, fetch_log_request)
                })
                .collect()
        }
    }

    fn fetchable_buckets(&self) -> Vec<TableBucket> {
        // Get buckets that are not already in the buffer
        let buffered = self.log_fetch_buffer.buffered_buckets();
        let buffered_set: HashSet<TableBucket> = buffered.into_iter().collect();
        self.log_scanner_status
            .fetchable_buckets(|tb| !buffered_set.contains(tb))
    }

    fn get_table_bucket_leader(&self, tb: &TableBucket) -> Option<i32> {
        let cluster = self.metadata.get_cluster();
        cluster.leader_for(tb).map(|leader| leader.id())
    }
}
