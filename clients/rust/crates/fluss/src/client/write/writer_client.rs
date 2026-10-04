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

use crate::bucketing::BucketingFunction;
use crate::client::metadata::Metadata;
use crate::client::write::IdempotenceManager;
use crate::client::write::broadcast;
use crate::client::write::bucket_assigner::{
    BucketAssigner, HashBucketAssigner, RoundRobinBucketAssigner, StickyBucketAssigner,
};
use crate::client::write::sender::Sender;
use crate::client::{RecordAccumulator, ResultHandle, WriteRecord};
use crate::cluster::Cluster;
use crate::config::Config;
use crate::config::NoKeyAssigner;
use crate::error::{Error, FlussError, Result};
use crate::metadata::{PhysicalTablePath, TableInfo};
use crate::metrics::WriterMetrics;
use crate::{BucketId, PartitionId};
use bytes::Bytes;
use dashmap::DashMap;
use log::warn;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[allow(dead_code)]
pub struct WriterClient {
    config: Config,
    max_request_size: i32,
    accumulate: Arc<RecordAccumulator>,
    shutdown_tx: Mutex<Option<mpsc::Sender<()>>>,
    sender_join_handle: Mutex<Option<JoinHandle<()>>>,
    metadata: Arc<Metadata>,
    bucket_assigners: DashMap<Arc<PhysicalTablePath>, BucketAssignerBinding>,
    idempotence_manager: Arc<IdempotenceManager>,
}

struct BucketAssignerBinding {
    partition_id: Option<PartitionId>,
    bucket_count: i32,
    assigner: Arc<dyn BucketAssigner>,
}

impl WriterClient {
    pub fn new(config: Config, metadata: Arc<Metadata>) -> Result<Self> {
        let ack = Self::get_ack(&config)?;

        let idempotence_manager = Arc::new(IdempotenceManager::new(
            config.writer_enable_idempotence,
            config.writer_max_inflight_requests_per_bucket,
        ));

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let accumulator = Arc::new(RecordAccumulator::new(
            config.clone(),
            Arc::clone(&idempotence_manager),
        ));

        // Writer metrics are emitted unlabeled (global per process). Resolve the
        // recorder once and share the cached handles with the sender.
        let metrics = Arc::new(WriterMetrics::new());

        let sender = Arc::new(Sender::new(
            metadata.clone(),
            accumulator.clone(),
            config.writer_request_max_size,
            30_000,
            ack,
            config.writer_retries,
            Arc::clone(&idempotence_manager),
            Arc::clone(&metrics),
        ));

        let join_handle = tokio::spawn(async move {
            if let Err(e) = sender.run_with_shutdown(shutdown_rx).await {
                warn!("Sender loop exited with error: {e}");
            }
        });

        Ok(Self {
            max_request_size: config.writer_request_max_size,
            config,
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
            sender_join_handle: Mutex::new(Some(join_handle)),
            accumulate: accumulator,
            metadata,
            bucket_assigners: Default::default(),
            idempotence_manager,
        })
    }

    fn get_ack(config: &Config) -> Result<i16> {
        let acks = config.writer_acks.as_str();
        if acks.eq_ignore_ascii_case("all") {
            Ok(-1)
        } else {
            acks.parse::<i16>().map_err(|e| Error::IllegalArgument {
                message: format!("invalid writer ack '{acks}': {e}"),
            })
        }
    }

    pub fn send(&self, record: &WriteRecord<'_>) -> Result<ResultHandle> {
        if self.accumulate.is_closed() {
            return Err(Error::WriterClosed {
                message: "Cannot send: writer is closed".to_string(),
            });
        }
        let physical_table_path = &record.physical_table_path;
        let cluster = self.metadata.get_cluster();
        // A dropped table is missing here once its metadata is evicted. Report the
        // same error its pending writes got, before the bucket assigner panics.
        let table_path = physical_table_path.get_table_path();
        cluster.get_table(table_path).map_err(|e| {
            if e.api_error() == Some(FlussError::InvalidTableException) {
                Error::table_not_exist(format!("Table not found: {table_path}"))
            } else {
                e
            }
        })?;
        let bucket_key = record.bucket_key.as_ref();

        let (bucket_assigner, bucket_id) = self.assign_bucket(
            &record.table_info,
            bucket_key,
            physical_table_path,
            &cluster,
        )?;

        let mut result = self.accumulate.append(
            record,
            bucket_id,
            &cluster,
            bucket_assigner.abort_if_batch_full(),
        )?;

        if result.abort_record_for_new_batch {
            let prev_bucket_id = bucket_id;
            bucket_assigner.on_new_batch(&cluster, prev_bucket_id);
            let bucket_id = bucket_assigner.assign_bucket(bucket_key, &cluster)?;
            result = self.accumulate.append(record, bucket_id, &cluster, false)?;
        }

        if result.batch_is_full || result.new_batch_created {
            self.accumulate.wakeup_sender();
        }

        Ok(result.result_handle.expect("result_handle should exist"))
    }
    fn assign_bucket(
        &self,
        table_info: &Arc<TableInfo>,
        bucket_key: Option<&Bytes>,
        table_path: &Arc<PhysicalTablePath>,
        cluster: &Arc<Cluster>,
    ) -> Result<(Arc<dyn BucketAssigner>, BucketId)> {
        let current = cluster.get_table(table_path.get_table_path())?;
        let partition_id = cluster.get_partition_id(table_path);
        let count = if current.is_partitioned() {
            match partition_id {
                Some(id) => cluster.routing_bucket_count(current.table_id, id)?,
                None if current.bucket_count_epoch == 0 => current.get_num_buckets(),
                None => {
                    return Err(Error::invalid_table(format!(
                        "Partition metadata for {table_path} is unavailable after bucket rescale"
                    )));
                }
            }
        } else {
            current.get_num_buckets()
        };
        let bucket_assigner = {
            if let Some(binding) = self.bucket_assigners.get(table_path)
                && binding.partition_id == partition_id
                && binding.bucket_count == count
            {
                Arc::clone(&binding.assigner)
            } else {
                let assigner = Self::create_bucket_assigner(
                    table_info,
                    Arc::clone(table_path),
                    &self.config,
                    count,
                )?;
                self.bucket_assigners.insert(
                    Arc::clone(table_path),
                    BucketAssignerBinding {
                        partition_id,
                        bucket_count: count,
                        assigner: Arc::clone(&assigner),
                    },
                );
                assigner
            }
        };
        let bucket_id = bucket_assigner.assign_bucket(bucket_key, cluster)?;
        Ok((bucket_assigner, bucket_id))
    }

    /// Close the writer with a timeout. Matches Java's two-phase shutdown:
    ///
    /// 1. **Graceful**: Signal the sender to drain all remaining batches.
    ///    `accumulator.close()` makes all batches immediately ready (no need
    ///    to wait for `batch_timeout_ms`).
    /// 2. **Force** (if timeout exceeded): Abort the sender task and fail
    ///    all remaining batches with an error.
    ///
    /// Idempotent: calling `close` a second time returns `Ok(())` immediately.
    pub async fn close(&self, timeout: Duration) -> Result<()> {
        // Take shutdown_tx and join_handle out of their Mutexes.
        // Second call sees None and returns early.
        let shutdown_tx = self.shutdown_tx.lock().take();
        let join_handle = self.sender_join_handle.lock().take();

        let Some(mut join_handle) = join_handle else {
            return Ok(());
        };

        // Phase 1: Signal graceful shutdown.
        // Mark accumulator closed so all batches become immediately sendable.
        self.accumulate.close();
        // Drop the shutdown sender — recv() returns None, breaking the sender loop.
        drop(shutdown_tx);

        // Phase 2: Wait for graceful drain, bounded by timeout.
        tokio::select! {
            result = &mut join_handle => {
                if let Err(e) = result {
                    warn!("Sender task panicked during shutdown: {e}");
                }
            }
            _ = tokio::time::sleep(timeout) => {
                // Phase 3: Force close — timeout exceeded.
                warn!("Graceful shutdown timed out after {timeout:?}, force closing");
                join_handle.abort();
                let _ = join_handle.await; // Wait for cancellation to complete
                self.accumulate.abort_batches(broadcast::Error::Client {
                    message: "Writer force closed (shutdown timeout exceeded)".to_string(),
                });
            }
        }
        Ok(())
    }

    /// Stop producing immediately and wake producers waiting for buffer space.
    /// Already sent requests may still be applied by the server: this is not
    /// rollback. Intended for cancellation of a dedicated writer session.
    pub fn abort(&self) {
        self.accumulate.close();
        if let Some(handle) = self.sender_join_handle.lock().take() {
            handle.abort();
        }
        self.shutdown_tx.lock().take();
        self.accumulate.abort_batches(broadcast::Error::Client {
            message: "Writer aborted; in-flight writes may have been committed".into(),
        });
    }

    pub async fn flush(&self) -> Result<()> {
        self.accumulate.begin_flush()?;
        self.accumulate.await_flush_completion().await?;
        Ok(())
    }

    #[cfg(feature = "integration_tests")]
    pub fn estimated_batch_size_for_table(
        &self,
        table_path: &Arc<PhysicalTablePath>,
    ) -> Option<usize> {
        self.accumulate.estimated_batch_size(table_path)
    }

    pub fn create_bucket_assigner(
        table_info: &Arc<TableInfo>,
        table_path: Arc<PhysicalTablePath>,
        config: &Config,
        bucket_count: i32,
    ) -> Result<Arc<dyn BucketAssigner>> {
        // Decide from the table's bucket key, not an individual record's key.
        if table_info.has_bucket_key() {
            let datalake_format = table_info.get_table_config().get_datalake_format()?;
            let function = <dyn BucketingFunction>::of(datalake_format.as_ref());
            Ok(Arc::new(HashBucketAssigner::new(bucket_count, function)))
        } else {
            match config.writer_bucket_no_key_assigner {
                NoKeyAssigner::Sticky => Ok(Arc::new(StickyBucketAssigner::new(
                    table_path,
                    bucket_count,
                ))),
                NoKeyAssigner::RoundRobin => Ok(Arc::new(RoundRobinBucketAssigner::new(
                    table_path,
                    bucket_count,
                ))),
            }
        }
    }
}
