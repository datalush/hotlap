// SPDX-License-Identifier: Apache-2.0
//! Lazy, per-partition KV snapshot sessions; a failed bucket fails the query.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use fluss::client::{FlussConnection, KvBatchScanner};
use fluss::metadata::{TableBucket, TablePath};
use fluss::record::to_arrow_schema;

use crate::metrics::{PartitionMetrics, ReaderLifetime};
use crate::offsets::{Capture, SharedCaptures};
use crate::partitions::{self, PartitionFilter, PartitionSelection};

pub(crate) struct KvScanSpec {
    pub(crate) connection: Arc<FlussConnection>,
    pub(crate) path: TablePath,
    pub(crate) schema: SchemaRef,
    pub(crate) full_schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) table_id: i64,
    pub(crate) buckets: i32,
    pub(crate) partitioned: bool,
    pub(crate) partition_filter: PartitionFilter,
    pub(crate) max_assigned_buckets: usize,
    pub(crate) partition_captures: Arc<SharedCaptures<PartitionSelection>>,
    pub(crate) timeout: Duration,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) deadlines: Arc<SharedCaptures<Instant>>,
}

impl KvScanSpec {
    pub(crate) fn partitions(
        self: &Arc<Self>,
        groups: &[Vec<i32>],
    ) -> Vec<Arc<dyn PartitionStream>> {
        groups
            .iter()
            .enumerate()
            .map(|(index, ids)| {
                Arc::new(KvPartition {
                    spec: Arc::clone(self),
                    bucket_ids: ids.clone(),
                    index,
                    group_count: groups.len(),
                }) as Arc<dyn PartitionStream>
            })
            .collect()
    }
}

#[derive(Clone)]
struct KvPartition {
    spec: Arc<KvScanSpec>,
    bucket_ids: Vec<i32>,
    index: usize,
    group_count: usize,
}

impl fmt::Debug for KvPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KvPartition")
            .field("path", &self.spec.path)
            .field("bucket_ids", &self.bucket_ids)
            .finish()
    }
}

impl PartitionStream for KvPartition {
    fn schema(&self) -> &SchemaRef {
        &self.spec.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let deadline =
            match self
                .spec
                .deadlines
                .deadline_for_partition(&ctx, self.index, self.spec.timeout)
            {
                Ok(deadline) => deadline,
                Err(error) => {
                    return Box::pin(RecordBatchStreamAdapter::new(
                        Arc::clone(self.schema()),
                        futures::stream::once(async { Err(error) }),
                    ));
                }
            };
        let metrics = PartitionMetrics::new(&self.spec.metrics, self.index, &self.bucket_ids);
        let reservation =
            MemoryConsumer::new("FlussKvScan").register(&ctx.runtime_env().memory_pool);
        let state = KvReadState {
            partition: self.clone(),
            buckets: None,
            partition_capture: self
                .spec
                .partitioned
                .then(|| self.spec.partition_captures.for_partition(&ctx, self.index)),
            next_bucket: 0,
            reader: None,
            first_page: false,
            deadline,
            _context: ctx,
            _lifetime: ReaderLifetime::new(metrics.active_streams.clone()),
            metrics,
            reservation,
        };
        let schema = Arc::clone(&self.spec.schema);
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            state
                .next_batch()
                .await
                .map(|batch| batch.map(|batch| (batch, state)))
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

struct KvReadState {
    partition: KvPartition,
    buckets: Option<Vec<(TableBucket, i32)>>,
    partition_capture: Option<Capture<PartitionSelection>>,
    next_bucket: usize,
    reader: Option<KvBatchScanner>,
    first_page: bool,
    deadline: Instant,
    _context: Arc<TaskContext>,
    _lifetime: ReaderLifetime,
    metrics: PartitionMetrics,
    reservation: MemoryReservation,
}

impl KvReadState {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.ensure_buckets().await?;
        loop {
            if self.reader.is_none() {
                let buckets = self.buckets.as_ref().expect("buckets initialized");
                if self.next_bucket == buckets.len() {
                    return Ok(None);
                }
                let (bucket, count) = buckets[self.next_bucket].clone();
                let remaining = self.remaining()?;
                let reader = tokio::time::timeout(remaining, self.open_bucket(bucket, count))
                    .await
                    .map_err(|_| expired())??;
                self.reader = Some(reader);
                self.first_page = true;
                self.next_bucket += 1;
            }
            let remaining = self.remaining()?;
            let before = self.reader.as_ref().unwrap().stats();
            let batch = {
                let _timer = self.metrics.read_time.timer();
                let _opening = self
                    .first_page
                    .then(|| self.metrics.kv_first_page_time.timer());
                tokio::time::timeout(remaining, self.reader.as_mut().unwrap().next_batch()).await
            };
            let after = self.reader.as_ref().unwrap().stats();
            self.metrics
                .kv_sessions_opened
                .add(after.sessions_opened - before.sessions_opened);
            self.metrics
                .kv_pages_received
                .add(after.pages_received - before.pages_received);
            let batch = batch.map_err(|_| expired())?.map_err(fluss_error)?;
            self.first_page = false;
            match batch {
                Some(batch) => {
                    self.metrics.record_decoded_batch(&batch);
                    let batch = crate::resources::reserve_batch(batch, &self.reservation)?;
                    let output = if self
                        .partition
                        .spec
                        .projection
                        .as_ref()
                        .is_some_and(Vec::is_empty)
                    {
                        batch.project(&[])?
                    } else {
                        batch
                    };
                    self.metrics.record_output_batch(&output);
                    return Ok(Some(output));
                }
                None => self.reader = None,
            }
        }
    }

    async fn ensure_buckets(&mut self) -> Result<()> {
        if self.buckets.is_some() {
            return Ok(());
        }
        let spec = &self.partition.spec;
        let group_count = self.partition.group_count;
        let group_index = self.partition.index;
        let partitions = if let Some(capture) = &self.partition_capture {
            let selected = tokio::time::timeout(
                self.remaining()?,
                capture.get_or_init(|| async {
                    partitions::discover(
                        &spec.connection,
                        &spec.path,
                        &spec.partition_filter,
                        spec.max_assigned_buckets,
                    )
                    .await
                    .map(Arc::new)
                    .map_err(|error| error.to_string())
                }),
            )
            .await
            .map_err(|_| expired())?;
            Some(
                selected
                    .as_ref()
                    .map(Arc::clone)
                    .map_err(|e| DataFusionError::Execution(e.clone()))?,
            )
        } else {
            None
        };
        self.buckets = Some(match partitions {
            Some(partitions) => {
                self.metrics.record_discovery(
                    &partitions.reported,
                    partitions.discovered,
                    partitions.selected.len(),
                );
                partitions
                    .selected
                    .iter()
                    .flat_map(|partition| {
                        let count = partition
                            .get_bucket_count()
                            .expect("partition layout validated");
                        (0..count)
                            .filter(move |&bucket| bucket as usize % group_count == group_index)
                            .map(move |bucket| {
                                (
                                    TableBucket::new_with_partition(
                                        spec.table_id,
                                        Some(partition.get_partition_id()),
                                        bucket,
                                    ),
                                    count,
                                )
                            })
                    })
                    .collect()
            }
            None => self
                .partition
                .bucket_ids
                .iter()
                .map(|&bucket| (TableBucket::new(spec.table_id, bucket), spec.buckets))
                .collect(),
        });
        self.metrics
            .buckets_assigned
            .add(self.buckets.as_ref().unwrap().len());
        Ok(())
    }

    async fn open_bucket(&self, bucket: TableBucket, count: i32) -> Result<KvBatchScanner> {
        let spec = &self.partition.spec;
        let table = spec
            .connection
            .get_table(&spec.path)
            .await
            .map_err(fluss_error)?;
        let info = table.get_table_info();
        if info.table_id != spec.table_id
            || (!spec.partitioned && info.get_num_buckets() != spec.buckets)
            || !info.has_primary_key()
            || info.is_partitioned() != spec.partitioned
            || to_arrow_schema(info.get_row_type())
                .map_err(fluss_error)?
                .as_ref()
                != spec.full_schema.as_ref()
        {
            return Err(DataFusionError::Execution(
                "Fluss KV table schema or topology changed; plan again".into(),
            ));
        }
        let scan = table.new_scan();
        let scan = match spec.projection.as_deref() {
            Some(indices) if !indices.is_empty() => scan.project(indices).map_err(fluss_error)?,
            _ => scan,
        };
        scan.create_kv_batch_scanner_with_bucket_count(bucket, count)
            .map_err(fluss_error)
    }

    fn remaining(&self) -> Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(expired)
    }
}

fn expired() -> DataFusionError {
    DataFusionError::External(Box::new(crate::FlussScanTimeout {
        read: crate::FlussReadCapability::KvSnapshot,
    }))
}

fn fluss_error(error: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
