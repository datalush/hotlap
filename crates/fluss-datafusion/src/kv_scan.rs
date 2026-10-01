// SPDX-License-Identifier: Apache-2.0
//! Lazy, per-partition KV snapshot sessions; a failed bucket fails the query.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use fluss::client::{FlussConnection, KvBatchScanner};
use fluss::metadata::{TableBucket, TablePath};
use fluss::record::to_arrow_schema;

use crate::metrics::{PartitionMetrics, ReaderLifetime};

pub(crate) struct KvScanSpec {
    pub(crate) connection: Arc<FlussConnection>,
    pub(crate) path: TablePath,
    pub(crate) schema: SchemaRef,
    pub(crate) full_schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) table_id: i64,
    pub(crate) buckets: i32,
    pub(crate) timeout: Duration,
    pub(crate) metrics: ExecutionPlanMetricsSet,
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
        let metrics = PartitionMetrics::new(&self.spec.metrics, self.index, &self.bucket_ids);
        let state = KvReadState {
            partition: self.clone(),
            next_bucket: 0,
            reader: None,
            deadline: Instant::now() + self.spec.timeout,
            _context: ctx,
            _lifetime: ReaderLifetime::new(metrics.active_streams.clone()),
            metrics,
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
    next_bucket: usize,
    reader: Option<KvBatchScanner>,
    deadline: Instant,
    _context: Arc<TaskContext>,
    _lifetime: ReaderLifetime,
    metrics: PartitionMetrics,
}

impl KvReadState {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.reader.is_none() {
                if self.next_bucket == self.partition.bucket_ids.len() {
                    return Ok(None);
                }
                let remaining = self.remaining()?;
                let reader = tokio::time::timeout(remaining, self.open_bucket())
                    .await
                    .map_err(|_| expired())??;
                self.reader = Some(reader);
                self.next_bucket += 1;
            }
            let remaining = self.remaining()?;
            let batch = {
                let _timer = self.metrics.read_time.timer();
                tokio::time::timeout(remaining, self.reader.as_mut().unwrap().next_batch())
                    .await
                    .map_err(|_| expired())?
                    .map_err(fluss_error)?
            };
            match batch {
                Some(batch) => {
                    self.metrics.record_decoded_batch(&batch);
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

    async fn open_bucket(&self) -> Result<KvBatchScanner> {
        let spec = &self.partition.spec;
        let table = spec
            .connection
            .get_table(&spec.path)
            .await
            .map_err(fluss_error)?;
        let info = table.get_table_info();
        if info.table_id != spec.table_id
            || info.get_num_buckets() != spec.buckets
            || !info.has_primary_key()
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
        scan.create_kv_batch_scanner(TableBucket::new(
            spec.table_id,
            self.partition.bucket_ids[self.next_bucket],
        ))
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
    DataFusionError::Execution("Fluss KV snapshot scan timed out".into())
}

fn fluss_error(error: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
