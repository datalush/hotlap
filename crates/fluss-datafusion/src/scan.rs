// SPDX-License-Identifier: Apache-2.0
//! Lazy per-partition read state: capture offsets, open reader, yield Arrow batches.

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
use fluss::client::{
    BoundedLogReadRange, EARLIEST_OFFSET, FlussConnection, FlussTable, RecordBatchLogReader,
    RecordBatchLogScanner, RecordBatchReadOutcome,
};
use fluss::metadata::{TableBucket, TablePath};
use fluss::predicate::Predicate;
use fluss::record::ScanBatch;
use fluss::rpc::message::OffsetSpec;

use crate::metrics::{PartitionMetrics, ReaderLifetime};
use crate::offsets::{Capture, OffsetCaptures, Offsets, validate_offsets};

const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Immutable inputs shared by the physical partitions of one planned scan.
pub(crate) struct ScanSpec {
    pub(crate) connection: Arc<FlussConnection>,
    pub(crate) path: TablePath,
    pub(crate) schema: SchemaRef,
    pub(crate) full_schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) filter: Option<Predicate>,
    pub(crate) table_id: i64,
    pub(crate) buckets: i32,
    pub(crate) timeout: Duration,
    pub(crate) captures: Arc<OffsetCaptures>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
}

impl ScanSpec {
    pub(crate) fn partitions(
        self: &Arc<Self>,
        groups: &[Vec<i32>],
    ) -> Vec<Arc<dyn PartitionStream>> {
        groups
            .iter()
            .enumerate()
            .map(|(index, bucket_ids)| {
                Arc::new(FlussPartition {
                    spec: Arc::clone(self),
                    bucket_ids: bucket_ids.clone(),
                    index,
                }) as Arc<dyn PartitionStream>
            })
            .collect()
    }
}

#[derive(Clone)]
struct FlussPartition {
    spec: Arc<ScanSpec>,
    bucket_ids: Vec<i32>,
    index: usize,
}

impl fmt::Debug for FlussPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussPartition")
            .field("path", &self.spec.path)
            .field("bucket_ids", &self.bucket_ids)
            .finish()
    }
}

impl PartitionStream for FlussPartition {
    fn schema(&self) -> &SchemaRef {
        &self.spec.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        ReadState::new(self.clone(), ctx).into_stream()
    }
}

struct ReadState {
    source: FlussPartition,
    reader: Option<RecordBatchLogReader>,
    deadline: Instant,
    capture: Capture,
    // Keep the execution identity alive for late partitions of this query.
    _context: Arc<TaskContext>,
    _lifetime: ReaderLifetime,
    metrics: PartitionMetrics,
}

impl ReadState {
    fn new(source: FlussPartition, ctx: Arc<TaskContext>) -> Self {
        let metrics = PartitionMetrics::new(&source.spec.metrics, source.index, &source.bucket_ids);
        let lifetime = ReaderLifetime::new(metrics.active_streams.clone());
        let capture = source.spec.captures.for_partition(&ctx, source.index);
        let deadline = Instant::now() + source.spec.timeout;
        Self {
            source,
            reader: None,
            deadline,
            capture,
            _context: ctx,
            _lifetime: lifetime,
            metrics,
        }
    }

    fn into_stream(self) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.source.spec.schema);
        let stream = futures::stream::try_unfold(self, |mut state| async move {
            state
                .next_batch()
                .await
                .map(|batch| batch.map(|batch| (batch, state)))
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.ensure_reader_started().await?;
        loop {
            match self.poll_reader().await? {
                RecordBatchReadOutcome::Batch(batch) => {
                    return self.prepare_output_batch(batch).map(Some);
                }
                RecordBatchReadOutcome::Finished => return Ok(None),
                RecordBatchReadOutcome::TimedOut => {}
            }
        }
    }

    async fn ensure_reader_started(&mut self) -> Result<()> {
        if self.reader.is_none() {
            let reader = tokio::time::timeout(self.remaining()?, self.start_reader())
                .await
                .map_err(|_| expired())??;
            self.reader = Some(reader);
        }
        Ok(())
    }

    async fn poll_reader(&mut self) -> Result<RecordBatchReadOutcome> {
        let remaining = self.remaining()?;
        // Inspect scanner progress regularly even when every fetched batch
        // was pruned. The total query deadline still bounds this loop.
        let poll_wait = remaining.min(POLL_INTERVAL);
        let result = {
            let _timer = self.metrics.read_time.timer();
            tokio::time::timeout(
                remaining,
                self.reader
                    .as_mut()
                    .expect("reader initialized")
                    .next_batch_with_timeout(poll_wait),
            )
            .await
            .map_err(|_| expired())?
        };
        result.map_err(fluss_error)
    }

    fn prepare_output_batch(&self, batch: ScanBatch) -> Result<RecordBatch> {
        let decoded = batch.into_batch();
        self.metrics.record_decoded_batch(&decoded);
        let output = if self
            .source
            .spec
            .projection
            .as_ref()
            .is_some_and(Vec::is_empty)
        {
            decoded.project(&[])?
        } else {
            decoded
        };
        self.metrics.record_output_batch(&output);
        Ok(output)
    }

    async fn start_reader(&self) -> Result<RecordBatchLogReader> {
        let offsets = self.shared_offsets().await?;
        let table = self.current_table().await?;
        let scanner = self.projected_scanner(&table)?;
        RecordBatchLogReader::new_from_ranges(scanner, self.ranges(&offsets))
            .await
            .map_err(fluss_error)
    }

    async fn shared_offsets(&self) -> Result<Offsets> {
        self.capture
            .get_or_init(|| async { self.capture_offsets().await.map_err(|e| e.to_string()) })
            .await
            .as_ref()
            .map(Arc::clone)
            .map_err(|error| DataFusionError::Execution(error.clone()))
    }

    async fn current_table(&self) -> Result<FlussTable<'_>> {
        let table = self
            .source
            .spec
            .connection
            .get_table(&self.source.spec.path)
            .await
            .map_err(fluss_error)?;
        let info = table.get_table_info();
        if info.table_id != self.source.spec.table_id
            || info.get_num_buckets() != self.source.spec.buckets
        {
            return Err(DataFusionError::Execution(
                "Fluss table topology changed; plan again".into(),
            ));
        }
        Ok(table)
    }

    fn projected_scanner(&self, table: &FlussTable<'_>) -> Result<RecordBatchLogScanner> {
        let scan = table.new_scan();
        let scan = match self.source.spec.projection.as_deref() {
            Some(indices) if !indices.is_empty() => scan.project(indices).map_err(fluss_error)?,
            _ => scan,
        };
        let scan = match &self.source.spec.filter {
            Some(predicate) => scan.filter(predicate.clone()).map_err(fluss_error)?,
            None => scan,
        };
        let scanner = scan
            .create_record_batch_log_scanner()
            .map_err(fluss_error)?;
        let expected_schema = if self
            .source
            .spec
            .projection
            .as_ref()
            .is_some_and(Vec::is_empty)
        {
            &self.source.spec.full_schema
        } else {
            &self.source.spec.schema
        };
        if scanner.schema() != *expected_schema {
            return Err(DataFusionError::Execution(
                "Fluss table schema changed; plan again".into(),
            ));
        }
        Ok(scanner)
    }

    fn ranges(&self, offsets: &Offsets) -> Vec<BoundedLogReadRange> {
        self.source
            .bucket_ids
            .iter()
            .map(|&bucket| BoundedLogReadRange {
                bucket: TableBucket::new(self.source.spec.table_id, bucket),
                starting_offset: EARLIEST_OFFSET,
                stopping_offset: offsets[&bucket],
            })
            .collect()
    }

    async fn capture_offsets(&self) -> Result<Offsets> {
        let _timer = self.metrics.capture_time.timer();
        let admin = self
            .source
            .spec
            .connection
            .get_admin()
            .map_err(fluss_error)?;
        let buckets: Vec<i32> = (0..self.source.spec.buckets).collect();
        let offsets = admin
            .list_offsets(&self.source.spec.path, &buckets, OffsetSpec::Latest)
            .await
            .map_err(fluss_error)?;
        validate_offsets(offsets, self.source.spec.buckets)
    }

    fn remaining(&self) -> Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|time| !time.is_zero())
            .ok_or_else(expired)
    }
}

fn expired() -> DataFusionError {
    DataFusionError::Execution("Fluss bounded log scan timed out; result is incomplete".into())
}

fn fluss_error(error: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
