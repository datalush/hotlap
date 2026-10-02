// SPDX-License-Identifier: Apache-2.0
//! Lazy per-partition read state: capture offsets, open reader, yield Arrow batches.

use std::collections::HashMap;
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
use fluss::client::{
    BoundedLogReadRange, FlussConnection, FlussTable, RecordBatchLogReader, RecordBatchLogScanner,
    RecordBatchReadOutcome,
};
use fluss::metadata::{TableBucket, TablePath};
use fluss::predicate::Predicate;
use fluss::record::ScanBatch;
use fluss::rpc::message::OffsetSpec;

use crate::metrics::{PartitionMetrics, ReaderLifetime, ReportOnce};
use crate::offsets::{
    Capture, OffsetWindow, SharedCaptures, ensure_retained, validate_offsets, validate_window,
};
use crate::partitions::{self, PartitionFilter};

const POLL_INTERVAL: Duration = Duration::from_millis(500);

pub(crate) struct PartitionedOffsets {
    discovered: usize,
    partitions: Vec<(i64, i32, Arc<OffsetWindow>)>,
    reported: ReportOnce,
}

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
    pub(crate) partitioned: bool,
    pub(crate) partition_filter: PartitionFilter,
    pub(crate) max_assigned_buckets: usize,
    pub(crate) partition_captures: Arc<SharedCaptures<PartitionedOffsets>>,
    pub(crate) timeout: Duration,
    pub(crate) captures: Arc<SharedCaptures<OffsetWindow>>,
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
                    group_count: groups.len(),
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
    group_count: usize,
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
    capture: Capture<OffsetWindow>,
    partition_capture: Option<Capture<PartitionedOffsets>>,
    // Keep the execution identity alive for late partitions of this query.
    _context: Arc<TaskContext>,
    _lifetime: ReaderLifetime,
    metrics: PartitionMetrics,
    reservation: MemoryReservation,
}

impl ReadState {
    fn new(source: FlussPartition, ctx: Arc<TaskContext>) -> Self {
        let metrics = PartitionMetrics::new(&source.spec.metrics, source.index, &source.bucket_ids);
        let lifetime = ReaderLifetime::new(metrics.active_streams.clone());
        let reservation =
            MemoryConsumer::new("FlussLogScan").register(&ctx.runtime_env().memory_pool);
        let capture = source.spec.captures.for_partition(&ctx, source.index);
        let partition_capture = source.spec.partitioned.then(|| {
            source
                .spec
                .partition_captures
                .for_partition(&ctx, source.index)
        });
        let deadline = Instant::now() + source.spec.timeout;
        Self {
            source,
            reader: None,
            deadline,
            capture,
            partition_capture,
            _context: ctx,
            _lifetime: lifetime,
            metrics,
            reservation,
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
        // DataFusion cannot request another source batch until it consumes the
        // previous one. Keep that batch charged to the shared query pool until
        // the next pull (or stream cancellation/drop).
        self.reservation.free();
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
        self.reservation
            .try_resize(decoded.get_array_memory_size())?;
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
        let ranges = self.ranges().await?;
        let table = self.current_table().await?;
        self.ensure_retention_before_start(&ranges).await?;
        let scanner = self.projected_scanner(&table)?;
        if let Some(capture) = &self.partition_capture {
            let snapshot = capture
                .get()
                .expect("partition offsets captured")
                .as_ref()
                .map_err(|error| DataFusionError::Execution(error.clone()))?;
            let counts = snapshot
                .partitions
                .iter()
                .map(|(id, count, _)| (*id, *count))
                .collect();
            RecordBatchLogReader::new_from_ranges_with_bucket_counts(scanner, ranges, counts)
                .await
                .map_err(fluss_error)
        } else {
            RecordBatchLogReader::new_from_ranges(scanner, ranges)
                .await
                .map_err(fluss_error)
        }
    }

    async fn ensure_retention_before_start(&self, ranges: &[BoundedLogReadRange]) -> Result<()> {
        let active: Vec<_> = ranges
            .iter()
            .filter(|range| range.starting_offset < range.stopping_offset)
            .collect();
        if active.is_empty() {
            return Ok(());
        }
        let spec = &self.source.spec;
        let admin = spec.connection.get_admin().map_err(fluss_error)?;
        if !spec.partitioned {
            let buckets: Vec<_> = active
                .iter()
                .map(|range| range.bucket.bucket_id())
                .collect();
            let current = admin
                .list_offsets(&spec.path, &buckets, OffsetSpec::Earliest)
                .await
                .map_err(fluss_error)?;
            for range in active {
                ensure_retained(
                    range.starting_offset,
                    current[&range.bucket.bucket_id()],
                    range.bucket.bucket_id(),
                )?;
            }
        } else {
            let snapshot = self
                .partition_capture
                .as_ref()
                .unwrap()
                .get()
                .expect("partition offsets captured")
                .as_ref()
                .map_err(|error| DataFusionError::Execution(error.clone()))?;
            let counts: HashMap<_, _> = snapshot
                .partitions
                .iter()
                .map(|(id, count, _)| (*id, *count))
                .collect();
            let mut by_partition: HashMap<i64, Vec<&BoundedLogReadRange>> = HashMap::new();
            for range in active {
                by_partition
                    .entry(range.bucket.partition_id().expect("partitioned bucket"))
                    .or_default()
                    .push(range);
            }
            for (id, partition_ranges) in by_partition {
                let buckets: Vec<_> = partition_ranges
                    .iter()
                    .map(|range| range.bucket.bucket_id())
                    .collect();
                let current = admin
                    .list_partition_offsets_by_id(
                        &spec.path,
                        id,
                        counts[&id],
                        &buckets,
                        OffsetSpec::Earliest,
                    )
                    .await
                    .map_err(fluss_error)?;
                for range in partition_ranges {
                    ensure_retained(
                        range.starting_offset,
                        current[&range.bucket.bucket_id()],
                        range.bucket.bucket_id(),
                    )?;
                }
            }
        }
        Ok(())
    }

    async fn shared_offsets(&self) -> Result<Arc<OffsetWindow>> {
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
            || (!self.source.spec.partitioned && info.get_num_buckets() != self.source.spec.buckets)
            || info.is_partitioned() != self.source.spec.partitioned
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

    async fn ranges(&self) -> Result<Vec<BoundedLogReadRange>> {
        let partitions = if let Some(capture) = &self.partition_capture {
            Some(
                capture
                    .get_or_init(|| async {
                        self.capture_partitioned_offsets()
                            .await
                            .map(Arc::new)
                            .map_err(|error| error.to_string())
                    })
                    .await
                    .as_ref()
                    .map(Arc::clone)
                    .map_err(|error| DataFusionError::Execution(error.clone()))?,
            )
        } else {
            None
        };
        let ranges: Vec<BoundedLogReadRange> = match partitions {
            Some(snapshot) => {
                self.metrics.record_discovery(
                    &snapshot.reported,
                    snapshot.discovered,
                    snapshot.partitions.len(),
                );
                snapshot
                    .partitions
                    .iter()
                    .flat_map(|(partition_id, count, window)| {
                        (0..*count)
                            .filter(move |&bucket| {
                                bucket as usize % self.source.group_count == self.source.index
                            })
                            .map(move |bucket| BoundedLogReadRange {
                                bucket: TableBucket::new_with_partition(
                                    self.source.spec.table_id,
                                    Some(*partition_id),
                                    bucket,
                                ),
                                starting_offset: window.earliest[&bucket],
                                stopping_offset: window.latest[&bucket],
                            })
                    })
                    .collect()
            }
            None => {
                let window = self.shared_offsets().await?;
                self.source
                    .bucket_ids
                    .iter()
                    .map(|&bucket| BoundedLogReadRange {
                        bucket: TableBucket::new(self.source.spec.table_id, bucket),
                        starting_offset: window.earliest[&bucket],
                        stopping_offset: window.latest[&bucket],
                    })
                    .collect()
            }
        };
        self.metrics.buckets_assigned.add(ranges.len());
        Ok(ranges)
    }

    async fn capture_partitioned_offsets(&self) -> Result<PartitionedOffsets> {
        let _timer = self.metrics.capture_time.timer();
        let spec = &self.source.spec;
        let selection = partitions::discover(
            &spec.connection,
            &spec.path,
            &spec.partition_filter,
            spec.max_assigned_buckets,
        )
        .await?;
        let discovered = selection.discovered;
        let admin = spec.connection.get_admin().map_err(fluss_error)?;
        let mut captured = Vec::with_capacity(selection.selected.len());
        for partition in selection.selected {
            let count = partition
                .get_bucket_count()
                .expect("partition layout validated");
            let buckets: Vec<_> = (0..count).collect();
            let earliest = admin
                .list_partition_offsets_by_id(
                    &spec.path,
                    partition.get_partition_id(),
                    count,
                    &buckets,
                    OffsetSpec::Earliest,
                )
                .await
                .map_err(fluss_error)?;
            let latest = admin
                .list_partition_offsets_by_id(
                    &spec.path,
                    partition.get_partition_id(),
                    count,
                    &buckets,
                    OffsetSpec::Latest,
                )
                .await
                .map_err(fluss_error)?;
            let window = validate_window(
                validate_offsets(earliest, count)?,
                validate_offsets(latest, count)?,
            )?;
            captured.push((partition.get_partition_id(), count, Arc::new(window)));
        }
        Ok(PartitionedOffsets {
            discovered,
            partitions: captured,
            reported: ReportOnce::default(),
        })
    }

    async fn capture_offsets(&self) -> Result<Arc<OffsetWindow>> {
        let _timer = self.metrics.capture_time.timer();
        let admin = self
            .source
            .spec
            .connection
            .get_admin()
            .map_err(fluss_error)?;
        let buckets: Vec<i32> = (0..self.source.spec.buckets).collect();
        let earliest = admin
            .list_offsets(&self.source.spec.path, &buckets, OffsetSpec::Earliest)
            .await
            .map_err(fluss_error)?;
        let latest = admin
            .list_offsets(&self.source.spec.path, &buckets, OffsetSpec::Latest)
            .await
            .map_err(fluss_error)?;
        Ok(Arc::new(validate_window(
            validate_offsets(earliest, self.source.spec.buckets)?,
            validate_offsets(latest, self.source.spec.buckets)?,
        )?))
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
