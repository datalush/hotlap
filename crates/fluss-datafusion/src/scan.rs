// SPDX-License-Identifier: Apache-2.0
//! Lazy per-partition read state: capture offsets, open reader, yield Arrow batches.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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

use crate::log_options::{LogReadMode, LogReadOptions, LogStart};
use crate::log_progress::{
    LogDelivery, LogProgress, LogReadPosition, LogTermination, safe_frontier,
};
use crate::metrics::{PartitionMetrics, ReaderLifetime, ReportOnce};
use crate::offsets::{
    Capture, OffsetWindow, SharedCaptures, ensure_retained, validate_offsets, validate_window,
};
use crate::partitions::{self, PartitionFilter};

const POLL_INTERVAL: Duration = Duration::from_millis(500);
static NEXT_EXECUTION_ID: AtomicU64 = AtomicU64::new(1);

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
    pub(crate) schema_id: i32,
    pub(crate) project_at_source: bool,
    pub(crate) buckets: i32,
    pub(crate) partitioned: bool,
    pub(crate) partition_filter: PartitionFilter,
    pub(crate) max_assigned_buckets: usize,
    pub(crate) partition_captures: Arc<SharedCaptures<PartitionedOffsets>>,
    pub(crate) options: LogReadOptions,
    pub(crate) captures: Arc<SharedCaptures<OffsetWindow>>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) deliveries: tokio::sync::broadcast::Sender<LogDelivery>,
    pub(crate) progress: tokio::sync::broadcast::Sender<LogProgress>,
    pub(crate) execution_ids: Arc<SharedCaptures<u64>>,
    pub(crate) deadlines: Arc<SharedCaptures<Instant>>,
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
        match ReadState::new(self.clone(), ctx) {
            Ok(state) => state.into_stream(),
            Err(error) => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(self.schema()),
                futures::stream::once(async { Err(error) }),
            )),
        }
    }
}

struct ReadState {
    source: FlussPartition,
    reader: Option<ActiveReader>,
    deadline: Option<Instant>,
    last_topology_check: Instant,
    capture: Capture<OffsetWindow>,
    execution_id: u64,
    positions: HashMap<TableBucket, (i64, Option<i64>)>,
    terminal: bool,
    partition_capture: Option<Capture<PartitionedOffsets>>,
    // Keep the execution identity alive for late partitions of this query.
    _context: Arc<TaskContext>,
    _lifetime: ReaderLifetime,
    metrics: PartitionMetrics,
    reservation: MemoryReservation,
}

enum ActiveReader {
    Bounded(RecordBatchLogReader),
    Streaming(RecordBatchLogScanner, VecDeque<ScanBatch>),
    EmptyStreaming,
}

impl ActiveReader {
    fn buffered_arrow_bytes(&self) -> usize {
        match self {
            Self::Bounded(reader) => reader.buffered_arrow_bytes(),
            Self::Streaming(_, buffer) => buffer
                .iter()
                .map(|batch| batch.batch().get_array_memory_size())
                .sum(),
            Self::EmptyStreaming => 0,
        }
    }

    async fn poll(&mut self, wait: Duration) -> Result<RecordBatchReadOutcome> {
        match self {
            Self::Bounded(reader) => reader
                .next_batch_with_timeout(wait)
                .await
                .map_err(fluss_error),
            Self::Streaming(scanner, buffer) => {
                if let Some(batch) = buffer.pop_front() {
                    return Ok(RecordBatchReadOutcome::Batch(batch));
                }
                buffer.extend(scanner.poll(wait).await.map_err(fluss_error)?);
                Ok(buffer.pop_front().map_or(
                    RecordBatchReadOutcome::TimedOut,
                    RecordBatchReadOutcome::Batch,
                ))
            }
            Self::EmptyStreaming => {
                tokio::time::sleep(wait).await;
                Ok(RecordBatchReadOutcome::TimedOut)
            }
        }
    }
}

impl ReadState {
    fn new(source: FlussPartition, ctx: Arc<TaskContext>) -> Result<Self> {
        let metrics = PartitionMetrics::new(&source.spec.metrics, source.index, &source.bucket_ids);
        let lifetime = ReaderLifetime::new(metrics.active_streams.clone());
        let reservation =
            MemoryConsumer::new("FlussLogScan").register(&ctx.runtime_env().memory_pool);
        let capture = source.spec.captures.for_partition(&ctx, source.index);
        let execution_capture = source.spec.execution_ids.for_partition(&ctx, source.index);
        if execution_capture.get().is_none() {
            let _ = execution_capture.set(Ok(Arc::new(
                NEXT_EXECUTION_ID.fetch_add(1, Ordering::Relaxed),
            )));
        }
        let execution_id = **execution_capture
            .get()
            .expect("execution initialized")
            .as_ref()
            .map_err(|error| DataFusionError::Execution(error.clone()))?;
        let partition_capture = source.spec.partitioned.then(|| {
            source
                .spec
                .partition_captures
                .for_partition(&ctx, source.index)
        });
        let deadline = (source.spec.options.mode == LogReadMode::Batch)
            .then(|| {
                source.spec.deadlines.deadline_for_partition(
                    &ctx,
                    source.index,
                    source.spec.options.batch_timeout,
                )
            })
            .transpose()?;
        Ok(Self {
            source,
            reader: None,
            deadline,
            last_topology_check: Instant::now(),
            capture,
            execution_id,
            positions: HashMap::new(),
            terminal: false,
            partition_capture,
            _context: ctx,
            _lifetime: lifetime,
            metrics,
            reservation,
        })
    }

    fn into_stream(self) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.source.spec.schema);
        let stream = futures::stream::try_unfold(self, |mut state| async move {
            match state.next_batch().await {
                Ok(Some(batch)) => Ok(Some((batch, state))),
                Ok(None) => {
                    state.terminate(LogTermination::Completed);
                    Ok(None)
                }
                Err(error) => {
                    state.terminate(LogTermination::Failed);
                    Err(error)
                }
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Queue bytes remain charged until the reader relinquishes them.
        // Offered batches have independent leases attached to Arrow buffers.
        self.ensure_reader_started().await?;
        loop {
            match self.poll_reader().await? {
                RecordBatchReadOutcome::Batch(batch) => {
                    return self.prepare_output_batch(batch).map(Some);
                }
                RecordBatchReadOutcome::Finished => {
                    self.publish_excluded();
                    return Ok(None);
                }
                RecordBatchReadOutcome::TimedOut => self.publish_excluded(),
            }
        }
    }

    async fn ensure_reader_started(&mut self) -> Result<()> {
        if self.reader.is_none() {
            let timeout = self.remaining()?.unwrap_or_else(|| {
                Duration::from_millis(
                    self.source
                        .spec
                        .connection
                        .config()
                        .scanner_remote_log_operation_timeout_ms,
                )
            });
            let (reader, ranges) = tokio::time::timeout(timeout, async {
                let ranges = self.ranges().await?;
                let reader = self.start_reader(ranges.clone()).await?;
                Ok::<_, DataFusionError>((reader, ranges))
            })
            .await
            .map_err(|_| {
                if self.deadline.is_some() {
                    expired()
                } else {
                    DataFusionError::Execution(
                        "Fluss streaming log source initialization timed out".into(),
                    )
                }
            })??;
            self.reader = Some(reader);
            let positions: Vec<_> = ranges
                .into_iter()
                .map(|range| LogReadPosition {
                    bucket: range.bucket,
                    start_offset: range.starting_offset,
                    stop_offset: (self.source.spec.options.mode == LogReadMode::Batch)
                        .then_some(range.stopping_offset),
                })
                .collect();
            self.positions = positions
                .iter()
                .map(|position| {
                    (
                        position.bucket.clone(),
                        (position.start_offset, position.stop_offset),
                    )
                })
                .collect();
            let _ = self.source.spec.progress.send(LogProgress::Initialized {
                execution_id: self.execution_id,
                table_id: self.source.spec.table_id,
                schema_id: self.source.spec.schema_id,
                partition: self.source.index,
                partitions: self.source.group_count,
                positions,
            });
        }
        Ok(())
    }

    async fn poll_reader(&mut self) -> Result<RecordBatchReadOutcome> {
        let remaining = self.remaining()?;
        if self.source.spec.options.mode == LogReadMode::Streaming
            && self.last_topology_check.elapsed() >= Duration::from_secs(5)
        {
            self.check_streaming_topology().await?;
            self.last_topology_check = Instant::now();
        }
        // Inspect scanner progress regularly even when every fetched batch
        // was pruned. The total query deadline still bounds this loop.
        let poll_wait = remaining.map_or(POLL_INTERVAL, |remaining| remaining.min(POLL_INTERVAL));
        let _timer = self.metrics.read_time.timer();
        let poll = self
            .reader
            .as_mut()
            .expect("reader initialized")
            .poll(poll_wait);
        let result = if let Some(remaining) = remaining {
            tokio::time::timeout(remaining, poll)
                .await
                .map_err(|_| expired())?
        } else {
            poll.await
        }?;
        self.reservation
            .try_resize(self.reader.as_ref().unwrap().buffered_arrow_bytes())?;
        Ok(result)
    }

    fn prepare_output_batch(&mut self, batch: ScanBatch) -> Result<RecordBatch> {
        let bucket = batch.bucket().clone();
        let base_offset = batch.base_offset();
        let next_offset = batch.last_offset().checked_add(1).ok_or_else(|| {
            DataFusionError::Execution(
                "Fluss offered offset overflows the log position range".into(),
            )
        })?;
        let rows = batch.num_records();
        let decoded = batch.into_batch();
        self.metrics.record_decoded_batch(&decoded);
        let decoded = crate::resources::reserve_batch(decoded, &self.reservation)?;
        let output = match &self.source.spec.projection {
            Some(indices) if indices.is_empty() || !self.source.spec.project_at_source => {
                decoded.project(indices)?
            }
            _ => decoded,
        };
        self.metrics.record_output_batch(&output);
        // The source has handed the batch to DataFusion; the engine must still
        // acknowledge its own processing and output before checkpointing it.
        let position = self.positions.get_mut(&bucket).ok_or_else(|| {
            DataFusionError::Execution("Fluss offered an unassigned log bucket".into())
        })?;
        if base_offset < position.0
            || next_offset < base_offset
            || position.1.is_some_and(|stop| next_offset > stop)
        {
            return Err(DataFusionError::Execution(
                "Fluss offered log range regressed or exceeded the captured stop".into(),
            ));
        }
        if base_offset > position.0 {
            let _ = self.source.spec.progress.send(LogProgress::Excluded {
                execution_id: self.execution_id,
                bucket: bucket.clone(),
                base_offset: position.0,
                next_offset: base_offset,
            });
        }
        position.0 = next_offset;
        let delivery = LogDelivery {
            execution_id: self.execution_id,
            bucket,
            base_offset,
            next_offset,
            rows,
        };
        let _ = self.source.spec.deliveries.send(delivery.clone());
        let _ = self
            .source
            .spec
            .progress
            .send(LogProgress::Offered(delivery));
        self.publish_excluded();
        Ok(output)
    }

    async fn start_reader(&self, ranges: Vec<BoundedLogReadRange>) -> Result<ActiveReader> {
        let table = self.current_table().await?;
        self.ensure_retention_before_start(&ranges).await?;
        let scanner = self.projected_scanner(&table)?;
        if self.source.spec.options.mode == LogReadMode::Streaming {
            if ranges.is_empty() {
                return Ok(ActiveReader::EmptyStreaming);
            }
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
                let offsets = ranges
                    .iter()
                    .map(|range| {
                        (
                            (
                                range.bucket.partition_id().expect("partitioned"),
                                range.bucket.bucket_id(),
                            ),
                            range.starting_offset,
                        )
                    })
                    .collect();
                scanner
                    .subscribe_partition_buckets_with_counts(&offsets, counts)
                    .await
                    .map_err(fluss_error)?;
            } else {
                let offsets = ranges
                    .iter()
                    .map(|range| (range.bucket.bucket_id(), range.starting_offset))
                    .collect();
                scanner
                    .subscribe_buckets(&offsets)
                    .await
                    .map_err(fluss_error)?;
            }
            return Ok(ActiveReader::Streaming(scanner, VecDeque::new()));
        }
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
                .map(ActiveReader::Bounded)
                .map_err(fluss_error)
        } else {
            RecordBatchLogReader::new_from_ranges(scanner, ranges)
                .await
                .map(ActiveReader::Bounded)
                .map_err(fluss_error)
        }
    }

    async fn ensure_retention_before_start(&self, ranges: &[BoundedLogReadRange]) -> Result<()> {
        let active: Vec<_> = ranges
            .iter()
            .filter(|range| {
                self.source.spec.options.mode == LogReadMode::Streaming
                    || range.starting_offset < range.stopping_offset
            })
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
            || (self.source.spec.options.mode == LogReadMode::Streaming
                && info.get_schema_id() != self.source.spec.schema_id)
        {
            return Err(DataFusionError::Execution(
                "Fluss table topology changed; plan again".into(),
            ));
        }
        Ok(table)
    }

    async fn check_streaming_topology(&self) -> Result<()> {
        let _table = self.current_table().await?;
        if let Some(capture) = &self.partition_capture {
            let snapshot = capture
                .get()
                .expect("partition offsets captured")
                .as_ref()
                .map_err(|error| DataFusionError::Execution(error.clone()))?;
            let current = partitions::discover(
                &self.source.spec.connection,
                &self.source.spec.path,
                &self.source.spec.partition_filter,
                self.source.spec.max_assigned_buckets,
            )
            .await?;
            if current.discovered != snapshot.discovered
                || current.selected.len() != snapshot.partitions.len()
                || current
                    .selected
                    .iter()
                    .zip(&snapshot.partitions)
                    .any(|(now, old)| {
                        now.get_partition_id() != old.0 || now.get_bucket_count() != Some(old.1)
                    })
            {
                return Err(DataFusionError::Execution(
                    "Fluss partition topology changed during streaming; start a new scan explicitly".into(),
                ));
            }
        }
        Ok(())
    }

    fn projected_scanner(&self, table: &FlussTable<'_>) -> Result<RecordBatchLogScanner> {
        let scan = table.new_scan();
        let scan = match self.source.spec.projection.as_deref() {
            Some(indices) if !indices.is_empty() && self.source.spec.project_at_source => {
                scan.project(indices).map_err(fluss_error)?
            }
            _ => scan,
        };
        let scan = match &self.source.spec.filter {
            Some(predicate) => scan.filter(predicate.clone()).map_err(fluss_error)?,
            None => scan,
        };
        let scanner = scan
            .create_record_batch_log_scanner()
            .map_err(fluss_error)?;
        let expected_schema = if !self.source.spec.project_at_source
            || self
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
        if let LogStart::Offsets(offsets) = &self.source.spec.options.start {
            let expected: Vec<TableBucket> = match &partitions {
                Some(snapshot) => snapshot
                    .partitions
                    .iter()
                    .flat_map(|(id, count, _)| {
                        (0..*count).map(|bucket| {
                            TableBucket::new_with_partition(
                                self.source.spec.table_id,
                                Some(*id),
                                bucket,
                            )
                        })
                    })
                    .collect(),
                None => (0..self.source.spec.buckets)
                    .map(|bucket| TableBucket::new(self.source.spec.table_id, bucket))
                    .collect(),
            };
            if offsets.len() != expected.len()
                || expected.iter().any(|bucket| !offsets.contains_key(bucket))
            {
                return Err(DataFusionError::Execution(
                    "Explicit Fluss offsets must cover exactly the selected buckets".into(),
                ));
            }
        }
        let mut ranges: Vec<BoundedLogReadRange> = match partitions {
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
        let start = &self.source.spec.options.start;
        for range in &mut ranges {
            let earliest = range.starting_offset;
            let latest = range.stopping_offset;
            let requested = match start {
                LogStart::Earliest => earliest,
                LogStart::Latest => latest,
                LogStart::Offsets(offsets) => *offsets.get(&range.bucket).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Missing explicit Fluss offset for bucket {:?}",
                        range.bucket
                    ))
                })?,
            };
            if requested < earliest || requested > latest {
                return Err(DataFusionError::Execution(format!(
                    "Fluss start offset {requested} is outside retained range [{earliest}, {latest}] for bucket {:?}",
                    range.bucket
                )));
            }
            range.starting_offset = requested;
        }
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

    fn remaining(&self) -> Result<Option<Duration>> {
        self.deadline
            .map(|deadline| {
                deadline
                    .checked_duration_since(Instant::now())
                    .filter(|time| !time.is_zero())
                    .ok_or_else(expired)
            })
            .transpose()
    }

    fn terminate(&mut self, status: LogTermination) {
        if !self.terminal {
            self.terminal = true;
            let _ = self.source.spec.progress.send(LogProgress::Terminated {
                execution_id: self.execution_id,
                partition: self.source.index,
                status,
            });
        }
    }

    fn publish_excluded(&mut self) {
        if self.source.spec.progress.receiver_count() == 0 {
            return;
        }
        let Some(reader) = &self.reader else {
            return;
        };
        let (consumed, queued) = match reader {
            ActiveReader::Bounded(reader) => (reader.consumed_offsets(), reader.buffered_offsets()),
            ActiveReader::Streaming(scanner, queue) => (
                scanner.get_subscribed_buckets(),
                queue
                    .iter()
                    .map(|batch| (batch.bucket().clone(), batch.base_offset()))
                    .collect(),
            ),
            ActiveReader::EmptyStreaming => return,
        };
        let consumed: HashMap<_, _> = consumed.into_iter().collect();
        let mut pending = HashMap::<TableBucket, i64>::new();
        for (bucket, offset) in queued {
            pending
                .entry(bucket)
                .and_modify(|first| *first = (*first).min(offset))
                .or_insert(offset);
        }
        for (bucket, (last, stop)) in &mut self.positions {
            let fetched = consumed.get(bucket).copied().unwrap_or_else(|| {
                if let ActiveReader::Bounded(reader) = reader
                    && !reader.has_remaining_range(bucket)
                {
                    return stop.unwrap_or(*last);
                }
                *last
            });
            let next = safe_frontier(fetched, pending.get(bucket).copied(), *stop);
            if next > *last {
                let _ = self.source.spec.progress.send(LogProgress::Excluded {
                    execution_id: self.execution_id,
                    bucket: bucket.clone(),
                    base_offset: *last,
                    next_offset: next,
                });
                *last = next;
            }
        }
    }
}

impl Drop for ReadState {
    fn drop(&mut self) {
        self.terminate(LogTermination::Cancelled);
    }
}

fn expired() -> DataFusionError {
    DataFusionError::External(Box::new(crate::FlussScanTimeout {
        read: crate::FlussReadCapability::Log(LogReadMode::Batch),
    }))
}

fn fluss_error(error: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
