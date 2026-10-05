// SPDX-License-Identifier: Apache-2.0
//! DML sink: hand DataFusion's Arrow input to the existing Fluss writers.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::write_progress::{RetainedWriteBytes, TrackedReservation, WriteExecution, WriteMetrics};
use crate::{FlussWriteOperation, FlussWriteStage, FlussWriteTermination};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result, SchemaExt};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use fluss::client::{AppendWriter, FlussConnection, UpsertWriter};
use fluss::metadata::TablePath;
use fluss::row::ColumnarRow;
use futures::StreamExt;

/// The client controls batching/buffer sizes and ACKs; these bounds prevent an
/// INSERT from inheriting the client's unlimited default writer retry budget.
#[derive(Clone, Copy, Debug)]
pub struct FlussWriteOptions {
    /// Independent finite allowance for destination preparation and each
    /// between-batch metadata check. Idle input has no completion deadline.
    pub preparation_timeout: Duration,
    /// Post-materialization input backing ceiling, including finite/continuous
    /// input from other providers. Not an Arrow allocator or total RSS bound.
    pub max_retained_batch_bytes: usize,
    /// Shared enqueue/flush-ACK deadline for one input batch. Does not include
    /// destination preparation or idle waiting for the next input batch.
    pub ack_timeout: Duration,
    /// Positive cap on the existing client's writer retry budget, not a second
    /// retry loop or a guarantee that replaying the statement is safe.
    pub max_retries: i32,
}

impl Default for FlussWriteOptions {
    fn default() -> Self {
        Self {
            preparation_timeout: Duration::from_secs(30),
            max_retained_batch_bytes: crate::resources::DEFAULT_MAX_RETAINED_BATCH_BYTES,
            ack_timeout: Duration::from_secs(30),
            max_retries: 3,
        }
    }
}

impl FlussWriteOptions {
    /// Reject zero or unbounded deadlines/retry budgets before planning DML.
    pub fn validate(self) -> Result<Self> {
        if self.preparation_timeout < Duration::from_millis(1)
            || self.preparation_timeout > Duration::from_secs(3600)
        {
            return Err(DataFusionError::Plan(
                "Fluss write preparation timeout must be in 1ms..=3600s".into(),
            ));
        }
        crate::resources::validate_batch_limit(self.max_retained_batch_bytes)?;
        if self.ack_timeout < Duration::from_millis(1)
            || self.ack_timeout > Duration::from_secs(3600)
        {
            return Err(DataFusionError::Plan(
                "Fluss write ACK timeout must be in 1ms..=3600s".into(),
            ));
        }
        if self.max_retries < 1 {
            return Err(DataFusionError::Plan(
                "Fluss writer retries must be positive".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum WriteKind {
    Log,
    Kv,
    DeleteKv,
    MergeKv,
}

pub(crate) struct FlussWriteTarget {
    pub(crate) connection: Arc<FlussConnection>,
    pub(crate) path: TablePath,
    pub(crate) table_id: i64,
    pub(crate) schema_id: i32,
    pub(crate) schema: SchemaRef,
    pub(crate) kind: WriteKind,
    pub(crate) options: FlussWriteOptions,
    pub(crate) writes: tokio::sync::broadcast::Sender<crate::FlussWriteProgress>,
    pub(crate) metrics: WriteMetrics,
}

impl fmt::Debug for FlussWriteTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussSink")
            .field("path", &self.path)
            .field("kind", &self.kind)
            .finish()
    }
}

pub(crate) fn plan_write(
    mut target: FlussWriteTarget,
    input: Arc<dyn ExecutionPlan>,
    insert_op: InsertOp,
) -> Result<Arc<dyn ExecutionPlan>> {
    if insert_op != InsertOp::Append {
        return Err(DataFusionError::NotImplemented(format!(
            "Fluss does not support {insert_op}; INSERT INTO is append for logs and upsert for KV"
        )));
    }
    target
        .schema
        .logically_equivalent_names_and_types(&input.schema())?;
    target.options = target.options.validate()?;
    Ok(Arc::new(DataSinkExec::new(input, Arc::new(target), None)))
}

impl DisplayAs for FlussWriteTarget {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FlussWriteSink: kind={:?}, table={}",
            self.kind, self.path
        )
    }
}

/// Each INSERT uses its own writer client, so flushing/closing it cannot
/// acknowledge or cancel another INSERT on the shared read connection.
struct CloseWriterOnDrop {
    connection: Option<Arc<FlussConnection>>,
    timeout: Duration,
    cancelled: Arc<AtomicBool>,
}

impl CloseWriterOnDrop {
    async fn close(mut self) -> Result<()> {
        if let Some(connection) = &self.connection {
            connection.close(self.timeout).await.map_err(fluss_error)?;
        }
        self.connection.take();
        Ok(())
    }
}

impl Drop for CloseWriterOnDrop {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(connection) = self.connection.take() {
            // Abort synchronously: cancellation must also wake a blocking
            // enqueue, and must not depend on another runtime task running.
            connection.abort_writes();
        }
    }
}

enum FlussWriter {
    Log(AppendWriter),
    Kv(UpsertWriter, TrackedReservation),
    DeleteKv(UpsertWriter, TrackedReservation),
    MergeKv(UpsertWriter, TrackedReservation),
}

struct WriterPoolAccounting {
    encoded: datafusion::execution::memory_pool::MemoryReservation,
    transport: datafusion::execution::memory_pool::MemoryReservation,
    routing: datafusion::execution::memory_pool::MemoryReservation,
    metrics: WriteMetrics,
}
fn writer_reservation(
    consumer: &datafusion::execution::memory_pool::MemoryReservation,
    bytes: usize,
    gauge: datafusion::physical_plan::metrics::Gauge,
) -> fluss::error::Result<Arc<dyn Send + Sync>> {
    let reservation = TrackedReservation::new(consumer.new_empty(), gauge);
    reservation
        .try_grow(bytes)
        .map_err(|error| fluss::error::Error::WriterMemoryAdmission {
            source: Box::new(error),
        })?;
    Ok(Arc::new(std::sync::Mutex::new(reservation)))
}
impl fluss::client::WriterMemoryAccounting for WriterPoolAccounting {
    fn reserve(&self, bytes: usize) -> fluss::error::Result<Arc<dyn Send + Sync>> {
        writer_reservation(&self.encoded, bytes, self.metrics.encoded_bytes.clone())
    }
    fn reserve_transport(&self, bytes: usize) -> fluss::error::Result<Arc<dyn Send + Sync>> {
        writer_reservation(&self.transport, bytes, self.metrics.transport_bytes.clone())
    }
    fn reserve_routing(&self, bytes: usize) -> fluss::error::Result<Arc<dyn Send + Sync>> {
        writer_reservation(&self.routing, bytes, self.metrics.routing_bytes.clone())
    }
}

impl FlussWriter {
    #[allow(clippy::too_many_arguments)]
    fn enqueue(
        &self,
        batch: arrow::record_batch::RecordBatch,
        row_type: Arc<fluss::metadata::RowType>,
        cancelled: &AtomicBool,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        max_bytes: usize,
        metrics: &WriteMetrics,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if let Self::Log(writer) = self {
            if cancelled.load(Ordering::Acquire) {
                return Err(DataFusionError::Execution(
                    "Fluss write cancelled; in-flight writes may have been committed".into(),
                ));
            }
            // Client owns routing and byte slicing for both finite/continuous
            // input. Its abort wakes enqueue and interrupts the routing loop.
            let routing_scratch = TrackedReservation::new(
                reservation.new_empty(),
                metrics.routing_scratch_bytes.clone(),
            );
            routing_scratch.try_grow(
                writer
                    .estimated_arrow_routing_bytes(&batch)
                    .map_err(fluss_error)?,
            )?;
            drop(
                writer
                    .append_arrow_batch_with_retainer(batch, |materialized| {
                        crate::resources::reserve_batch(
                            materialized,
                            reservation,
                            max_bytes,
                            &metrics.arrow_bytes,
                        )
                        .map_err(|error| {
                            fluss::error::Error::UnexpectedError {
                                message: "Fluss Arrow materialization memory admission failed"
                                    .into(),
                                source: Some(Box::new(error)),
                            }
                        })
                    })
                    .map_err(fluss_error)?,
            );
            return Ok(());
        }
        // Build typed column views once; route *each* row so a DataFusion
        // batch may contain several Fluss partitions/buckets safely.
        let encoding_scratch = match self {
            Self::Kv(_, scratch) | Self::DeleteKv(_, scratch) | Self::MergeKv(_, scratch) => {
                scratch
            }
            Self::Log(_) => unreachable!("log append is columnar"),
        };
        // Native KV/key encoders retain reusable row scratch across ACKs and
        // idle input. Keep its conservative allowance with the writer owner,
        // including a still-running worker after the async query is cancelled.
        encoding_scratch.try_resize(
            encoding_scratch.size().max(
                batch
                    .get_array_memory_size()
                    .saturating_mul(2)
                    .saturating_add(4096),
            ),
        )?;
        let actions = if matches!(self, Self::MergeKv(_, _)) {
            Some(
                batch
                    .column(batch.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Invalid Fluss MERGE action column".into())
                    })?
                    .clone(),
            )
        } else {
            None
        };
        let batch = if actions.is_some() {
            batch.project(&(0..batch.num_columns() - 1).collect::<Vec<_>>())?
        } else {
            batch
        };
        let mut row =
            ColumnarRow::new(Arc::new(batch.clone()), row_type, 0, None).map_err(fluss_error)?;
        for id in 0..batch.num_rows() {
            if cancelled.load(Ordering::Acquire) {
                return Err(DataFusionError::Execution(
                    "Fluss write cancelled; in-flight writes may have been committed".into(),
                ));
            }
            row.set_row_id(id);
            match self {
                Self::Log(_) => unreachable!("log append is columnar"),
                Self::Kv(writer, _) => {
                    drop(writer.upsert(&row).map_err(fluss_error)?);
                }
                Self::DeleteKv(writer, _) => {
                    drop(writer.delete(&row).map_err(fluss_error)?);
                }
                Self::MergeKv(writer, _) => {
                    if actions.as_ref().unwrap().value(id) {
                        drop(writer.delete(&row).map_err(fluss_error)?);
                    } else {
                        drop(writer.upsert(&row).map_err(fluss_error)?);
                    }
                }
            }
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        match self {
            Self::Log(writer) => writer.flush().await.map_err(fluss_error),
            Self::Kv(writer, _) | Self::DeleteKv(writer, _) | Self::MergeKv(writer, _) => {
                writer.flush().await.map_err(fluss_error)
            }
        }
    }
}

#[async_trait]
impl DataSink for FlussWriteTarget {
    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.set.clone_inner())
    }
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        input: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let operation = match self.kind {
            WriteKind::Log => FlussWriteOperation::Append,
            WriteKind::Kv => FlussWriteOperation::Upsert,
            WriteKind::DeleteKv => FlussWriteOperation::Delete,
            WriteKind::MergeKv => FlussWriteOperation::Merge,
        };
        let mut execution = WriteExecution::new(
            self.writes.clone(),
            self.metrics.clone(),
            self.path.clone(),
            self.table_id,
            self.schema_id,
            operation,
            &self.connection.config().writer_acks,
            self.options,
        )?;
        let result = self.execute_write(input, context, &mut execution).await;
        execution.finish(if result.is_ok() {
            FlussWriteTermination::Completed
        } else {
            FlussWriteTermination::Failed
        });
        result
    }
}

impl FlussWriteTarget {
    async fn execute_write(
        &self,
        mut input: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
        execution: &mut WriteExecution,
    ) -> Result<u64> {
        let preparation_timer = execution.metrics.preparation_time.timer();
        let mut config = self.connection.config().clone();
        validate_ack_policy(&config.writer_acks)?;
        config.writer_retries = config.writer_retries.min(self.options.max_retries);
        config.writer_buffer_wait_timeout_ms = config
            .writer_buffer_wait_timeout_ms
            .min(self.options.ack_timeout.as_millis() as u64);
        let preparation_deadline = tokio::time::Instant::now() + self.options.preparation_timeout;
        let connection = Arc::new(
            tokio::time::timeout_at(preparation_deadline, FlussConnection::new(config))
                .await
                .map_err(|_| write_phase_timeout(crate::FlussWritePhase::Preparation))?
                .map_err(fluss_error)?,
        );
        connection
            .set_writer_memory_accounting(Arc::new(WriterPoolAccounting {
                encoded: MemoryConsumer::new("FlussWriteEncoded")
                    .register(&context.runtime_env().memory_pool),
                transport: MemoryConsumer::new("FlussWriteTransport")
                    .register(&context.runtime_env().memory_pool),
                routing: MemoryConsumer::new("FlussWriteRoutingMetadata")
                    .register(&context.runtime_env().memory_pool),
                metrics: execution.metrics.clone(),
            }))
            .map_err(fluss_error)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let close = CloseWriterOnDrop {
            connection: Some(Arc::clone(&connection)),
            timeout: self.options.ack_timeout,
            cancelled: Arc::clone(&cancelled),
        };
        let table = tokio::time::timeout_at(preparation_deadline, connection.get_table(&self.path))
            .await
            .map_err(|_| write_phase_timeout(crate::FlussWritePhase::Preparation))?
            .map_err(fluss_error)?;
        self.check_identity(&table)?;
        if matches!(self.kind, WriteKind::MergeKv) {
            validate_merge_policy(table.get_table_info().get_properties())?;
        }
        if matches!(self.kind, WriteKind::DeleteKv) {
            validate_delete_policy(table.get_table_info().get_properties())?;
        }
        let mut partitions = std::collections::HashMap::new();
        if table.get_table_info().is_partitioned() {
            tokio::time::timeout_at(
                preparation_deadline,
                refresh_write_partitions(&connection, &self.path, &mut partitions),
            )
            .await
            .map_err(|_| write_phase_timeout(crate::FlussWritePhase::Preparation))??;
        }
        let row_type = Arc::new(table.get_table_info().get_row_type().clone());
        let destination_schema = fluss::record::to_arrow_schema(&row_type).map_err(fluss_error)?;
        let key_indices = table
            .get_table_info()
            .get_primary_keys()
            .iter()
            .map(|name| destination_schema.index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let key_converter = if matches!(self.kind, WriteKind::MergeKv) {
            Some(arrow::row::RowConverter::new(
                key_indices
                    .iter()
                    .map(|&index| {
                        arrow::row::SortField::new(
                            destination_schema.field(index).data_type().clone(),
                        )
                    })
                    .collect(),
            )?)
        } else {
            None
        };
        let mut seen_keys = std::collections::HashSet::<Vec<u8>>::new();
        let key_reservation = TrackedReservation::new(
            MemoryConsumer::new("FlussMergeKeys").register(&context.runtime_env().memory_pool),
            execution.metrics.merge_key_bytes.clone(),
        );
        let encoding_scratch = TrackedReservation::new(
            MemoryConsumer::new("FlussKvEncodingScratch")
                .register(&context.runtime_env().memory_pool),
            execution.metrics.kv_scratch_bytes.clone(),
        );
        let writer = Arc::new(match self.kind {
            WriteKind::Log => FlussWriter::Log(
                table
                    .new_append()
                    .map_err(fluss_error)?
                    .create_writer()
                    .map_err(fluss_error)?,
            ),
            WriteKind::Kv => FlussWriter::Kv(
                table
                    .new_upsert()
                    .map_err(fluss_error)?
                    .create_writer()
                    .map_err(fluss_error)?,
                encoding_scratch,
            ),
            WriteKind::DeleteKv => FlussWriter::DeleteKv(
                table
                    .new_upsert()
                    .map_err(fluss_error)?
                    .create_writer()
                    .map_err(fluss_error)?,
                encoding_scratch,
            ),
            WriteKind::MergeKv => FlussWriter::MergeKv(
                table
                    .new_upsert()
                    .map_err(fluss_error)?
                    .create_writer()
                    .map_err(fluss_error)?,
                encoding_scratch,
            ),
        });
        if tokio::time::Instant::now() >= preparation_deadline {
            return Err(write_phase_timeout(crate::FlussWritePhase::Preparation));
        }
        let mut confirmed = 0_u64;
        drop(preparation_timer);
        loop {
            execution.stage(FlussWriteStage::Input);
            let next = {
                let _timer = execution.metrics.input_wait_time.timer();
                input.next().await
            };
            let Some(next) = next else {
                execution.input_exhausted();
                break;
            };
            let batch = next?;
            if batch.num_rows() == 0 {
                continue;
            }
            execution.receive(batch.num_rows())?;
            execution.stage(FlussWriteStage::Metadata);
            let metadata_timer = execution.metrics.metadata_time.timer();
            let metadata_deadline = tokio::time::Instant::now() + self.options.preparation_timeout;
            let current =
                tokio::time::timeout_at(metadata_deadline, connection.get_table(&self.path))
                    .await
                    .map_err(|_| write_phase_timeout(crate::FlussWritePhase::Metadata))?
                    .map_err(fluss_error)?;
            self.check_identity(&current)?;
            if matches!(self.kind, WriteKind::MergeKv) {
                validate_merge_policy(current.get_table_info().get_properties())?;
            }
            if current.get_table_info().is_partitioned() {
                tokio::time::timeout_at(
                    metadata_deadline,
                    refresh_write_partitions(&connection, &self.path, &mut partitions),
                )
                .await
                .map_err(|_| write_phase_timeout(crate::FlussWritePhase::Metadata))??;
            }
            // Include validation, admission and MERGE key preparation in this
            // batch's one enqueue/ACK budget; no separate retry allowance.
            let deadline = tokio::time::Instant::now() + self.options.ack_timeout;
            drop(metadata_timer);
            execution.stage(FlussWriteStage::Validation);
            if matches!(self.kind, WriteKind::DeleteKv) {
                validate_delete_policy(current.get_table_info().get_properties())?;
            }
            self.schema
                .logically_equivalent_names_and_types(&batch.schema())?;
            validate_required_values(&destination_schema, &batch)?;
            let retained_bytes = crate::resources::batch_backing_bytes(&batch)?;
            if retained_bytes > self.options.max_retained_batch_bytes {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "Fluss write input retains {retained_bytes} backing bytes, exceeding max_retained_batch_bytes={}",
                    self.options.max_retained_batch_bytes
                )));
            }
            let reservation =
                MemoryConsumer::new("FlussWriteBatch").register(&context.runtime_env().memory_pool);
            let batch = if matches!(self.kind, WriteKind::Log) {
                // Prebuilt client batches retain Arrow beyond enqueue and may
                // outlive this task on cancellation. Reuse the read-side lease
                // mechanism so their buffers carry the reservation until drop.
                crate::resources::reserve_batch(
                    batch,
                    &reservation,
                    self.options.max_retained_batch_bytes,
                    &execution.metrics.arrow_bytes,
                )?
            } else {
                reservation.try_grow(batch.get_array_memory_size())?;
                batch
            };
            let retained = (!matches!(self.kind, WriteKind::Log)).then(|| {
                RetainedWriteBytes::new(
                    execution.metrics.arrow_bytes.clone(),
                    batch.get_array_memory_size(),
                )
            });
            if let Some(converter) = &key_converter {
                let actions = batch
                    .column(batch.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| DataFusionError::Execution("Invalid MERGE actions".into()))?;
                if actions.true_count() != 0 {
                    validate_delete_policy(current.get_table_info().get_properties())?;
                }
                let columns = key_indices
                    .iter()
                    .map(|&i| batch.column(i).clone())
                    .collect::<Vec<_>>();
                let scratch = TrackedReservation::new(
                    MemoryConsumer::new("FlussMergeKeyEncoding")
                        .register(&context.runtime_env().memory_pool),
                    execution.metrics.merge_key_bytes.clone(),
                );
                scratch.try_grow(
                    columns
                        .iter()
                        .map(|column| column.get_array_memory_size())
                        .sum::<usize>()
                        .saturating_mul(2)
                        .saturating_add(batch.num_rows().saturating_mul(32)),
                )?;
                let encoded = converter.convert_columns(&columns)?;
                scratch.try_resize(encoded.size())?;
                for key in encoded.iter() {
                    if seen_keys.contains(key.as_ref()) {
                        return Err(DataFusionError::Execution(
                            "Fluss MERGE has multiple modifying actions for one primary key; earlier batches may already be committed".into(),
                        ));
                    }
                    // Conservative allowance for encoded bytes and hash-table
                    // overhead; this set lasts until the finite MERGE ends.
                    key_reservation
                        .try_grow(key.as_ref().len().saturating_mul(2).saturating_add(64))?;
                    seen_keys.insert(key.as_ref().to_vec());
                }
            }
            let rows = batch.num_rows() as u64;
            if tokio::time::Instant::now() >= deadline {
                return Err(write_timeout());
            }
            // Own a reservation until the blocking task releases its retained
            // batch, even when its async caller is cancelled. Source/operator
            // reservations remain separate and may overlap conservatively.
            // Fluss's bounded writer memory limiter can wait synchronously.
            // Do not block the Tokio worker needed by its sender task.
            let batch_writer = Arc::clone(&writer);
            let batch_row_type = Arc::clone(&row_type);
            let stop = Arc::clone(&cancelled);
            let max_bytes = self.options.max_retained_batch_bytes;
            let batch_metrics = execution.metrics.clone();
            execution.enqueue();
            let enqueue_timer = execution.metrics.enqueue_time.timer();
            let enqueue = tokio::task::spawn_blocking(move || {
                let _retained = retained;
                batch_writer.enqueue(
                    batch,
                    batch_row_type,
                    &stop,
                    &reservation,
                    max_bytes,
                    &batch_metrics,
                )
            });
            tokio::time::timeout_at(deadline, enqueue)
                .await
                .map_err(|_| write_timeout())?
                .map_err(|error| DataFusionError::External(Box::new(error)))??;
            drop(enqueue_timer);
            execution.stage(FlussWriteStage::Ack);
            let ack_timer = execution.metrics.ack_time.timer();
            // Confirm *each* input batch, including a singleton from a sparse
            // unbounded source. Waiting for EOF would never flush streaming DML.
            tokio::time::timeout_at(deadline, writer.flush())
                .await
                .map_err(|_| write_timeout())??;
            drop(ack_timer);
            confirmed = confirmed.checked_add(rows).ok_or_else(|| {
                DataFusionError::Execution("Fluss inserted row count overflowed".into())
            })?;
            execution.confirmed();
        }
        execution.stage(FlussWriteStage::Cleanup);
        let _timer = execution.metrics.cleanup_time.timer();
        close.close().await?;
        Ok(confirmed)
    }
}

impl FlussWriteTarget {
    fn check_identity(&self, table: &fluss::client::FlussTable<'_>) -> Result<()> {
        let info = table.get_table_info();
        if info.table_id != self.table_id || info.get_schema_id() != self.schema_id {
            return Err(DataFusionError::Execution(
                "Fluss write destination changed table identity or schema; plan again".into(),
            ));
        }
        Ok(())
    }
}

fn fluss_error(error: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn write_timeout() -> DataFusionError {
    write_phase_timeout(crate::FlussWritePhase::EnqueueAndAck)
}

fn write_phase_timeout(phase: crate::FlussWritePhase) -> DataFusionError {
    DataFusionError::External(Box::new(crate::FlussWriteTimeout { phase }))
}

async fn refresh_write_partitions(
    connection: &FlussConnection,
    path: &TablePath,
    known: &mut std::collections::HashMap<String, (i64, i32)>,
) -> Result<()> {
    let admin = connection.get_admin().map_err(fluss_error)?;
    let latest = admin
        .list_partition_infos(path)
        .await
        .map_err(fluss_error)?;
    let mut current = std::collections::HashMap::new();
    for partition in &latest {
        let count = partition.get_bucket_count().ok_or_else(|| {
            DataFusionError::Execution("Fluss write partition layout is unavailable".into())
        })?;
        current.insert(
            partition.get_partition_name().clone(),
            (partition.get_partition_id(), count),
        );
    }
    if known
        .iter()
        .any(|(name, layout)| current.get(name) != Some(layout))
    {
        return Err(DataFusionError::Execution("Fluss write partition identity/layout changed; replan; earlier writes may already be committed".into()));
    }
    connection
        .get_metadata()
        .check_and_update_partition_metadata_by_ids(
            path,
            &latest
                .iter()
                .map(|p| p.get_partition_id())
                .collect::<Vec<_>>(),
        )
        .await
        .map_err(fluss_error)?;
    *known = current;
    Ok(())
}

fn validate_ack_policy(acks: &str) -> Result<()> {
    if acks.eq_ignore_ascii_case("all")
        || acks.parse::<i16>().is_ok_and(|ack| ack == -1 || ack == 1)
    {
        Ok(())
    } else {
        Err(DataFusionError::Plan(
            "Fluss SQL writes require writer_acks=all, -1 or 1 for server-confirmed counts".into(),
        ))
    }
}

pub(crate) fn validate_delete_policy(
    properties: &std::collections::HashMap<String, String>,
) -> Result<()> {
    let behavior = properties
        .get("table.delete.behavior")
        .map(String::as_str)
        .unwrap_or_else(|| {
            if properties.contains_key("table.merge-engine") {
                "ignore"
            } else {
                "allow"
            }
        });
    if behavior.eq_ignore_ascii_case("allow") {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "Fluss SQL DELETE requires table.delete.behavior=allow; effective behavior is {behavior}"
        )))
    }
}

pub(crate) fn validate_merge_policy(
    properties: &std::collections::HashMap<String, String>,
) -> Result<()> {
    if properties.contains_key("table.merge-engine") {
        return Err(DataFusionError::NotImplemented(
            "Fluss SQL MERGE requires ordinary full-row KV replacement; configured merge engines may ignore, version or aggregate upserts".into(),
        ));
    }
    Ok(())
}

fn validate_required_values(
    schema: &SchemaRef,
    batch: &arrow::record_batch::RecordBatch,
) -> Result<()> {
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if !field.is_nullable() && column.null_count() != 0 {
            return Err(DataFusionError::Execution(format!(
                "Fluss write column '{}' is required but contains null values",
                field.name()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn writer_accounting_uses_the_real_pool_and_guards_survive_adapter_drop() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
        use fluss::client::WriterMemoryAccounting;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2048));
        let adapter = WriterPoolAccounting {
            encoded: MemoryConsumer::new("encoder").register(&pool),
            transport: MemoryConsumer::new("transport").register(&pool),
            routing: MemoryConsumer::new("routing").register(&pool),
            metrics: Default::default(),
        };
        let first = adapter.reserve(1024).unwrap();
        let second = adapter.reserve(512).unwrap();
        let error = adapter.reserve(1024).err().unwrap();
        assert!(matches!(
            error,
            fluss::error::Error::WriterMemoryAdmission { .. }
        ));
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<DataFusionError>()
                .is_some()
        );
        assert_eq!(pool.reserved(), 1536);
        drop(adapter);
        drop(first);
        assert_eq!(pool.reserved(), 512);
        drop(second);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn preparation_and_input_limits_cannot_be_unbounded_by_zero_or_bad_units() {
        let options = FlussWriteOptions::default();
        assert_eq!(options.preparation_timeout, Duration::from_secs(30));
        assert_eq!(options.max_retained_batch_bytes, 64 * 1024 * 1024);
        assert!(
            FlussWriteOptions {
                preparation_timeout: Duration::from_nanos(1),
                ..options
            }
            .validate()
            .is_err()
        );
        assert!(
            FlussWriteOptions {
                max_retained_batch_bytes: 0,
                ..options
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn server_confirmed_counts_require_an_ack_policy() {
        for policy in ["all", "ALL", "-1", "1"] {
            assert!(validate_ack_policy(policy).is_ok());
        }
        for policy in ["0", "2", "garbage"] {
            assert!(validate_ack_policy(policy).is_err());
        }
    }

    #[test]
    fn submillisecond_ack_budget_cannot_become_a_zero_client_timeout() {
        let options = FlussWriteOptions {
            ack_timeout: Duration::from_nanos(1),
            ..Default::default()
        };
        assert!(options.validate().is_err());
        assert!(
            FlussWriteOptions {
                ack_timeout: Duration::from_millis(1),
                ..options
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn deletes_never_report_success_for_an_ignoring_merge_engine() {
        use std::collections::HashMap;
        assert!(validate_delete_policy(&HashMap::new()).is_ok());
        let mut config = HashMap::from([("table.merge-engine".into(), "versioned".into())]);
        assert!(validate_delete_policy(&config).is_err());
        config.insert("table.delete.behavior".into(), "allow".into());
        assert!(validate_delete_policy(&config).is_ok());
        for behavior in ["ignore", "disable"] {
            config.insert("table.delete.behavior".into(), behavior.into());
            assert!(validate_delete_policy(&config).is_err());
        }
    }

    #[test]
    fn nullable_input_cannot_hide_nulls_in_a_required_destination() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
        )])
        .unwrap();
        assert!(validate_required_values(&schema, &batch).is_err());
        assert!(validate_required_values(&schema, &batch.slice(0, 1)).is_ok());
    }

    #[test]
    fn columnar_sink_leases_cover_slices_and_independent_gathers_after_worker_drop() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
        use datafusion::physical_plan::metrics::Gauge;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let worker = MemoryConsumer::new("FlussWriteBatch").register(&pool);
        let original = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        )])
        .unwrap();
        let input =
            crate::resources::reserve_batch(original, &worker, usize::MAX, &Gauge::new()).unwrap();
        let slice = input.slice(1, 2);
        let first = pool.reserved();
        let indices = arrow::array::UInt32Array::from(vec![0, 2]);
        let gathered = RecordBatch::try_new(
            input.schema(),
            vec![arrow::compute::take(input.column(0), &indices, None).unwrap()],
        )
        .unwrap();
        let gathered =
            crate::resources::reserve_batch(gathered, &worker, usize::MAX, &Gauge::new()).unwrap();
        assert!(pool.reserved() > first);
        drop(input);
        drop(worker);
        // A client queue may retain either independent materialization after
        // the worker/query has gone; neither depends on an async scope guard.
        assert!(pool.reserved() > first);
        drop(slice);
        assert!(pool.reserved() > 0);
        assert_eq!(
            gathered
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[1, 3]
        );
        drop(gathered);
        assert_eq!(pool.reserved(), 0);
    }
}
