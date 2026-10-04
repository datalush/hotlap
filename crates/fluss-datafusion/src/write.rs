// SPDX-License-Identifier: Apache-2.0
//! DML sink: hand DataFusion's Arrow input to the existing Fluss writers.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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
            ack_timeout: Duration::from_secs(30),
            max_retries: 3,
        }
    }
}

impl FlussWriteOptions {
    /// Reject zero or unbounded deadlines/retry budgets before planning DML.
    pub fn validate(self) -> Result<Self> {
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
    Kv(UpsertWriter),
    DeleteKv(UpsertWriter),
    MergeKv(UpsertWriter),
}

impl FlussWriter {
    fn enqueue(
        &self,
        batch: arrow::record_batch::RecordBatch,
        row_type: Arc<fluss::metadata::RowType>,
        cancelled: &AtomicBool,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
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
            reservation.try_grow(
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
                            usize::MAX,
                            &datafusion::physical_plan::metrics::Gauge::new(),
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
        let actions = if matches!(self, Self::MergeKv(_)) {
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
                Self::Kv(writer) => {
                    drop(writer.upsert(&row).map_err(fluss_error)?);
                }
                Self::DeleteKv(writer) => {
                    drop(writer.delete(&row).map_err(fluss_error)?);
                }
                Self::MergeKv(writer) => {
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
            Self::Kv(writer) | Self::DeleteKv(writer) | Self::MergeKv(writer) => {
                writer.flush().await.map_err(fluss_error)
            }
        }
    }
}

#[async_trait]
impl DataSink for FlussWriteTarget {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut input: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut config = self.connection.config().clone();
        validate_ack_policy(&config.writer_acks)?;
        config.writer_retries = config.writer_retries.min(self.options.max_retries);
        config.writer_buffer_wait_timeout_ms = config
            .writer_buffer_wait_timeout_ms
            .min(self.options.ack_timeout.as_millis() as u64);
        let connection = Arc::new(FlussConnection::new(config).await.map_err(fluss_error)?);
        let cancelled = Arc::new(AtomicBool::new(false));
        let close = CloseWriterOnDrop {
            connection: Some(Arc::clone(&connection)),
            timeout: self.options.ack_timeout,
            cancelled: Arc::clone(&cancelled),
        };
        let table = connection
            .get_table(&self.path)
            .await
            .map_err(fluss_error)?;
        self.check_identity(&table)?;
        if matches!(self.kind, WriteKind::DeleteKv) {
            validate_delete_policy(table.get_table_info().get_properties())?;
        }
        if table.get_table_info().is_partitioned() {
            // WriterClient routes synchronously from its metadata cache. Old
            // partitions can have different counts after a table rescale.
            let ids = connection
                .get_admin()
                .map_err(fluss_error)?
                .list_partition_infos(&self.path)
                .await
                .map_err(fluss_error)?
                .iter()
                .map(|partition| partition.get_partition_id())
                .collect::<Vec<_>>();
            connection
                .get_metadata()
                .check_and_update_partition_metadata_by_ids(&self.path, &ids)
                .await
                .map_err(fluss_error)?;
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
        let key_reservation =
            MemoryConsumer::new("FlussMergeKeys").register(&context.runtime_env().memory_pool);
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
            ),
            WriteKind::DeleteKv => FlussWriter::DeleteKv(
                table
                    .new_upsert()
                    .map_err(fluss_error)?
                    .create_writer()
                    .map_err(fluss_error)?,
            ),
            WriteKind::MergeKv => FlussWriter::MergeKv(
                table
                    .new_upsert()
                    .map_err(fluss_error)?
                    .create_writer()
                    .map_err(fluss_error)?,
            ),
        });
        let mut confirmed = 0_u64;
        while let Some(next) = input.next().await {
            let batch = next?;
            if batch.num_rows() == 0 {
                continue;
            }
            let current = connection
                .get_table(&self.path)
                .await
                .map_err(fluss_error)?;
            self.check_identity(&current)?;
            if matches!(self.kind, WriteKind::DeleteKv) {
                validate_delete_policy(current.get_table_info().get_properties())?;
            }
            self.schema
                .logically_equivalent_names_and_types(&batch.schema())?;
            validate_required_values(&destination_schema, &batch)?;
            let reservation =
                MemoryConsumer::new("FlussWriteBatch").register(&context.runtime_env().memory_pool);
            let batch = if matches!(self.kind, WriteKind::Log) {
                // Prebuilt client batches retain Arrow beyond enqueue and may
                // outlive this task on cancellation. Reuse the read-side lease
                // mechanism so their buffers carry the reservation until drop.
                crate::resources::reserve_batch(
                    batch,
                    &reservation,
                    usize::MAX,
                    &datafusion::physical_plan::metrics::Gauge::new(),
                )?
            } else {
                reservation.try_grow(batch.get_array_memory_size())?;
                batch
            };
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
                let scratch = MemoryConsumer::new("FlussMergeKeyEncoding")
                    .register(&context.runtime_env().memory_pool);
                scratch.try_grow(
                    batch
                        .get_array_memory_size()
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
            let deadline = tokio::time::Instant::now() + self.options.ack_timeout;
            // Own a reservation until the blocking task releases its retained
            // batch, even when its async caller is cancelled. Source/operator
            // reservations remain separate and may overlap conservatively.
            // Fluss's bounded writer memory limiter can wait synchronously.
            // Do not block the Tokio worker needed by its sender task.
            let batch_writer = Arc::clone(&writer);
            let batch_row_type = Arc::clone(&row_type);
            let stop = Arc::clone(&cancelled);
            let enqueue = tokio::task::spawn_blocking(move || {
                batch_writer.enqueue(batch, batch_row_type, &stop, &reservation)
            });
            tokio::time::timeout_at(deadline, enqueue)
                .await
                .map_err(|_| write_timeout())?
                .map_err(|error| DataFusionError::External(Box::new(error)))??;
            // Confirm *each* input batch, including a singleton from a sparse
            // unbounded source. Waiting for EOF would never flush streaming DML.
            tokio::time::timeout_at(deadline, writer.flush())
                .await
                .map_err(|_| write_timeout())??;
            confirmed = confirmed.checked_add(rows).ok_or_else(|| {
                DataFusionError::Execution("Fluss inserted row count overflowed".into())
            })?;
        }
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
    DataFusionError::Execution(
        "Fluss write enqueue/ACK timed out; in-flight writes may have been committed".into(),
    )
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
