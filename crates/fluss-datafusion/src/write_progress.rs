// SPDX-License-Identifier: Apache-2.0
//! Bounded write observations. Counts describe operations, never net rows or checkpoints.

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Time,
};
use fluss::metadata::TablePath;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

pub(crate) const WRITE_OBSERVATION_CAPACITY: usize = 128;
static NEXT_WRITE_EXECUTION: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlussWriteOperation {
    Append,
    Upsert,
    Delete,
    Merge,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlussWriteAck {
    All,
    Leader,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlussWriteStage {
    Preparation,
    Input,
    Metadata,
    Validation,
    Enqueue,
    Ack,
    Cleanup,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlussWriteTermination {
    Completed,
    Failed,
    Cancelled,
}

/// A successful whole-input-batch flush confirms all its operations. Once
/// enqueue was attempted, any error leaves the entire batch conservatively
/// uncertain: some client groups may have ACKed, but aggregate flush cannot
/// establish a per-row partition of those outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlussWriteBatchOutcome {
    Confirmed,
    RejectedBeforeEnqueue,
    Uncertain,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FlussWriteCounts {
    pub received: u64,
    pub confirmed: u64,
    pub rejected_before_enqueue: u64,
    pub uncertain: u64,
    pub pending: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlussWriteSummary {
    pub execution_id: u64,
    pub table_id: i64,
    pub schema_id: i32,
    pub operation: FlussWriteOperation,
    pub ack: Option<FlussWriteAck>,
    pub counts: FlussWriteCounts,
    pub batches_received: u64,
    pub batches_confirmed: u64,
    pub status: FlussWriteTermination,
    pub stage: FlussWriteStage,
    pub input_exhausted: bool,
}

/// Subscribe before execution. Tokio RecvError::Lagged means missing detail;
/// cumulative snapshots remain lower-bound/application knowledge, not a
/// complete batch history. No row payloads, SQL or error strings are retained.
#[derive(Clone, Debug)]
pub enum FlussWriteProgress {
    Initialized {
        execution_id: u64,
        path: TablePath,
        table_id: i64,
        schema_id: i32,
        operation: FlussWriteOperation,
        ack: Option<FlussWriteAck>,
        options: crate::FlussWriteOptions,
    },
    BatchReceived {
        execution_id: u64,
        batch_id: u64,
        operations: u64,
        counts: FlussWriteCounts,
    },
    BatchOutcome {
        execution_id: u64,
        batch_id: u64,
        operations: u64,
        outcome: FlussWriteBatchOutcome,
        counts: FlussWriteCounts,
    },
    Terminated(FlussWriteSummary),
}

/// One fixed metric set per physical sink, aggregated if that plan is reexecuted.
/// Execution identity belongs in observations, never in metric labels.
#[derive(Clone)]
pub(crate) struct WriteMetrics {
    pub set: ExecutionPlanMetricsSet,
    pub received: Count,
    pub confirmed: Count,
    pub rejected: Count,
    pub uncertain: Count,
    pub batches_confirmed: Count,
    pub failed: Count,
    pub cancelled: Count,
    pub pending: Gauge,
    pub active: Gauge,
    pub arrow_bytes: Gauge,
    pub encoded_bytes: Gauge,
    pub transport_bytes: Gauge,
    pub routing_bytes: Gauge,
    pub routing_scratch_bytes: Gauge,
    pub kv_scratch_bytes: Gauge,
    pub merge_key_bytes: Gauge,
    pub preparation_time: Time,
    pub metadata_time: Time,
    pub input_wait_time: Time,
    pub enqueue_time: Time,
    pub ack_time: Time,
    pub cleanup_time: Time,
}
impl Default for WriteMetrics {
    fn default() -> Self {
        let set = ExecutionPlanMetricsSet::new();
        let builder = || MetricBuilder::new(&set);
        Self {
            received: builder().counter("fluss_write_received_operations", 0),
            confirmed: builder().counter("fluss_write_confirmed_operations", 0),
            rejected: builder().counter("fluss_write_rejected_before_enqueue_operations", 0),
            uncertain: builder().counter("fluss_write_uncertain_operations", 0),
            batches_confirmed: builder().counter("fluss_write_confirmed_batches", 0),
            failed: builder().counter("fluss_write_failed_executions", 0),
            cancelled: builder().counter("fluss_write_cancelled_executions", 0),
            pending: builder().gauge("fluss_write_pending_operations", 0),
            active: builder().gauge("fluss_write_active_executions", 0),
            arrow_bytes: builder().gauge("fluss_write_retained_arrow_bytes", 0),
            encoded_bytes: builder().gauge("fluss_write_encoded_bytes", 0),
            transport_bytes: builder().gauge("fluss_write_transport_bytes", 0),
            routing_bytes: builder().gauge("fluss_write_routing_metadata_bytes", 0),
            routing_scratch_bytes: builder().gauge("fluss_write_routing_scratch_bytes", 0),
            kv_scratch_bytes: builder().gauge("fluss_write_kv_scratch_bytes", 0),
            merge_key_bytes: builder().gauge("fluss_write_merge_key_bytes", 0),
            preparation_time: builder().subset_time("fluss_write_preparation_time", 0),
            metadata_time: builder().subset_time("fluss_write_metadata_time", 0),
            input_wait_time: builder().subset_time("fluss_write_input_wait_time", 0),
            enqueue_time: builder().subset_time("fluss_write_enqueue_time", 0),
            ack_time: builder().subset_time("fluss_write_ack_time", 0),
            cleanup_time: builder().subset_time("fluss_write_cleanup_time", 0),
            set,
        }
    }
}

/// Metric lifetime follows the existing reservation, with no second allocator.
pub(crate) struct TrackedReservation {
    reservation: MemoryReservation,
    gauge: Gauge,
}
impl TrackedReservation {
    pub fn new(reservation: MemoryReservation, gauge: Gauge) -> Self {
        Self { reservation, gauge }
    }
    pub fn size(&self) -> usize {
        self.reservation.size()
    }
    pub fn try_grow(&self, bytes: usize) -> Result<()> {
        self.reservation.try_grow(bytes)?;
        self.gauge.add(bytes);
        Ok(())
    }
    pub fn try_resize(&self, bytes: usize) -> Result<()> {
        let old = self.size();
        self.reservation.try_resize(bytes)?;
        if bytes > old {
            self.gauge.add(bytes - old);
        } else {
            self.gauge.sub(old - bytes);
        }
        Ok(())
    }
}
impl Drop for TrackedReservation {
    fn drop(&mut self) {
        self.gauge.sub(self.size());
    }
}

pub(crate) struct RetainedWriteBytes {
    gauge: Gauge,
    bytes: usize,
}
impl RetainedWriteBytes {
    pub fn new(gauge: Gauge, bytes: usize) -> Self {
        gauge.add(bytes);
        Self { gauge, bytes }
    }
}
impl Drop for RetainedWriteBytes {
    fn drop(&mut self) {
        self.gauge.sub(self.bytes);
    }
}

pub(crate) struct WriteExecution {
    sender: broadcast::Sender<FlussWriteProgress>,
    pub metrics: WriteMetrics,
    summary: FlussWriteSummary,
    attempted: bool,
    terminated: bool,
}
impl WriteExecution {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sender: broadcast::Sender<FlussWriteProgress>,
        metrics: WriteMetrics,
        path: TablePath,
        table_id: i64,
        schema_id: i32,
        operation: FlussWriteOperation,
        acks: &str,
        options: crate::FlussWriteOptions,
    ) -> Result<Self> {
        let execution_id = NEXT_WRITE_EXECUTION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| {
                DataFusionError::Execution("Fluss write execution identity exhausted".into())
            })?;
        let ack = if acks.eq_ignore_ascii_case("all") || acks.parse::<i16>() == Ok(-1) {
            Some(FlussWriteAck::All)
        } else if acks.parse::<i16>() == Ok(1) {
            Some(FlussWriteAck::Leader)
        } else {
            None
        };
        let _ = sender.send(FlussWriteProgress::Initialized {
            execution_id,
            path,
            table_id,
            schema_id,
            operation,
            ack,
            options,
        });
        metrics.active.add(1);
        Ok(Self {
            sender,
            metrics,
            summary: FlussWriteSummary {
                execution_id,
                table_id,
                schema_id,
                operation,
                ack,
                counts: FlussWriteCounts::default(),
                batches_received: 0,
                batches_confirmed: 0,
                status: FlussWriteTermination::Cancelled,
                stage: FlussWriteStage::Preparation,
                input_exhausted: false,
            },
            attempted: false,
            terminated: false,
        })
    }
    pub fn stage(&mut self, stage: FlussWriteStage) {
        self.summary.stage = stage;
    }
    pub fn receive(&mut self, rows: usize) -> Result<()> {
        let rows = rows as u64;
        self.summary.counts.received =
            self.summary
                .counts
                .received
                .checked_add(rows)
                .ok_or_else(|| {
                    DataFusionError::Execution("Fluss write received count overflowed".into())
                })?;
        self.summary.batches_received =
            self.summary
                .batches_received
                .checked_add(1)
                .ok_or_else(|| {
                    DataFusionError::Execution("Fluss write batch identity overflowed".into())
                })?;
        self.summary.counts.pending = rows;
        self.attempted = false;
        self.metrics.received.add(rows as usize);
        self.metrics.pending.add(rows as usize);
        let _ = self.sender.send(FlussWriteProgress::BatchReceived {
            execution_id: self.summary.execution_id,
            batch_id: self.summary.batches_received,
            operations: rows,
            counts: self.summary.counts,
        });
        Ok(())
    }
    pub fn enqueue(&mut self) {
        self.attempted = true;
        self.stage(FlussWriteStage::Enqueue);
    }
    pub fn confirmed(&mut self) {
        self.resolve(FlussWriteBatchOutcome::Confirmed);
        self.summary.batches_confirmed += 1;
        self.metrics.batches_confirmed.add(1);
    }
    fn resolve(&mut self, outcome: FlussWriteBatchOutcome) {
        let rows = self.summary.counts.pending;
        if rows == 0 {
            return;
        }
        self.summary.counts.pending = 0;
        self.metrics.pending.sub(rows as usize);
        match outcome {
            FlussWriteBatchOutcome::Confirmed => {
                self.summary.counts.confirmed += rows;
                self.metrics.confirmed.add(rows as usize);
            }
            FlussWriteBatchOutcome::RejectedBeforeEnqueue => {
                self.summary.counts.rejected_before_enqueue += rows;
                self.metrics.rejected.add(rows as usize);
            }
            FlussWriteBatchOutcome::Uncertain => {
                self.summary.counts.uncertain += rows;
                self.metrics.uncertain.add(rows as usize);
            }
        }
        let _ = self.sender.send(FlussWriteProgress::BatchOutcome {
            execution_id: self.summary.execution_id,
            batch_id: self.summary.batches_received,
            operations: rows,
            outcome,
            counts: self.summary.counts,
        });
    }
    pub fn input_exhausted(&mut self) {
        self.summary.input_exhausted = true;
    }
    pub fn finish(&mut self, status: FlussWriteTermination) {
        if self.terminated {
            return;
        }
        self.resolve(if self.attempted {
            FlussWriteBatchOutcome::Uncertain
        } else {
            FlussWriteBatchOutcome::RejectedBeforeEnqueue
        });
        self.summary.status = status;
        match status {
            FlussWriteTermination::Failed => self.metrics.failed.add(1),
            FlussWriteTermination::Cancelled => self.metrics.cancelled.add(1),
            FlussWriteTermination::Completed => {}
        }
        self.metrics.active.sub(1);
        let _ = self
            .sender
            .send(FlussWriteProgress::Terminated(self.summary.clone()));
        self.terminated = true;
    }
}
impl Drop for WriteExecution {
    fn drop(&mut self) {
        self.finish(if std::thread::panicking() {
            FlussWriteTermination::Failed
        } else {
            FlussWriteTermination::Cancelled
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn execution() -> (WriteExecution, broadcast::Receiver<FlussWriteProgress>) {
        let (sender, receiver) = broadcast::channel(WRITE_OBSERVATION_CAPACITY);
        let execution = WriteExecution::new(
            sender,
            WriteMetrics::default(),
            TablePath::new("db", "table"),
            1,
            1,
            FlussWriteOperation::Append,
            "all",
            crate::FlussWriteOptions::default(),
        )
        .unwrap();
        (execution, receiver)
    }
    #[test]
    fn cancellation_retains_confirmations_and_distinguishes_validation_from_attempted_enqueue() {
        for attempted in [false, true] {
            let (mut execution, mut receiver) = execution();
            let metrics = execution.metrics.clone();
            execution.receive(3).unwrap();
            execution.enqueue();
            execution.confirmed();
            execution.receive(2).unwrap();
            if attempted {
                execution.enqueue();
            }
            drop(execution);
            let summary = loop {
                if let FlussWriteProgress::Terminated(summary) = receiver.try_recv().unwrap() {
                    break summary;
                }
            };
            assert_eq!(summary.status, FlussWriteTermination::Cancelled);
            assert_eq!(summary.counts.confirmed, 3);
            assert_eq!(summary.counts.uncertain, if attempted { 2 } else { 0 });
            assert_eq!(
                summary.counts.rejected_before_enqueue,
                if attempted { 0 } else { 2 }
            );
            assert_eq!(metrics.pending.value(), 0);
            assert_eq!(metrics.active.value(), 0);
        }
    }
    #[test]
    fn lag_is_detectable_and_final_snapshot_preserves_totals_without_batch_history() {
        let (mut execution, mut receiver) = execution();
        for _ in 0..200 {
            execution.receive(1).unwrap();
            execution.enqueue();
            execution.confirmed();
        }
        execution.input_exhausted();
        execution.stage(FlussWriteStage::Cleanup);
        execution.finish(FlussWriteTermination::Completed);
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
        let summary = loop {
            if let FlussWriteProgress::Terminated(summary) = receiver.try_recv().unwrap() {
                break summary;
            }
        };
        assert_eq!(summary.counts.received, 200);
        assert_eq!(summary.counts.confirmed, 200);
        assert_eq!(summary.batches_confirmed, 200);
        assert!(summary.input_exhausted);
    }

    #[test]
    fn idle_cancel_and_cleanup_failure_never_erase_acknowledged_work() {
        for cleanup_error in [false, true] {
            let (mut execution, mut receiver) = execution();
            execution.receive(2).unwrap();
            execution.enqueue();
            execution.confirmed();
            if cleanup_error {
                execution.input_exhausted();
                execution.stage(FlussWriteStage::Cleanup);
                execution.finish(FlussWriteTermination::Failed);
            } else {
                execution.stage(FlussWriteStage::Input);
            }
            drop(execution);
            let summary = loop {
                if let FlussWriteProgress::Terminated(summary) = receiver.try_recv().unwrap() {
                    break summary;
                }
            };
            assert_eq!(summary.counts.confirmed, 2);
            assert_eq!(summary.counts.uncertain, 0);
            assert_eq!(summary.counts.rejected_before_enqueue, 0);
            assert_eq!(summary.input_exhausted, cleanup_error);
        }
    }

    #[test]
    fn repeated_execution_has_unique_ids_without_growing_metric_cardinality() {
        let (sender, _) = broadcast::channel(WRITE_OBSERVATION_CAPACITY);
        let metrics = WriteMetrics::default();
        let count = metrics.set.clone_inner().iter().count();
        let mut ids = std::collections::HashSet::new();
        for _ in 0..200 {
            let mut execution = WriteExecution::new(
                sender.clone(),
                metrics.clone(),
                TablePath::new("db", "table"),
                1,
                1,
                FlussWriteOperation::Upsert,
                "01",
                crate::FlussWriteOptions::default(),
            )
            .unwrap();
            assert!(ids.insert(execution.summary.execution_id));
            assert_eq!(execution.summary.ack, Some(FlussWriteAck::Leader));
            execution.receive(1).unwrap();
            execution.enqueue();
            execution.confirmed();
            execution.input_exhausted();
            execution.finish(FlussWriteTermination::Completed);
        }
        assert_eq!(metrics.set.clone_inner().iter().count(), count);
        assert_eq!(metrics.confirmed.value(), 200);
        assert_eq!(metrics.active.value(), 0);
        assert_eq!(metrics.pending.value(), 0);
    }
}
