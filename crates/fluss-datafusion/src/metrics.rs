// SPDX-License-Identifier: Apache-2.0
//! Expose Fluss scan metrics through DataFusion's EXPLAIN ANALYZE.

use std::fmt;
use std::sync::Arc;

use datafusion::common::format::{MetricCategory, MetricType};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::statistics::StatisticsArgs;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, ReplaceChildrenOptions,
    SendableRecordBatchStream,
};

#[derive(Debug)]
pub(crate) struct FlussScanExec {
    inner: Arc<StreamingTableExec>,
    metrics: ExecutionPlanMetricsSet,
    groups: Vec<Vec<i32>>,
}

impl FlussScanExec {
    pub(crate) fn new(
        inner: StreamingTableExec,
        metrics: ExecutionPlanMetricsSet,
        groups: Vec<Vec<i32>>,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            metrics,
            groups,
        }
    }
}

impl DisplayAs for FlussScanExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlussScanExec: bucket_groups={:?}", self.groups)
    }
}

impl ExecutionPlan for FlussScanExec {
    fn name(&self) -> &'static str {
        "FlussScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "FlussScanExec is a leaf plan".into(),
            ));
        }
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "FlussScanExec is a leaf plan".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, ctx)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_from_inputs(
        &self,
        stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.inner.statistics_from_inputs(stats, args)
    }
}

#[derive(Clone)]
pub(crate) struct PartitionMetrics {
    pub(crate) output_rows: Count,
    pub(crate) arrow_output_bytes: Count,
    pub(crate) arrow_decoded_bytes: Count,
    pub(crate) output_batches: Count,
    pub(crate) capture_time: Time,
    pub(crate) read_time: Time,
    pub(crate) peak_batch_bytes: Gauge,
    pub(crate) active_streams: Gauge,
}

impl PartitionMetrics {
    pub(crate) fn new(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        bucket_ids: &[i32],
    ) -> Self {
        let ids = bucket_ids
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let builder = || {
            MetricBuilder::new(metrics)
                .with_type(MetricType::Summary)
                .with_new_label("buckets", ids.clone())
        };
        builder()
            .with_category(MetricCategory::Rows)
            .counter("fluss_buckets_assigned", partition)
            .add(bucket_ids.len());
        Self {
            output_rows: builder().output_rows(partition),
            arrow_output_bytes: builder()
                .with_category(MetricCategory::Bytes)
                .counter("arrow_output_bytes", partition),
            arrow_decoded_bytes: builder()
                .with_category(MetricCategory::Bytes)
                .counter("arrow_decoded_bytes", partition),
            output_batches: builder().output_batches(partition),
            capture_time: builder().subset_time("fluss_offset_capture_time", partition),
            read_time: builder().subset_time("fluss_read_time", partition),
            peak_batch_bytes: builder()
                .peak_memory_usage("fluss_peak_decoded_arrow_batch_bytes", partition),
            active_streams: builder()
                .with_category(MetricCategory::Rows)
                .gauge("fluss_active_partition_streams", partition),
        }
    }
}

/// The stream owns this guard; dropping a cancelled/failed stream releases it.
pub(crate) struct ReaderLifetime(Gauge);

impl ReaderLifetime {
    pub(crate) fn new(active: Gauge) -> Self {
        active.add(1);
        Self(active)
    }
}

impl Drop for ReaderLifetime {
    fn drop(&mut self) {
        self.0.sub(1);
    }
}
