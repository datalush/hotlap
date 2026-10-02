// SPDX-License-Identifier: Apache-2.0
//! Leaf execution plan that exposes scan metrics to DataFusion.

use std::fmt;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
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
    source: String,
}

impl FlussScanExec {
    pub(crate) fn new(
        inner: StreamingTableExec,
        metrics: ExecutionPlanMetricsSet,
        groups: Vec<Vec<i32>>,
        source: String,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            metrics,
            groups,
            source,
        }
    }
}

impl DisplayAs for FlussScanExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FlussScanExec: {}, bucket_groups={:?}",
            self.source, self.groups
        )
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
