//! Engine-side metric increments.
//!
//! The counters live in the shared [`MetricsRegistry`], so the runtime that
//! drives this core observes the same snapshot. Updates resolve a cell under the
//! registry lock, so they happen once per batch: the per-row loops accumulate
//! locally and publish a single total.

use std::sync::Arc;

use hotlap_core::MetricsRegistry;

use super::EngineCore;

impl EngineCore {
    /// Creates an empty core that reports into `metrics`.
    ///
    /// The runtime builds the core this way so the same registry can also be
    /// handed to the sink tasks and the outer handle.
    pub fn with_metrics(metrics: Arc<MetricsRegistry>) -> Self {
        Self {
            metrics,
            ..Self::new()
        }
    }

    /// The registry this core reports into.
    pub fn metrics(&self) -> Arc<MetricsRegistry> {
        Arc::clone(&self.metrics)
    }

    /// Counts the rows received by one push (`rows_ingested`).
    pub(super) fn count_ingested(&self, rows: usize) {
        self.metrics.add("rows_ingested", rows as u64);
    }

    /// Publishes window drops newly observed since the last push.
    ///
    /// Window counters are cumulative per view, so the registry is advanced by
    /// the delta rather than the running total.
    pub(super) fn refresh_late_closed(&mut self) {
        let total = self.late_closed_total();
        let delta = total.saturating_sub(self.late_closed_seen);
        self.late_closed_seen = total;
        self.metrics.add("late_closed_dropped", delta);
    }

    /// Sum of `late_closed_dropped` over every built view.
    pub(super) fn late_closed_total(&self) -> u64 {
        self.views
            .values()
            .map(|view| view.graph.window_late_closed_dropped())
            .sum()
    }
}
