//! Engine-side metric increments.
//!
//! The counters live in the shared [`MetricsRegistry`], so the runtime that
//! drives this core observes the same snapshot. Updates go through handles
//! resolved once at construction, so a push never takes the registry lock.

use std::sync::Arc;

use hotlap_core::MetricsRegistry;

use super::EngineCore;

impl EngineCore {
    /// Creates an empty core that reports into `metrics`.
    ///
    /// The runtime builds the core this way so the same registry can also be
    /// handed to the sink tasks and the outer handle.
    pub fn with_metrics(metrics: Arc<MetricsRegistry>) -> Self {
        Self::with_registry(metrics)
    }

    /// The registry this core reports into.
    pub fn metrics(&self) -> Arc<MetricsRegistry> {
        Arc::clone(&self.metrics)
    }

    /// Counts the rows received by one push (`rows_ingested`).
    pub(super) fn count_ingested(&self, rows: usize) {
        self.rows_ingested.add(rows as u64);
    }

    /// Publishes the open tumbling windows across every built view
    /// (`windows_open`).
    pub(super) fn refresh_windows_open(&self) {
        let total: u64 = self
            .views
            .values()
            .map(|view| view.graph.windows_open())
            .sum();
        self.windows_open.set(total);
    }

    /// Publishes window drops newly observed since the last push.
    ///
    /// Window counters are cumulative per view, so the registry is advanced by
    /// the delta rather than the running total.
    pub(super) fn refresh_late_closed(&mut self) {
        let total = self.late_closed_total();
        let delta = total.saturating_sub(self.late_closed_seen);
        self.late_closed_seen = total;
        self.late_closed_dropped.add(delta);
    }

    /// Sum of `late_closed_dropped` over every built view.
    pub(super) fn late_closed_total(&self) -> u64 {
        self.views
            .values()
            .map(|view| view.graph.window_late_closed_dropped())
            .sum()
    }
}
