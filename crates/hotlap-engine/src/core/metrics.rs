//! Engine-side metric increments.
//!
//! The counters live in the shared [`MetricsRegistry`], so the runtime that
//! drives this core observes the same snapshot. Updates go through handles
//! resolved once at construction, so a push never takes the registry lock.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use hotlap_core::{Metric, MetricsRegistry};

use super::EngineCore;
use super::retention::InputRetention;

/// Shared registry plus the cached lock-free handles for the hot per-push
/// counters, resolved once so a push never takes the registry lock.
pub(crate) struct MetricHandles {
    /// Shared counter registry; the runtime injects the same `Arc` so both see
    /// one consistent snapshot.
    pub(crate) registry: Arc<MetricsRegistry>,
    /// New rows received by a push (`rows_ingested`).
    pub(crate) rows_ingested: Metric,
    /// Rows emitted by a view's graph (`rows_emitted`).
    pub(crate) rows_emitted: Metric,
    /// Input rows dropped as late (`late_dropped`).
    pub(crate) late_dropped: Metric,
    /// Window deltas dropped after their window closed.
    pub(crate) late_closed_dropped: Metric,
    /// Highest `late_closed_dropped` total already published, so a push only
    /// adds the new window drops instead of the whole history.
    pub(crate) late_closed_seen: u64,
    /// Open tumbling windows across every built view, published as a gauge.
    pub(crate) windows_open: Metric,
}

impl MetricHandles {
    /// Resolves every hot-path handle from `registry` once.
    pub(crate) fn new(registry: Arc<MetricsRegistry>) -> Self {
        Self {
            rows_ingested: registry.metric("rows_ingested"),
            rows_emitted: registry.metric("rows_emitted"),
            late_dropped: registry.metric("late_dropped"),
            late_closed_dropped: registry.metric("late_closed_dropped"),
            windows_open: registry.metric("windows_open"),
            registry,
            late_closed_seen: 0,
        }
    }
}

impl EngineCore {
    /// Creates an empty core reporting into `metrics`, resolving the hot-path
    /// handle cache once so later updates are lock-free.
    pub(super) fn with_registry(metrics: Arc<MetricsRegistry>) -> Self {
        Self {
            views: HashMap::new(),
            registered: HashSet::new(),
            specs: HashMap::new(),
            watermarks: HashMap::new(),
            split_watermarks: HashMap::new(),
            schemas: HashMap::new(),
            late: HashMap::new(),
            frozen: false,
            epoch: 0,
            retention: InputRetention::disabled(),
            failed: false,
            metrics: MetricHandles::new(metrics),
        }
    }

    /// Creates an empty core that reports into `metrics`.
    ///
    /// The runtime builds the core this way so the same registry can also be
    /// handed to the sink tasks and the outer handle.
    pub fn with_metrics(metrics: Arc<MetricsRegistry>) -> Self {
        Self::with_registry(metrics)
    }

    /// The registry this core reports into.
    pub fn metrics(&self) -> Arc<MetricsRegistry> {
        Arc::clone(&self.metrics.registry)
    }

    /// Counts the rows received by one push (`rows_ingested`).
    pub(super) fn count_ingested(&self, rows: usize) {
        self.metrics.rows_ingested.add(rows as u64);
    }

    /// Publishes the open tumbling windows across every built view
    /// (`windows_open`).
    pub(super) fn refresh_windows_open(&self) {
        let total: u64 = self
            .views
            .values()
            .map(|view| view.graph.windows_open())
            .sum();
        self.metrics.windows_open.set(total);
    }

    /// Publishes window drops newly observed since the last push.
    ///
    /// Window counters are cumulative per view, so the registry is advanced by
    /// the delta rather than the running total.
    pub(super) fn refresh_late_closed(&mut self) {
        let total = self.late_closed_total();
        let delta = total.saturating_sub(self.metrics.late_closed_seen);
        self.metrics.late_closed_seen = total;
        self.metrics.late_closed_dropped.add(delta);
    }

    /// Sum of `late_closed_dropped` over every built view.
    pub(super) fn late_closed_total(&self) -> u64 {
        self.views
            .values()
            .map(|view| view.graph.window_late_closed_dropped())
            .sum()
    }
}
