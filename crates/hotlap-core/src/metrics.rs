//! In-process metrics registry.
//!
//! [`MetricsRegistry`] owns a set of named counters and gauges backed by
//! atomics, so it can be shared across threads (wrap it in an [`Arc`]) and
//! updated through `&self`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// A lock-free handle to one metric cell.
///
/// Resolving a name through [`MetricsRegistry::metric`] takes the registry lock
/// once and caches the underlying atomic; afterwards [`inc`](Metric::inc),
/// [`add`](Metric::add) and [`set`](Metric::set) touch only that atomic. A hot
/// loop should resolve its handles once and reuse them, so updates never take
/// the registry lock.
#[derive(Clone, Debug)]
pub struct Metric {
    cell: Arc<AtomicU64>,
}

impl Metric {
    /// Increments the counter by one.
    pub fn inc(&self) {
        self.add(1);
    }

    /// Adds `n` to the counter.
    pub fn add(&self, n: u64) {
        self.cell.fetch_add(n, Ordering::Relaxed);
    }

    /// Sets the gauge to `value`, replacing any previous value.
    pub fn set(&self, value: u64) {
        self.cell.store(value, Ordering::Relaxed);
    }

    /// Current value of the cell.
    pub fn get(&self) -> u64 {
        self.cell.load(Ordering::Relaxed)
    }
}

/// A registry of named counters and gauges.
///
/// Counters are advanced with [`inc`](MetricsRegistry::inc) and
/// [`add`](MetricsRegistry::add); gauges are overwritten with
/// [`set`](MetricsRegistry::set). Each metric lives in its own atomic cell, but
/// the name-to-cell map is behind a [`Mutex`]: every name lookup takes that
/// short lock to resolve the cell. Callers in a hot path should resolve a
/// [`Metric`] handle once via [`metric`](MetricsRegistry::metric) and update it
/// lock-free. [`snapshot`](MetricsRegistry::snapshot) takes the lock to read a
/// consistent view.
///
/// ```
/// use hotlap_core::MetricsRegistry;
///
/// let metrics = MetricsRegistry::new();
/// metrics.inc("rows_ingested");
/// metrics.add("rows_ingested", 4);
/// metrics.set("windows_open", 3);
///
/// // A cached handle skips the registry lock on every update.
/// let rows = metrics.metric("rows_ingested");
/// rows.add(1);
///
/// let snap = metrics.snapshot();
/// assert_eq!(snap["rows_ingested"], 6);
/// assert_eq!(snap["windows_open"], 3);
/// ```
#[derive(Debug, Default)]
pub struct MetricsRegistry {
    inner: Mutex<BTreeMap<String, Arc<AtomicU64>>>,
}

impl MetricsRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolves `name` to a lock-free [`Metric`] handle, creating the cell if
    /// absent. Cache the handle to update without taking the registry lock.
    pub fn metric(&self, name: &str) -> Metric {
        Metric {
            cell: self.cell(name),
        }
    }

    /// Returns the atomic cell for `name`, creating it if absent.
    fn cell(&self, name: &str) -> Arc<AtomicU64> {
        let mut map = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        map.entry(name.to_owned())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    /// Increments the counter `name` by one.
    pub fn inc(&self, name: &str) {
        self.add(name, 1);
    }

    /// Adds `n` to the counter `name`.
    pub fn add(&self, name: &str, n: u64) {
        self.metric(name).add(n);
    }

    /// Sets the gauge `name` to `value`, replacing any previous value.
    pub fn set(&self, name: &str, value: u64) {
        self.metric(name).set(value);
    }

    /// Returns a point-in-time copy of every metric, ordered by name.
    pub fn snapshot(&self) -> BTreeMap<String, u64> {
        let map = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        map.iter()
            .map(|(name, cell)| (name.clone(), cell.load(Ordering::Relaxed)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_gauges_snapshot() {
        let m = MetricsRegistry::default();
        m.inc("rows_ingested");
        m.add("rows_ingested", 4);
        m.set("windows_open", 3);
        let s = m.snapshot();
        assert_eq!(s.get("rows_ingested"), Some(&5));
        assert_eq!(s.get("windows_open"), Some(&3));
    }

    #[test]
    fn snapshot_is_name_ordered() {
        let m = MetricsRegistry::new();
        m.inc("zeta");
        m.inc("alpha");
        let names: Vec<_> = m.snapshot().into_keys().collect();
        assert_eq!(names, vec!["alpha", "zeta"]);
    }

    #[test]
    fn cached_metric_handle_updates_the_snapshot() {
        let m = MetricsRegistry::new();
        let handle = m.metric("hits");
        handle.add(2);
        handle.inc();
        handle.set(9);
        assert_eq!(handle.get(), 9);
        // The handle resolves and registers the cell in the same registry.
        assert_eq!(m.snapshot().get("hits"), Some(&9));
    }

    #[test]
    fn shared_registry_updates_across_threads() {
        let m = Arc::new(MetricsRegistry::new());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let m = Arc::clone(&m);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        m.inc("hits");
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker thread panicked");
        }
        assert_eq!(m.snapshot().get("hits"), Some(&400));
    }
}
