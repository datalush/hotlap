//! In-process metrics registry.
//!
//! [`MetricsRegistry`] owns a set of named counters and gauges backed by
//! atomics, so it can be shared across threads (wrap it in an [`Arc`]) and
//! updated through `&self`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// A registry of named counters and gauges.
///
/// Counters are advanced with [`inc`](MetricsRegistry::inc) and
/// [`add`](MetricsRegistry::add); gauges are overwritten with
/// [`set`](MetricsRegistry::set). Each metric lives in its own atomic cell, but
/// the name-to-cell map is behind a [`Mutex`]: every update takes that short
/// lock to resolve the cell and then updates the atomic. Callers must therefore
/// publish values at coarse granularity — once per batch, never once per row.
/// [`snapshot`](MetricsRegistry::snapshot) also takes the lock to read a
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
/// let snap = metrics.snapshot();
/// assert_eq!(snap["rows_ingested"], 5);
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
        self.cell(name).fetch_add(n, Ordering::Relaxed);
    }

    /// Sets the gauge `name` to `value`, replacing any previous value.
    pub fn set(&self, name: &str, value: u64) {
        self.cell(name).store(value, Ordering::Relaxed);
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
