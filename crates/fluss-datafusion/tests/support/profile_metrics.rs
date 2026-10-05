// SPDX-License-Identifier: Apache-2.0
//! Profile-only stable counters/gauges over the existing metrics facade.
//! Histograms are no-op: the profile owns a fixed-size latency histogram instead.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Count(AtomicU64);
impl metrics::CounterFn for Count {
    fn increment(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    fn absolute(&self, n: u64) {
        self.0.fetch_max(n, Ordering::Relaxed);
    }
}
#[derive(Default)]
struct Gauge(AtomicU64);
impl metrics::GaugeFn for Gauge {
    fn increment(&self, n: f64) {
        let _ = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some((f64::from_bits(v) + n).to_bits())
            });
    }
    fn decrement(&self, n: f64) {
        self.increment(-n);
    }
    fn set(&self, n: f64) {
        self.0.store(n.to_bits(), Ordering::Relaxed);
    }
}
#[derive(Default)]
struct State {
    counters: Mutex<HashMap<metrics::Key, Arc<Count>>>,
    gauges: Mutex<HashMap<metrics::Key, Arc<Gauge>>>,
}
#[derive(Clone, Default)]
pub struct ProfileMetrics(Arc<State>);
impl ProfileMetrics {
    pub fn counter(&self, name: &str) -> u64 {
        self.0
            .counters
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .map(|(_, value)| value.0.load(Ordering::Relaxed))
            .sum()
    }
    pub fn gauge(&self, name: &str) -> usize {
        self.0
            .gauges
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .map(|(_, value)| f64::from_bits(value.0.load(Ordering::Relaxed)).max(0.0) as usize)
            .max()
            .unwrap_or(0)
    }
}
impl metrics::Recorder for ProfileMetrics {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::from_arc(Arc::clone(
            self.0
                .counters
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_default(),
        ))
    }
    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::from_arc(Arc::clone(
            self.0
                .gauges
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_default(),
        ))
    }
    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}
