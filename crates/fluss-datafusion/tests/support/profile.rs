// SPDX-License-Identifier: Apache-2.0
//! Fixed-size, millisecond-upper-bound latency histogram for load profiles.
pub struct Latencies {
    bins: Vec<u64>,
    count: u64,
    pub overflow: u64,
}
impl Latencies {
    pub fn new(max_ms: usize) -> Self {
        Self {
            bins: vec![0; max_ms + 1],
            count: 0,
            overflow: 0,
        }
    }
    pub fn record(&mut self, elapsed: std::time::Duration) {
        let ms = elapsed.as_micros().div_ceil(1000) as usize;
        let last = self.bins.len() - 1;
        self.overflow += u64::from(ms > last);
        self.bins[ms.min(last)] += 1;
        self.count += 1;
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    pub fn percentile(&self, percent: u64) -> usize {
        let rank = (self.count * percent).div_ceil(100).max(1);
        let mut cumulative = 0;
        for (ms, count) in self.bins.iter().enumerate() {
            cumulative += count;
            if cumulative >= rank {
                return ms;
            }
        }
        0
    }
}
