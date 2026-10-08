//! Per-split watermark tracking.
//!
//! Each split carries its own monotonic watermark (`max(event_ts) - lag`), so a
//! fast split cannot advance the watermark of a slower one. A record is dropped
//! as late only against **its own split's** watermark, and the input's
//! watermark — the one window operators close against — is the minimum across
//! the input's splits. This keeps valid records of a slower split even when a
//! faster split has already advanced past them.

use arrow::array::BooleanArray;

use hotlap_core::{InputId, SplitId, ZSetBatch};

use crate::error::EngineError;
use crate::ops::filter;
use crate::zset::int64_diffs;

use super::{EngineCore, time};

impl EngineCore {
    /// Drops late insertions and advances `split`'s watermark.
    ///
    /// Only insertions (`diff > 0`) below the split's current watermark are
    /// dropped; a retraction must always be applied or downstream state would
    /// be corrupted. The input's effective watermark is recomputed as the
    /// minimum across the input's seen splits.
    pub(super) fn filter_late(
        &mut self,
        input: InputId,
        split: SplitId,
        batch: &ZSetBatch,
    ) -> Result<ZSetBatch, EngineError> {
        if self.specs.is_empty() {
            return Ok(batch.clone());
        }
        let spec =
            self.specs.get(&input).copied().ok_or_else(|| {
                EngineError::Unsupported(format!("input {input:?} has no watermark"))
            })?;
        let current = self
            .split_watermarks
            .get(&(input, split))
            .copied()
            .unwrap_or(0);
        let times = time::time_values(&batch.batch, spec.time_col)?;
        let diffs = int64_diffs(batch.diff())?;
        let mut mask = Vec::with_capacity(batch.len());
        let mut max_ts = i64::MIN;
        for index in 0..batch.len() {
            let ts = times.value(index);
            max_ts = max_ts.max(ts);
            let late = diffs.value(index) > 0 && ts < current;
            if late {
                *self.late.entry(input).or_insert(0) += 1;
                self.metrics.inc("late_dropped");
            }
            mask.push(!late);
        }
        self.advance_split(input, split, max_ts, spec.lag);
        filter(batch, &BooleanArray::from(mask))
    }

    /// Advances `(input, split)` monotonically, then refreshes the input minimum.
    fn advance_split(&mut self, input: InputId, split: SplitId, max_ts: i64, lag: i64) {
        if max_ts == i64::MIN {
            // Empty batch: do not register a split that produced no timestamp,
            // which would otherwise pin the input minimum at zero.
            return;
        }
        let candidate = max_ts.saturating_sub(lag).max(0);
        let entry = self.split_watermarks.entry((input, split)).or_insert(0);
        *entry = (*entry).max(candidate);
        self.refresh_watermark(input);
    }

    /// Recomputes `input`'s effective watermark as the minimum across its
    /// splits. The map is non-empty for a declared or seen split, so a
    /// not-yet-seen declared split pins the minimum at its initial value.
    pub(super) fn refresh_watermark(&mut self, input: InputId) {
        let minimum = self
            .split_watermarks
            .iter()
            .filter(|((owner, _), _)| *owner == input)
            .map(|(_, watermark)| *watermark)
            .min()
            .unwrap_or(0);
        self.watermarks.insert(input, minimum);
    }
}
