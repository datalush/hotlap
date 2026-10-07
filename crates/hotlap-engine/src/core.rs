//! Arrow-native [`IncrementalCore`]: recompute each view from accumulated inputs.

mod traits;

use std::collections::{HashMap, HashSet};

use arrow::array::BooleanArray;

use hotlap_core::plan::has_window;
use hotlap_core::{CoreError, InputId, Plan, ViewId, WatermarkSpec, ZSetBatch};

use crate::core_eval::{eval_plan, time_values};
use crate::error::EngineError;
use crate::ops::filter;
use crate::zset::int64_diffs;

/// A view's compiled plan plus its last output and last drained output.
pub(super) struct ViewState {
    pub(super) plan: Plan,
    pub(super) tapped: bool,
    pub(super) current: Option<ZSetBatch>,
    pub(super) drained: Option<ZSetBatch>,
}

/// Differential-dataflow-free engine kernel.
///
/// Inputs accumulate consolidated Z-sets. On every push each view is recomputed
/// from the accumulated inputs, so its output is the full relation for the
/// current inputs; changelogs are derived by diffing successive relations.
pub struct EngineCore {
    pub(super) inputs: HashMap<InputId, ZSetBatch>,
    pub(super) views: HashMap<ViewId, ViewState>,
    pub(super) registered: HashSet<InputId>,
    pub(super) specs: HashMap<InputId, WatermarkSpec>,
    pub(super) watermarks: HashMap<InputId, i64>,
    pub(super) late: HashMap<InputId, u64>,
    pub(super) frozen: bool,
}

impl EngineCore {
    /// Creates an empty core with no inputs, views or watermarks.
    pub fn new() -> Self {
        Self {
            inputs: HashMap::new(),
            views: HashMap::new(),
            registered: HashSet::new(),
            specs: HashMap::new(),
            watermarks: HashMap::new(),
            late: HashMap::new(),
            frozen: false,
        }
    }

    /// Freezes the schema: rejects mixed watermark declarations and windowed
    /// views without event-time, then blocks further declarations.
    pub(super) fn freeze(&mut self) -> Result<(), CoreError> {
        if !self.specs.is_empty() && self.specs.len() != self.registered.len() {
            return Err(CoreError::Unsupported(
                "cannot mix inputs with and without a declared watermark".into(),
            ));
        }
        let event_time = !self.specs.is_empty();
        if !event_time && self.views.values().any(|view| has_window(&view.plan)) {
            return Err(CoreError::Unsupported(
                "tumbling windows require event-time inputs (declare_watermark)".into(),
            ));
        }
        self.frozen = true;
        Ok(())
    }

    /// Drops late insertions and advances `input`'s logical watermark.
    ///
    /// Only insertions (`diff > 0`) below the current watermark are dropped; a
    /// retraction must always be applied or downstream state would be corrupted.
    pub(super) fn filter_late(
        &mut self,
        input: InputId,
        batch: &ZSetBatch,
    ) -> Result<ZSetBatch, EngineError> {
        if self.specs.is_empty() {
            return Ok(batch.clone());
        }
        let spec = self.specs.get(&input).copied().ok_or_else(|| {
            EngineError::Unsupported(format!("input {input:?} has no watermark"))
        })?;
        let current = *self.watermarks.get(&input).unwrap_or(&0);
        let times = time_values(&batch.batch, spec.time_col)?;
        let diffs = int64_diffs(batch.diff())?;
        let mut mask = Vec::with_capacity(batch.len());
        let mut max_ts = i64::MIN;
        for index in 0..batch.len() {
            let ts = times.value(index);
            max_ts = max_ts.max(ts);
            let late = diffs.value(index) > 0 && ts < current;
            if late {
                *self.late.entry(input).or_insert(0) += 1;
            }
            mask.push(!late);
        }
        let kept = filter(batch, &BooleanArray::from(mask))?;
        let next = if kept.is_empty() {
            current
        } else {
            current.max((max_ts - spec.lag).max(0))
        };
        self.watermarks.insert(input, next);
        Ok(kept)
    }

    /// Recomputes every view from the accumulated inputs.
    pub(super) fn refresh_views(&mut self) -> Result<(), CoreError> {
        let ids: Vec<ViewId> = self.views.keys().copied().collect();
        for id in ids {
            let plan = self.views[&id].plan.clone();
            let current = eval_plan(&plan, &self.inputs, &self.watermarks)?;
            if let Some(view) = self.views.get_mut(&id) {
                view.current = current;
            }
        }
        Ok(())
    }
}

impl Default for EngineCore {
    fn default() -> Self {
        Self::new()
    }
}
