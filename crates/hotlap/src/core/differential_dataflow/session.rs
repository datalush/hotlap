//! Live session state for the shared dataflow: schema phase and running maps.
//!
//! Split out of `mod.rs` so the worker and circuit can share the state types
//! without any single file growing past the size budget.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use differential_dataflow::input::InputSession;
use timely::dataflow::operators::probe::Handle;

use crate::core::{CoreError, InputId, ViewId, WatermarkSpec};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row, Scalar};

/// Factor between the logical event-time watermark (unscaled, batch-independent)
/// and the DD frontier time. Each kept push advances the fed input's frontier by
/// at least one scaled step, so records inserted at the current logical time are
/// always strictly below the new frontier and visible in the same push.
pub(super) const TIME_SCALE: u64 = 1 << 20;

/// Consolidated output Z-set of a view, accumulated from the output stream.
/// Single-threaded: only the worker thread touches it, so `Rc`/`RefCell` suffice.
pub(super) type State = Rc<RefCell<BTreeMap<Row, i64>>>;

/// A compiled view: its output probe, accumulated state and plan (for validation).
pub(super) struct ViewState {
    pub(super) probe: Handle<u64>,
    pub(super) state: State,
    pub(super) plan: Plan,
}

/// Live half of the core: the single dataflow's inputs, views and drain index.
pub(super) struct Running {
    pub(super) inputs: HashMap<InputId, InputSession<u64, Row, isize>>,
    pub(super) views: HashMap<ViewId, ViewState>,
    /// Views that read each input, used to wait on the right probes per push.
    pub(super) consumers: HashMap<InputId, Vec<ViewId>>,
    pub(super) registered: HashSet<InputId>,
    /// Row length observed for each input, learned on its first push. Used to
    /// validate plan indices (including above joins) as data arrives.
    pub(super) arities: HashMap<InputId, usize>,
    pub(super) event_time: bool,
    pub(super) watermarks: HashMap<InputId, WatermarkSpec>,
    /// Logical, batch-independent watermark per input (unscaled event time).
    pub(super) watermarks_now: HashMap<InputId, u64>,
    /// Actual DD frontier per input, scaled by [`TIME_SCALE`]; advances on every
    /// push so same-push records are visible, independent of the logical watermark.
    pub(super) frontier_now: HashMap<InputId, u64>,
    pub(super) late: HashMap<InputId, u64>,
}

/// Schema state: declarations accumulate, then freeze on the first push.
pub(super) enum Phase {
    Building {
        inputs: Vec<InputId>,
        views: Vec<(ViewId, Plan)>,
        watermarks: HashMap<InputId, WatermarkSpec>,
    },
    Running(Running),
}

impl Running {
    /// Learn `input`'s row arity from `batch`, rejecting a batch inconsistent with
    /// a previously observed arity. Empty batches learn nothing.
    pub(super) fn learn_arity(
        &mut self,
        input: InputId,
        batch: &ChangeBatch,
    ) -> Result<(), CoreError> {
        for (row, _) in &batch.rows {
            let arity = row.0.len();
            if let Some(&known) = self.arities.get(&input)
                && known != arity
            {
                return Err(CoreError::Unsupported(format!(
                    "input {input:?} row arity {arity} inconsistent with known arity {known}"
                )));
            }
            self.arities.insert(input, arity);
        }
        Ok(())
    }

    /// Read the non-zero consolidated Z-set of an existing view.
    pub(super) fn snapshot(&self, view: ViewId) -> Result<Vec<Row>, CoreError> {
        let vs = self
            .views
            .get(&view)
            .ok_or_else(|| CoreError::Unsupported(format!("unknown view {view:?}")))?;
        let store = vs.state.borrow();
        Ok(store
            .iter()
            .filter(|(_, diff)| **diff != 0)
            .map(|(row, _)| row.clone())
            .collect())
    }

    /// En modo event-time, separa las filas no tardías (devueltas) y avanza el
    /// watermark lógico de `input` de forma monótona y **independiente del batching**:
    /// `next = current.max((max_ts - lag).max(0))` si se conserva alguna fila, o
    /// `current` si no. El descarte de tardíos compara contra el `current` previo.
    /// La frontera DD (visibilidad) se gestiona aparte en `frontier_now`. En modo
    /// epoch no toca nada.
    pub(super) fn filter_late(
        &mut self,
        input: InputId,
        batch: &ChangeBatch,
    ) -> (ChangeBatch, u64) {
        let current = *self.watermarks_now.get(&input).unwrap_or(&0);
        if !self.event_time {
            return (batch.clone(), current);
        }
        let spec = self.watermarks[&input];
        let mut kept = ChangeBatch::default();
        let mut max_ts = i64::MIN;
        for (row, diff) in &batch.rows {
            let ts = time_of(row, spec.time_col);
            max_ts = max_ts.max(ts);
            if (ts as u64) < current {
                *self.late.entry(input).or_insert(0) += 1;
            } else {
                kept.push(row.clone(), *diff);
            }
        }
        let next = if kept.rows.is_empty() {
            current
        } else {
            current.max((max_ts - spec.lag).max(0) as u64)
        };
        self.watermarks_now.insert(input, next);
        (kept, next)
    }
}

/// Lee la columna de event-time como `i64` no negativo; `Null`/otro tipo y los
/// negativos se tratan como 0.
fn time_of(row: &Row, col: usize) -> i64 {
    match row.col(col) {
        Scalar::I64(v) => v.max(0),
        _ => 0,
    }
}
