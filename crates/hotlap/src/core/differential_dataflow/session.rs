//! Live session state for the shared dataflow: schema phase and running maps.
//!
//! Split out of `mod.rs` so the worker and circuit can share the state types
//! without any single file growing past the size budget.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use differential_dataflow::input::InputSession;
use timely::dataflow::operators::probe::Handle;

use crate::core::{CoreError, InputId, ViewId};
use crate::plan::Plan;
use crate::row::Row;

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
}

/// Schema state: declarations accumulate, then freeze on the first push.
pub(super) enum Phase {
    Building {
        inputs: Vec<InputId>,
        views: Vec<(ViewId, Plan)>,
    },
    Running(Running),
}

impl Running {
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
}
