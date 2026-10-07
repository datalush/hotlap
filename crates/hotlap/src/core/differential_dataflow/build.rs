//! Dataflow construction: wire declared inputs and views into one timely scope.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use differential_dataflow::VecCollection;
use differential_dataflow::input::{Input, InputSession};
use timely::worker::Worker;

use super::circuit::{compile, sources};
use super::session::{Changes, Running, State, ViewState};
use crate::core::{CoreError, InputId, ViewId, WatermarkSpec};
use crate::plan::Plan;
use crate::row::Row;

/// Build the single scope holding every declared input and view.
pub(super) fn build_dataflow(
    worker: &mut Worker,
    inputs: &[InputId],
    views: &[(ViewId, Plan)],
    watermarks: &HashMap<InputId, WatermarkSpec>,
    tapped: &HashSet<ViewId>,
) -> Result<Running, CoreError> {
    if !watermarks.is_empty() && watermarks.len() != inputs.len() {
        return Err(CoreError::Unsupported(
            "cannot mix inputs with and without a declared watermark".into(),
        ));
    }
    let event_time = !watermarks.is_empty();
    if !event_time && views.iter().any(|(_, plan)| crate::plan::has_window(plan)) {
        return Err(CoreError::Unsupported(
            "tumbling windows require event-time inputs (declare_watermark)".into(),
        ));
    }
    Ok(worker.dataflow::<u64, _, _>(|scope| {
        let mut sessions: HashMap<InputId, InputSession<u64, Row, isize>> = HashMap::new();
        let mut collections: HashMap<InputId, VecCollection<'_, u64, Row, isize>> = HashMap::new();
        for &id in inputs {
            let (session, coll) = scope.new_collection::<Row, isize>();
            sessions.insert(id, session);
            collections.insert(id, coll);
        }
        let (view_states, consumers) = assemble_views(&collections, views, tapped);
        Running {
            inputs: sessions,
            views: view_states,
            consumers,
            registered: inputs.iter().copied().collect(),
            arities: HashMap::new(),
            event_time,
            watermarks: watermarks.clone(),
            watermarks_now: inputs.iter().map(|&i| (i, 0u64)).collect(),
            frontier_now: inputs.iter().map(|&i| (i, 0u64)).collect(),
            late: HashMap::new(),
        }
    }))
}

/// Compile every plan and collect its output probe, state and consumer index.
fn assemble_views<'scope>(
    collections: &HashMap<InputId, VecCollection<'scope, u64, Row, isize>>,
    views: &[(ViewId, Plan)],
    tapped: &HashSet<ViewId>,
) -> (HashMap<ViewId, ViewState>, HashMap<InputId, Vec<ViewId>>) {
    let mut view_states: HashMap<ViewId, ViewState> = HashMap::new();
    let mut consumers: HashMap<InputId, Vec<ViewId>> = HashMap::new();
    for (view, plan) in views {
        let state: State = Rc::new(RefCell::new(BTreeMap::new()));
        let changes: Changes = Rc::new(RefCell::new(Vec::new()));
        let sink = state.clone();
        let delta_sink = changes.clone();
        let is_tapped = tapped.contains(view);
        let (probe, _out) = compile(collections, plan)
            .inspect(move |update| {
                let (row, _time, diff) = update;
                *sink.borrow_mut().entry(row.clone()).or_insert(0) += *diff as i64;
                if is_tapped {
                    delta_sink.borrow_mut().push((row.clone(), *diff as i64));
                }
            })
            .probe();
        for src in sources(plan) {
            consumers.entry(src).or_default().push(*view);
        }
        view_states.insert(
            *view,
            ViewState {
                probe,
                state,
                changes,
                plan: plan.clone(),
            },
        );
    }
    (view_states, consumers)
}
