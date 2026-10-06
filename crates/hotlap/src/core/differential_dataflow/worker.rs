//! Worker-thread loop and per-command handlers for the differential core.

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use differential_dataflow::input::Input;
use timely::worker::Worker;

use super::{Command, State, ViewState, circuit};
use crate::core::{CoreError, ViewId};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row};

/// Worker-thread body: own the timely worker, build views on demand and step
/// continuously so arrangements are retained across pushes.
pub(super) fn run_worker(rx: mpsc::Receiver<Command>) {
    // `execute_directly` requires the closure to be `Sync`; `Receiver` is not, so guard it.
    let rx = Mutex::new(rx);
    timely::execute_directly(move |worker| {
        let mut views: HashMap<ViewId, ViewState> = HashMap::new();
        loop {
            worker.step();
            let cmd = rx.lock().unwrap().recv_timeout(Duration::from_millis(1));
            match cmd {
                Ok(Command::Build { view, plan, reply }) => {
                    let _ = reply.send(handle_build(worker, &mut views, view, plan));
                }
                Ok(Command::Push { view, batch, reply }) => {
                    let _ = reply.send(handle_push(worker, &mut views, view, batch));
                }
                Ok(Command::Snapshot { view, reply }) => {
                    let _ = reply.send(handle_snapshot(&views, view));
                }
                Ok(Command::Shutdown { reply }) => {
                    let _ = reply.send(());
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    });
}

/// Build a new view's circuit and register it, rejecting duplicate ids.
fn handle_build(
    worker: &mut Worker,
    views: &mut HashMap<ViewId, ViewState>,
    view: ViewId,
    plan: Plan,
) -> Result<(), CoreError> {
    match views.entry(view) {
        Entry::Occupied(_) => Err(CoreError::Unsupported(format!(
            "view {view:?} already built"
        ))),
        Entry::Vacant(slot) => {
            let (input, probe, state) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, coll) = scope.new_collection::<Row, isize>();
                let state: State = Rc::new(RefCell::new(BTreeMap::new()));
                let sink = state.clone();
                let (probe, _out) = circuit::compile(coll, &plan)
                    .inspect(move |update| {
                        let (row, _time, diff) = update;
                        let mut store = sink.borrow_mut();
                        *store.entry(row.clone()).or_insert(0) += *diff as i64;
                    })
                    .probe();
                (input, probe, state)
            });
            slot.insert(ViewState {
                input,
                probe,
                state,
                next_time: 0,
                plan,
            });
            Ok(())
        }
    }
}

/// Feed a batch into an existing view's live session.
fn handle_push(
    worker: &mut Worker,
    views: &mut HashMap<ViewId, ViewState>,
    view: ViewId,
    batch: ChangeBatch,
) -> Result<(), CoreError> {
    match views.get_mut(&view) {
        None => Err(CoreError::Unsupported(format!("unknown view {view:?}"))),
        Some(vs) => circuit::push_batch(worker, vs, &batch),
    }
}

/// Read the non-zero consolidated Z-set of an existing view.
fn handle_snapshot(
    views: &HashMap<ViewId, ViewState>,
    view: ViewId,
) -> Result<Vec<Row>, CoreError> {
    match views.get(&view) {
        None => Err(CoreError::Unsupported(format!("unknown view {view:?}"))),
        Some(vs) => {
            let store = vs.state.borrow();
            Ok(store
                .iter()
                .filter(|(_, diff)| **diff != 0)
                .map(|(row, _)| row.clone())
                .collect())
        }
    }
}
