//! Worker-thread loop and command handlers for the differential core.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use differential_dataflow::VecCollection;
use differential_dataflow::input::{Input, InputSession};
use timely::worker::Worker;

use super::session::{Phase, Running, State, ViewState};
use super::{Command, circuit};
use crate::core::{CoreError, InputId, ViewId};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row};

/// Worker-thread body: own the timely worker, build the one dataflow on the
/// first push and step continuously so arrangements are retained.
pub(super) fn run_worker(rx: mpsc::Receiver<Command>) {
    // `execute_directly` requires the closure to be `Sync`; `Receiver` is not, so guard it.
    let rx = Mutex::new(rx);
    timely::execute_directly(move |worker| {
        let mut phase = Phase::Building {
            inputs: Vec::new(),
            views: Vec::new(),
        };
        loop {
            worker.step();
            let cmd = rx.lock().unwrap().recv_timeout(Duration::from_millis(1));
            match cmd {
                Ok(Command::RegisterInput { input, reply }) => {
                    let _ = reply.send(handle_register(&mut phase, input));
                }
                Ok(Command::Build { view, plan, reply }) => {
                    let _ = reply.send(handle_build(&mut phase, view, plan));
                }
                Ok(Command::Push {
                    input,
                    batch,
                    reply,
                }) => {
                    let _ = reply.send(handle_push(worker, &mut phase, input, batch));
                }
                Ok(Command::Snapshot { view, reply }) => {
                    let _ = reply.send(handle_snapshot(&phase, view));
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

/// Declare an input before the dataflow is built; reject duplicates and late calls.
fn handle_register(phase: &mut Phase, input: InputId) -> Result<(), CoreError> {
    match phase {
        Phase::Building { inputs, .. } => {
            if inputs.contains(&input) {
                return Err(CoreError::Unsupported(format!(
                    "input {input:?} already registered"
                )));
            }
            inputs.push(input);
            Ok(())
        }
        Phase::Running(_) => Err(CoreError::Unsupported("engine already running".into())),
    }
}

/// Record a view before the dataflow is built; reject duplicates and late calls.
/// Source references are checked at build time, so forward references are allowed.
fn handle_build(phase: &mut Phase, view: ViewId, plan: Plan) -> Result<(), CoreError> {
    match phase {
        Phase::Building { views, .. } => {
            if views.iter().any(|(built, _)| *built == view) {
                return Err(CoreError::Unsupported(format!(
                    "view {view:?} already built"
                )));
            }
            views.push((view, plan));
            Ok(())
        }
        Phase::Running(_) => Err(CoreError::Unsupported("engine already running".into())),
    }
}

/// Reject plans whose `Source` ids were never registered, before building.
fn validate_schema(inputs: &[InputId], views: &[(ViewId, Plan)]) -> Result<(), CoreError> {
    let registered: HashSet<InputId> = inputs.iter().copied().collect();
    for (_, plan) in views {
        circuit::validate(plan, None, &registered)?;
    }
    Ok(())
}

/// Build the dataflow on first push, then feed the batch to the requested input.
fn handle_push(
    worker: &mut Worker,
    phase: &mut Phase,
    input: InputId,
    batch: ChangeBatch,
) -> Result<(), CoreError> {
    if let Phase::Building { inputs, views } = phase {
        if !inputs.contains(&input) {
            return Err(CoreError::Unsupported(format!("unknown input {input:?}")));
        }
        validate_schema(inputs, views)?;
    }
    if matches!(phase, Phase::Building { .. }) {
        let running = match phase {
            Phase::Building { inputs, views } => build_dataflow(worker, inputs, views),
            Phase::Running(_) => unreachable!("matched Building above"),
        };
        *phase = Phase::Running(running);
    }
    match phase {
        Phase::Running(running) => run_push(worker, running, input, &batch),
        Phase::Building { .. } => unreachable!("build occurred above"),
    }
}

/// Build the single scope holding every declared input and view.
fn build_dataflow(worker: &mut Worker, inputs: &[InputId], views: &[(ViewId, Plan)]) -> Running {
    worker.dataflow::<u64, _, _>(|scope| {
        let mut sessions: HashMap<InputId, InputSession<u64, Row, isize>> = HashMap::new();
        let mut collections: HashMap<InputId, VecCollection<'_, u64, Row, isize>> = HashMap::new();
        for &id in inputs {
            let (session, coll) = scope.new_collection::<Row, isize>();
            sessions.insert(id, session);
            collections.insert(id, coll);
        }
        let mut view_states: HashMap<ViewId, ViewState> = HashMap::new();
        let mut consumers: HashMap<InputId, Vec<ViewId>> = HashMap::new();
        for (view, plan) in views {
            let state: State = Rc::new(RefCell::new(BTreeMap::new()));
            let sink = state.clone();
            let (probe, _out) = circuit::compile(&collections, plan)
                .inspect(move |update| {
                    let (row, _time, diff) = update;
                    *sink.borrow_mut().entry(row.clone()).or_insert(0) += *diff as i64;
                })
                .probe();
            for src in circuit::sources(plan) {
                consumers.entry(src).or_default().push(*view);
            }
            view_states.insert(
                *view,
                ViewState {
                    probe,
                    state,
                    plan: plan.clone(),
                },
            );
        }
        Running {
            inputs: sessions,
            views: view_states,
            consumers,
            registered: inputs.iter().copied().collect(),
        }
    })
}

/// Validate the batch against every consuming view, then feed and drain the input.
fn run_push(
    worker: &mut Worker,
    running: &mut Running,
    input: InputId,
    batch: &ChangeBatch,
) -> Result<(), CoreError> {
    if !running.inputs.contains_key(&input) {
        return Err(CoreError::Unsupported(format!("unknown input {input:?}")));
    }
    let consumers = running.consumers.get(&input).cloned().unwrap_or_default();
    for view in &consumers {
        circuit::validate_rows(&running.views[view].plan, batch, &running.registered)?;
    }
    let target = {
        let session = running.inputs.get_mut(&input).expect("checked above");
        circuit::feed(session, batch);
        *session.time()
    };
    circuit::drain(worker, &running.views, &consumers, target)
}

/// Read the non-zero consolidated Z-set of an existing view.
fn handle_snapshot(phase: &Phase, view: ViewId) -> Result<Vec<Row>, CoreError> {
    match phase {
        Phase::Running(running) => running.snapshot(view),
        Phase::Building { .. } => Err(CoreError::Unsupported(format!("unknown view {view:?}"))),
    }
}
