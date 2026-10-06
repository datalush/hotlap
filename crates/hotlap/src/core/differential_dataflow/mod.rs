//! Stateful `differential-dataflow` implementation of [`IncrementalCore`].
//!
//! This is the only module that may name `differential-dataflow`/`timely` types. It
//! owns a live timely worker on its own thread (so DD operators retain their
//! arrangements between pushes) and speaks to it through a command channel. Logical
//! time is advanced monotonically by one tick per push; a probe on the output tells
//! us when the push has settled before we read the consolidated Z-set.

mod circuit;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use differential_dataflow::input::{Input, InputSession};
use timely::dataflow::operators::probe::Handle;

use crate::core::{CoreError, IncrementalCore, ViewId};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row};

/// Consolidated output Z-set of a view, accumulated from the output stream.
type State = Arc<Mutex<BTreeMap<Row, i64>>>;

/// Upper bound on `worker.step()` calls while draining one push before giving up.
/// The frontier must advance by at least one step per batch in normal operation; this
/// guard turns a stuck dataflow into an error instead of hanging the worker thread
/// (and, transitively, `push` and `Drop`).
const MAX_DRAIN_STEPS: usize = 1_000_000;

struct ViewState {
    input: InputSession<u64, Row, isize>,
    probe: Handle<u64>,
    state: State,
    next_time: u64,
    plan: Plan,
}

enum Command {
    Build {
        view: ViewId,
        plan: Plan,
        reply: mpsc::Sender<Result<(), CoreError>>,
    },
    Push {
        view: ViewId,
        batch: ChangeBatch,
        reply: mpsc::Sender<Result<(), CoreError>>,
    },
    Snapshot {
        view: ViewId,
        reply: mpsc::Sender<Result<Vec<Row>, CoreError>>,
    },
    Shutdown {
        reply: mpsc::Sender<()>,
    },
}

/// Differential-dataflow-backed [`IncrementalCore`] with a persistent worker session.
pub struct DifferentialCore {
    tx: mpsc::Sender<Command>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl DifferentialCore {
    pub fn new() -> Result<Self, CoreError> {
        let (tx, rx) = mpsc::channel::<Command>();
        let worker = std::thread::spawn(move || run_worker(rx));
        Ok(Self {
            tx,
            worker: Some(worker),
        })
    }

    fn send(&self, cmd: Command) -> Result<(), CoreError> {
        self.tx
            .send(cmd)
            .map_err(|_| CoreError::Unsupported("core worker is not running".into()))
    }
}

impl IncrementalCore for DifferentialCore {
    fn build_view(&mut self, view: ViewId, plan: &Plan) -> Result<(), CoreError> {
        // SP1a compiles only linear pipelines rooted at GroupCount; SP1b adds joins.
        if !matches!(plan, Plan::GroupCount { .. }) {
            return Err(CoreError::Unsupported(
                "SP1a supports only pipelines rooted at GroupCount".into(),
            ));
        }
        let (reply, rx) = mpsc::channel();
        self.send(Command::Build {
            view,
            plan: plan.clone(),
            reply,
        })?;
        rx.recv()
            .map_err(|_| CoreError::Unsupported("core worker dropped build request".into()))?
    }

    fn push(&mut self, view_input: ViewId, batch: &ChangeBatch) -> Result<(), CoreError> {
        let (reply, rx) = mpsc::channel();
        self.send(Command::Push {
            view: view_input,
            batch: batch.clone(),
            reply,
        })?;
        rx.recv()
            .map_err(|_| CoreError::Unsupported("core worker dropped push request".into()))?
    }

    fn snapshot(&mut self, view: ViewId) -> Result<Vec<Row>, CoreError> {
        let (reply, rx) = mpsc::channel();
        self.send(Command::Snapshot { view, reply })?;
        rx.recv()
            .map_err(|_| CoreError::Unsupported("core worker dropped snapshot request".into()))?
    }
}

impl Drop for DifferentialCore {
    fn drop(&mut self) {
        let (reply, rx) = mpsc::channel();
        if self.tx.send(Command::Shutdown { reply }).is_ok() {
            let _ = rx.recv();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Worker-thread body: own the timely worker, build views on demand and step
/// continuously so arrangements are retained across pushes.
fn run_worker(rx: mpsc::Receiver<Command>) {
    // `execute_directly` requires the closure to be `Sync`; `Receiver` is not, so guard it.
    let rx = Mutex::new(rx);
    timely::execute_directly(move |worker| {
        let mut views: HashMap<ViewId, ViewState> = HashMap::new();
        loop {
            worker.step();
            let cmd = rx.lock().unwrap().recv_timeout(Duration::from_millis(1));
            match cmd {
                Ok(Command::Build { view, plan, reply }) => {
                    let result = match views.entry(view) {
                        std::collections::hash_map::Entry::Occupied(_) => Err(
                            CoreError::Unsupported(format!("view {view:?} already built")),
                        ),
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            let (input, probe, state) = worker.dataflow::<u64, _, _>(|scope| {
                                let (input, coll) = scope.new_collection::<Row, isize>();
                                let state: State = Arc::new(Mutex::new(BTreeMap::new()));
                                let sink = state.clone();
                                let (probe, _out) = circuit::compile(coll, &plan)
                                    .inspect(move |update| {
                                        let (row, _time, diff) = update;
                                        let mut store = sink.lock().unwrap();
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
                    };
                    let _ = reply.send(result);
                }
                Ok(Command::Push { view, batch, reply }) => {
                    let result = match views.get_mut(&view) {
                        None => Err(CoreError::Unsupported(format!("unknown view {view:?}"))),
                        Some(vs) => circuit::push_batch(worker, vs, &batch),
                    };
                    let _ = reply.send(result);
                }
                Ok(Command::Snapshot { view, reply }) => {
                    let result = match views.get(&view) {
                        None => Err(CoreError::Unsupported(format!("unknown view {view:?}"))),
                        Some(vs) => {
                            let store = vs.state.lock().unwrap();
                            Ok(store
                                .iter()
                                .filter(|(_, diff)| **diff != 0)
                                .map(|(row, _)| row.clone())
                                .collect())
                        }
                    };
                    let _ = reply.send(result);
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
