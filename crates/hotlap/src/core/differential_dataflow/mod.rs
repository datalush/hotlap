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

mod worker;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::mpsc;

use differential_dataflow::input::InputSession;
use timely::dataflow::operators::probe::Handle;

use crate::core::{CoreError, IncrementalCore, ViewId};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row};

/// Consolidated output Z-set of a view, accumulated from the output stream.
/// Single-threaded: only the worker thread touches it, so `Rc`/`RefCell` suffice.
type State = Rc<RefCell<BTreeMap<Row, i64>>>;

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
        let worker = std::thread::spawn(move || worker::run_worker(rx));
        Ok(Self {
            tx,
            worker: Some(worker),
        })
    }

    fn send(&self, cmd: Command) -> Result<(), CoreError> {
        self.tx
            .send(cmd)
            .map_err(|_| CoreError::Infrastructure("core worker is not running".into()))
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
            .map_err(|_| CoreError::Infrastructure("core worker dropped build request".into()))?
    }

    fn push(&mut self, view_input: ViewId, batch: &ChangeBatch) -> Result<(), CoreError> {
        let (reply, rx) = mpsc::channel();
        self.send(Command::Push {
            view: view_input,
            batch: batch.clone(),
            reply,
        })?;
        rx.recv()
            .map_err(|_| CoreError::Infrastructure("core worker dropped push request".into()))?
    }

    fn snapshot(&mut self, view: ViewId) -> Result<Vec<Row>, CoreError> {
        let (reply, rx) = mpsc::channel();
        self.send(Command::Snapshot { view, reply })?;
        rx.recv()
            .map_err(|_| CoreError::Infrastructure("core worker dropped snapshot request".into()))?
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
