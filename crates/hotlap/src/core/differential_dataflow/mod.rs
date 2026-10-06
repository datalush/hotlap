//! Stateful `differential-dataflow` implementation of [`IncrementalCore`].
//!
//! This is the only module that may name `differential-dataflow`/`timely` types. It
//! owns a live timely worker on its own thread and speaks to it through a command
//! channel. One dataflow holds every input and view in the same scope; it is built
//! once, on the first push, because DD cannot add operators to a live `dataflow`.

mod build;

mod circuit;

mod join;

mod push;

mod session;

#[cfg(test)]
mod tests;

mod validate;

mod worker;

use std::sync::mpsc;

use crate::core::{CoreError, IncrementalCore, InputId, ViewId, WatermarkSpec};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row};

/// Upper bound on `worker.step()` calls while draining one push before giving up.
/// The frontier must advance by at least one step per batch in normal operation; this
/// guard turns a stuck dataflow into an error instead of hanging the worker thread
/// (and, transitively, `push` and `Drop`).
const MAX_DRAIN_STEPS: usize = 1_000_000;

enum Command {
    RegisterInput {
        input: InputId,
        reply: mpsc::Sender<Result<(), CoreError>>,
    },
    Build {
        view: ViewId,
        plan: Plan,
        reply: mpsc::Sender<Result<(), CoreError>>,
    },
    DeclareWatermark {
        input: InputId,
        spec: WatermarkSpec,
        reply: mpsc::Sender<Result<(), CoreError>>,
    },
    Push {
        input: InputId,
        batch: ChangeBatch,
        reply: mpsc::Sender<Result<(), CoreError>>,
    },
    Snapshot {
        view: ViewId,
        reply: mpsc::Sender<Result<Vec<Row>, CoreError>>,
    },
    LateDropped {
        input: InputId,
        reply: mpsc::Sender<Result<u64, CoreError>>,
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

    fn request<T>(
        &self,
        make: impl FnOnce(mpsc::Sender<Result<T, CoreError>>) -> Command,
        dropped: &str,
    ) -> Result<T, CoreError> {
        let (reply, rx) = mpsc::channel();
        self.tx
            .send(make(reply))
            .map_err(|_| CoreError::Infrastructure("core worker is not running".into()))?;
        rx.recv()
            .map_err(|_| CoreError::Infrastructure(dropped.to_string()))?
    }
}

#[cfg(test)]
impl DifferentialCore {
    /// Test-only: stop the worker but keep `self.tx` alive, so the next command
    /// fails at the channel boundary and yields `CoreError::Infrastructure`.
    pub(crate) fn stop_worker_for_test(&mut self) {
        let (reply, rx) = std::sync::mpsc::channel();
        if self.tx.send(Command::Shutdown { reply }).is_ok() {
            let _ = rx.recv();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl IncrementalCore for DifferentialCore {
    fn register_input(&mut self, input: InputId) -> Result<(), CoreError> {
        self.request(
            |reply| Command::RegisterInput { input, reply },
            "core worker dropped register request",
        )
    }

    fn build_view(&mut self, view: ViewId, plan: &Plan) -> Result<(), CoreError> {
        self.request(
            |reply| Command::Build {
                view,
                plan: plan.clone(),
                reply,
            },
            "core worker dropped build request",
        )
    }

    fn declare_watermark(&mut self, input: InputId, spec: WatermarkSpec) -> Result<(), CoreError> {
        self.request(
            |reply| Command::DeclareWatermark { input, spec, reply },
            "core worker dropped declare_watermark request",
        )
    }

    fn push(&mut self, input: InputId, batch: &ChangeBatch) -> Result<(), CoreError> {
        self.request(
            |reply| Command::Push {
                input,
                batch: batch.clone(),
                reply,
            },
            "core worker dropped push request",
        )
    }

    fn snapshot(&mut self, view: ViewId) -> Result<Vec<Row>, CoreError> {
        self.request(
            |reply| Command::Snapshot { view, reply },
            "core worker dropped snapshot request",
        )
    }

    fn late_dropped(&self, input: InputId) -> Result<u64, CoreError> {
        self.request(
            |reply| Command::LateDropped { input, reply },
            "core worker dropped late_dropped request",
        )
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
