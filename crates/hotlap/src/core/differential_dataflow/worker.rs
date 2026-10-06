//! Worker-thread loop and command handlers for the differential core.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use timely::worker::Worker;

use super::session::Phase;
use super::{Command, build, push, validate};
use crate::core::{CoreError, InputId, ViewId, WatermarkSpec};
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
            watermarks: HashMap::new(),
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
                Ok(Command::DeclareWatermark { input, spec, reply }) => {
                    let _ = reply.send(handle_declare(&mut phase, input, spec));
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
                Ok(Command::LateDropped { input, reply }) => {
                    let v = match &phase {
                        Phase::Running(r) => Ok(*r.late.get(&input).unwrap_or(&0)),
                        Phase::Building { .. } => Ok(0),
                    };
                    let _ = reply.send(v);
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
/// Inputs must already be registered: `Plan::Source` is checked here so an unknown
/// source fails fast at declaration rather than at the first push. Column/arity
/// bounds are deferred to the push, since the schema is unknown until data arrives.
fn handle_build(phase: &mut Phase, view: ViewId, plan: Plan) -> Result<(), CoreError> {
    match phase {
        Phase::Building { inputs, views, .. } => {
            if views.iter().any(|(built, _)| *built == view) {
                return Err(CoreError::Unsupported(format!(
                    "view {view:?} already built"
                )));
            }
            let registered: HashSet<InputId> = inputs.iter().copied().collect();
            validate::validate(&plan, &HashMap::new(), &registered)?;
            views.push((view, plan));
            Ok(())
        }
        Phase::Running(_) => Err(CoreError::Unsupported("engine already running".into())),
    }
}

/// Declara el watermark de una fuente antes del build. Rechaza input desconocido,
/// duplicado y llamadas tras el primer push.
fn handle_declare(phase: &mut Phase, input: InputId, spec: WatermarkSpec) -> Result<(), CoreError> {
    match phase {
        Phase::Building {
            inputs, watermarks, ..
        } => {
            if !inputs.contains(&input) {
                return Err(CoreError::Unsupported(format!("unknown input {input:?}")));
            }
            if spec.lag < 0 {
                return Err(CoreError::Unsupported("watermark lag must be >= 0".into()));
            }
            if watermarks.contains_key(&input) {
                return Err(CoreError::Unsupported(format!(
                    "watermark already declared for {input:?}"
                )));
            }
            watermarks.insert(input, spec);
            Ok(())
        }
        Phase::Running(_) => Err(CoreError::Unsupported("engine already running".into())),
    }
}

/// Build the dataflow on first push, then feed the batch to the requested input.
fn handle_push(
    worker: &mut Worker,
    phase: &mut Phase,
    input: InputId,
    batch: ChangeBatch,
) -> Result<(), CoreError> {
    if let Phase::Building { inputs, .. } = phase
        && !inputs.contains(&input)
    {
        return Err(CoreError::Unsupported(format!("unknown input {input:?}")));
    }
    if matches!(phase, Phase::Building { .. }) {
        let running = match phase {
            Phase::Building {
                inputs,
                views,
                watermarks,
            } => build::build_dataflow(worker, inputs, views, watermarks)?,
            Phase::Running(_) => unreachable!("matched Building above"),
        };
        *phase = Phase::Running(Box::new(running));
    }
    match phase {
        Phase::Running(running) => push::run_push(worker, running, input, &batch),
        Phase::Building { .. } => unreachable!("build occurred above"),
    }
}

/// Read the non-zero consolidated Z-set of an existing view.
fn handle_snapshot(phase: &Phase, view: ViewId) -> Result<Vec<Row>, CoreError> {
    match phase {
        Phase::Running(running) => running.snapshot(view),
        Phase::Building { views, .. } => {
            if views.iter().any(|(declared, _)| *declared == view) {
                return Err(CoreError::Unsupported("view not built yet".into()));
            }
            Err(CoreError::Unsupported(format!("unknown view {view:?}")))
        }
    }
}
