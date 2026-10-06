//! Stateful `differential-dataflow` implementation of [`IncrementalCore`].
//!
//! This is the only module that may name `differential-dataflow`/`timely` types. It
//! owns a live timely worker on its own thread (so DD operators retain their
//! arrangements between pushes) and speaks to it through a command channel. Logical
//! time is advanced monotonically by one tick per push; a probe on the output tells
//! us when the push has settled before we read the consolidated Z-set.

use std::collections::{BTreeMap, HashMap};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::VecCollection;
use timely::dataflow::operators::probe::Handle;

use crate::core::{CoreError, IncrementalCore, ViewId};
use crate::plan::{Plan, Predicate};
use crate::row::{ChangeBatch, Row, Scalar};

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
pub struct DdCore {
    tx: mpsc::Sender<Command>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl DdCore {
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

impl IncrementalCore for DdCore {
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

impl Drop for DdCore {
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
                        std::collections::hash_map::Entry::Occupied(_) => {
                            Err(CoreError::Unsupported(format!("view {view:?} already built")))
                        }
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            let (input, probe, state) = worker.dataflow::<u64, _, _>(|scope| {
                                let (input, coll) = scope.new_collection::<Row, isize>();
                                let state: State = Arc::new(Mutex::new(BTreeMap::new()));
                                let sink = state.clone();
                                let (probe, _out) = compile(coll, &plan)
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
                        Some(vs) => push_batch(worker, vs, &batch),
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

/// Validate that `col` is in range for a child row of length `arity`.
fn check_col(col: usize, arity: Option<usize>, what: &str) -> Result<(), CoreError> {
    if let Some(n) = arity
        && col >= n
    {
        return Err(CoreError::Unsupported(format!(
            "{what} column {col} out of range (arity {n})"
        )));
    }
    Ok(())
}

/// Validate that every index referenced by `plan` is in range for a child row of
/// length `arity`. `arity` is `None` only while scanning a plan detached from data.
fn validate(plan: &Plan, arity: Option<usize>) -> Result<Option<usize>, CoreError> {
    match plan {
        Plan::Scan => Ok(arity),
        Plan::Filter { input, pred } => {
            let arity = validate(input, arity)?;
            let col = match pred {
                Predicate::Eq(col, _) | Predicate::Gt(col, _) => *col,
            };
            check_col(col, arity, "filter")?;
            Ok(arity)
        }
        Plan::Project { input, cols } => {
            let arity = validate(input, arity)?;
            for &col in cols {
                check_col(col, arity, "project")?;
            }
            Ok(Some(cols.len()))
        }
        Plan::GroupCount { input, key } => {
            let arity = validate(input, arity)?;
            for &col in key {
                check_col(col, arity, "group key")?;
            }
            Ok(Some(key.len() + 1))
        }
    }
}

/// Advance logical time monotonically and feed the batch into the view's input.
fn push_batch(
    worker: &mut timely::worker::Worker,
    vs: &mut ViewState,
    batch: &ChangeBatch,
) -> Result<(), CoreError> {
    // Validate bounds per row (schemas are row-shaped, not declared in SP1a).
    for (row, _) in &batch.rows {
        validate(&vs.plan, Some(row.0.len()))?;
    }

    let time = vs.next_time;
    vs.next_time += 1;

    vs.input.advance_to(time);
    for (row, diff) in &batch.rows {
        vs.input.update(row.clone(), *diff as isize);
    }
    // Advance past the batch so DD consolidates and the output frontier moves.
    vs.input.advance_to(time + 1);
    vs.input.flush();

    // Drain until the output probe is past the batch time, ensuring every output
    // update at `time` has been observed before a snapshot can read the Z-set.
    let mut steps = 0;
    while vs.probe.less_than(vs.input.time()) {
        if steps >= MAX_DRAIN_STEPS {
            return Err(CoreError::Unsupported(format!(
                "output frontier did not advance after {MAX_DRAIN_STEPS} steps"
            )));
        }
        worker.step();
        steps += 1;
    }
    Ok(())
}

/// Compile a linear plan over a collection of rows. Infallible: bounds are checked
/// by [`validate`] when batches are pushed, and unsupported roots are rejected by
/// `build_view`.
fn compile<'scope>(
    coll: VecCollection<'scope, u64, Row, isize>,
    plan: &Plan,
) -> VecCollection<'scope, u64, Row, isize> {
    match plan {
        Plan::Scan => coll,
        Plan::Filter { input, pred } => {
            let pred = pred.clone();
            compile(coll, input).filter(move |row| pred.eval(row))
        }
        Plan::Project { input, cols } => {
            let cols = cols.clone();
            compile(coll, input).map(move |row| {
                Row(cols.iter().map(|&col| row.0[col].clone()).collect())
            })
        }
        Plan::GroupCount { input, key } => {
            let key = key.clone();
            compile(coll, input)
                .map(move |row| Row(key.iter().map(|&col| row.0[col].clone()).collect()))
                .count()
                .map(|(k, count)| {
                    let mut out = k.0;
                    out.push(Scalar::I64(count as i64));
                    Row(out)
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::fixture;
    use crate::plan::Predicate;
    use crate::row::{ChangeBatch, Row, Scalar};

    #[test]
    fn filtered_group_count_matches_recompute() {
        let mut core = DdCore::new().unwrap();
        let plan = Plan::GroupCount {
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::Scan),
                pred: Predicate::Gt(0, 1),
            }),
            key: vec![0],
        };
        core.build_view(ViewId(0), &plan).unwrap();
        let mut b = ChangeBatch::default();
        for f in fixture() {
            b.push(Row(vec![Scalar::I64(f.key), Scalar::I64(f.value)]), f.diff);
        }
        core.push(ViewId(0), &b).unwrap();
        let got: Vec<(i64, i64)> = core
            .snapshot(ViewId(0))
            .unwrap()
            .into_iter()
            .map(|r| match (&r.0[0], &r.0[1]) {
                (Scalar::I64(k), Scalar::I64(c)) => (*k, *c),
                _ => panic!("shape"),
            })
            .collect();
        // Filter key>1: rows for key 2 (+1,-1 => 0) and key 3 (+1) => [(3,1)]
        assert_eq!(got, vec![(3, 1)]);
    }

    #[test]
    fn stateful_retraction_without_rebuild() {
        let mut core = DdCore::new().unwrap();
        let plan = Plan::GroupCount {
            input: Box::new(Plan::Scan),
            key: vec![0],
        };
        core.build_view(ViewId(0), &plan).unwrap();

        let mut first = ChangeBatch::default();
        first.push(Row(vec![Scalar::I64(1), Scalar::I64(10)]), 1);
        first.push(Row(vec![Scalar::I64(2), Scalar::I64(20)]), 1);
        core.push(ViewId(0), &first).unwrap();
        let after_first: Vec<(i64, i64)> = snapshot_pairs(&mut core);
        assert_eq!(after_first, vec![(1, 1), (2, 1)]);

        // A later push retracts key 2; the live session updates without rebuild/replay.
        let mut second = ChangeBatch::default();
        second.push(Row(vec![Scalar::I64(2), Scalar::I64(20)]), -1);
        core.push(ViewId(0), &second).unwrap();
        assert_eq!(snapshot_pairs(&mut core), vec![(1, 1)]);
    }

    /// Snapshot as `(key, count)` pairs, asserting the `[key, count]` row shape.
    fn snapshot_pairs(core: &mut DdCore) -> Vec<(i64, i64)> {
        core.snapshot(ViewId(0))
            .unwrap()
            .into_iter()
            .map(|r| match (&r.0[0], &r.0[1]) {
                (Scalar::I64(k), Scalar::I64(c)) => (*k, *c),
                _ => panic!("shape"),
            })
            .collect()
    }

    #[test]
    fn validate_rejects_out_of_range_columns() {
        let project = Plan::Project {
            input: Box::new(Plan::Scan),
            cols: vec![5],
        };
        assert!(matches!(
            validate(&project, Some(2)),
            Err(CoreError::Unsupported(_))
        ));

        let group = Plan::GroupCount {
            input: Box::new(Plan::Scan),
            key: vec![9],
        };
        assert!(matches!(
            validate(&group, Some(2)),
            Err(CoreError::Unsupported(_))
        ));

        // In-range chains are accepted; `Project` narrows the arity seen downstream.
        let ok = Plan::GroupCount {
            input: Box::new(Plan::Project {
                input: Box::new(Plan::Scan),
                cols: vec![1, 0],
            }),
            key: vec![0, 1],
        };
        assert!(validate(&ok, Some(2)).is_ok());
    }

    #[test]
    fn unknown_view_errors_on_push_and_snapshot() {
        let mut core = DdCore::new().unwrap();
        let batch = ChangeBatch::default();
        assert!(matches!(
            core.push(ViewId(7), &batch),
            Err(CoreError::Unsupported(_))
        ));
        assert!(matches!(
            core.snapshot(ViewId(7)),
            Err(CoreError::Unsupported(_))
        ));
    }

    #[test]
    fn project_then_group_count_snapshot() {
        let mut core = DdCore::new().unwrap();
        // Group by the projected value column: `[k, v] -> [v] -> [v, count]`.
        let plan = Plan::GroupCount {
            input: Box::new(Plan::Project {
                input: Box::new(Plan::Scan),
                cols: vec![1],
            }),
            key: vec![0],
        };
        core.build_view(ViewId(0), &plan).unwrap();

        let mut batch = ChangeBatch::default();
        batch.push(Row(vec![Scalar::I64(1), Scalar::I64(10)]), 1);
        batch.push(Row(vec![Scalar::I64(2), Scalar::I64(10)]), 1);
        batch.push(Row(vec![Scalar::I64(3), Scalar::I64(20)]), 1);
        core.push(ViewId(0), &batch).unwrap();

        assert_eq!(snapshot_pairs(&mut core), vec![(10, 2), (20, 1)]);
    }
}
