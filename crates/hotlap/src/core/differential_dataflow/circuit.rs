//! Plan validation, compilation, and batch feeding for the differential core.

use std::collections::HashMap;

use differential_dataflow::VecCollection;
use differential_dataflow::input::InputSession;
use timely::worker::Worker;

use super::MAX_DRAIN_STEPS;
use super::join;
use super::session::{TIME_SCALE, ViewState};
use crate::core::{CoreError, InputId, ViewId};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row, Scalar};

/// Collect the input ids a plan reads from, in traversal order.
pub(super) fn sources(plan: &Plan) -> Vec<InputId> {
    let mut out = Vec::new();
    collect_sources(plan, &mut out);
    out
}

fn collect_sources(plan: &Plan, out: &mut Vec<InputId>) {
    match plan {
        Plan::Source(id) => out.push(*id),
        Plan::Filter { input, .. } | Plan::Project { input, .. } => collect_sources(input, out),
        Plan::GroupCount { input, .. } => collect_sources(input, out),
        Plan::Join { left, right, .. } => {
            collect_sources(left, out);
            collect_sources(right, out);
        }
    }
}

/// Apply `batch` to `input`, then advance *every* input one logical step and
/// return the new step. All inputs share one logical clock, so a binary operator's
/// output frontier (the minimum of its inputs' frontiers) can always reach the
/// step we drain to, even when only one side received updates. Without this, a
/// join's frontier would stay pinned to the un-fed side and drain would time out.
///
/// The shared clock is a deliberate simplification: it makes a `push` a global
/// barrier over every input. Per-input event-time/window semantics and the GC that
/// depends on them are deferred to the windowing phase and will revisit this.
pub(super) fn feed(
    sessions: &mut HashMap<InputId, InputSession<u64, Row, isize>>,
    input: InputId,
    batch: &ChangeBatch,
) -> u64 {
    let time = {
        let session = sessions.get_mut(&input).expect("caller checked the input");
        for (row, diff) in &batch.rows {
            session.update(row.clone(), *diff as isize);
        }
        *session.time()
    };
    let next = time + 1;
    for session in sessions.values_mut() {
        session.advance_to(next);
        session.flush();
    }
    next
}

/// Inserta el lote en el tiempo actual de `input` y avanza su **frontera DD** a
/// `target = max(logical_next * TIME_SCALE, frontier_actual + 1)`, devolviéndolo.
/// Otros inputs conservan su frontera; los operadores binarios propagan el mínimo.
/// El `+1` garantiza visibilidad en el mismo push sin tocar el watermark lógico.
pub(super) fn feed_event_time(
    sessions: &mut HashMap<InputId, InputSession<u64, Row, isize>>,
    frontier_now: &mut HashMap<InputId, u64>,
    input: InputId,
    batch: &ChangeBatch,
    logical_next: u64,
) -> u64 {
    let current_frontier = *frontier_now.get(&input).unwrap_or(&0);
    let target = (logical_next * TIME_SCALE).max(current_frontier + 1);
    let session = sessions.get_mut(&input).expect("caller checked the input");
    for (row, diff) in &batch.rows {
        session.update(row.clone(), *diff as isize);
    }
    session.advance_to(target);
    session.flush();
    frontier_now.insert(input, target);
    target
}

/// Fail if draining has consumed the step budget without the output frontier
/// advancing, turning a stuck dataflow into an error instead of a hang.
fn ensure_drain_budget(steps: usize) -> Result<(), CoreError> {
    if steps >= MAX_DRAIN_STEPS {
        return Err(CoreError::Infrastructure(format!(
            "output frontier did not advance after {MAX_DRAIN_STEPS} steps"
        )));
    }
    Ok(())
}

/// Step until every consuming view's probe is at or past `target`, ensuring all
/// output updates at that time are observed before a snapshot reads the Z-set.
pub(super) fn drain_targets(
    worker: &mut Worker,
    views: &HashMap<ViewId, ViewState>,
    consumers: &[ViewId],
    targets: &HashMap<ViewId, u64>,
) -> Result<(), CoreError> {
    let mut steps = 0;
    while consumers
        .iter()
        .any(|v| targets.get(v).is_some_and(|t| views[v].probe.less_than(t)))
    {
        ensure_drain_budget(steps)?;
        worker.step();
        steps += 1;
    }
    Ok(())
}

/// Compile a plan over the shared collections. Infallible: bounds are checked by
/// [`validate`] when batches are pushed, and unsupported plans are rejected when
/// a view is built.
pub(super) fn compile<'scope>(
    collections: &HashMap<InputId, VecCollection<'scope, u64, Row, isize>>,
    plan: &Plan,
) -> VecCollection<'scope, u64, Row, isize> {
    match plan {
        Plan::Source(id) => collections[id].clone(),
        Plan::Filter { input, pred } => {
            let pred = pred.clone();
            compile(collections, input).filter(move |row| pred.eval(row))
        }
        Plan::Project { input, cols } => {
            let cols = cols.clone();
            compile(collections, input)
                .map(move |row| Row(cols.iter().map(|&col| row.col(col)).collect()))
        }
        Plan::GroupCount { input, key } => {
            let key = key.clone();
            compile(collections, input)
                .map(move |row| Row(key.iter().map(|&col| row.col(col)).collect()))
                .count()
                .map(|(k, count)| {
                    let mut out = k.0;
                    out.push(Scalar::I64(count as i64));
                    Row(out)
                })
        }
        Plan::Join {
            left,
            right,
            left_key,
            right_key,
        } => join::equijoin(
            compile(collections, left),
            compile(collections, right),
            left_key,
            right_key,
        ),
    }
}
