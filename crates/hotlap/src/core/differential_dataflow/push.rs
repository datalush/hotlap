//! Push execution: event-time filtering, feeding and per-view draining.

use std::collections::HashMap;

use timely::worker::Worker;

use super::circuit;
use super::session::Running;
use super::validate;
use crate::core::{CoreError, InputId, ViewId};
use crate::row::ChangeBatch;

/// Validate the batch against every consuming view, then feed and drain the input.
pub(super) fn run_push(
    worker: &mut Worker,
    running: &mut Running,
    input: InputId,
    batch: &ChangeBatch,
) -> Result<(), CoreError> {
    if !running.inputs.contains_key(&input) {
        return Err(CoreError::Unsupported(format!("unknown input {input:?}")));
    }
    let (kept, wm) = running.filter_late(input, batch);
    let consumers = running.consumers.get(&input).cloned().unwrap_or_default();
    running.learn_arity(input, &kept)?;
    for view in &consumers {
        validate::validate(
            &running.views[view].plan,
            &running.arities,
            &running.registered,
        )?;
    }
    let epoch = if running.event_time {
        circuit::feed_event_time(&mut running.inputs, input, &kept, wm);
        wm
    } else {
        circuit::feed(&mut running.inputs, input, &kept)
    };
    let targets = targets_for(running, &consumers, epoch);
    circuit::drain_targets(worker, &running.views, &consumers, &targets)
}

/// Objetivo de drenaje por consumidor: en epoch, `epoch`; en event-time, el mínimo
/// de los watermarks de las fuentes del plan de la vista.
fn targets_for(running: &Running, consumers: &[ViewId], epoch: u64) -> HashMap<ViewId, u64> {
    consumers
        .iter()
        .map(|view| {
            let target = if running.event_time {
                circuit::sources(&running.views[view].plan)
                    .iter()
                    .map(|src| *running.watermarks_now.get(src).unwrap_or(&0))
                    .min()
                    .unwrap_or(0)
            } else {
                epoch
            };
            (*view, target)
        })
        .collect()
}
