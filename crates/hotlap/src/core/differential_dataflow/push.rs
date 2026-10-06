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
    let consumers = running.consumers.get(&input).cloned().unwrap_or_default();
    // Validate against the RAW batch first: arity is invariant to dropping late
    // rows, and a rejected push must not advance the clock or bump the late metric.
    running.learn_arity(input, batch)?;
    for view in &consumers {
        validate::validate(
            &running.views[view].plan,
            &running.arities,
            &running.registered,
        )?;
    }
    let (kept, wm) = running.filter_late(input, batch);
    let epoch = if running.event_time {
        circuit::feed_event_time(
            &mut running.inputs,
            &mut running.frontier_now,
            input,
            &kept,
            wm,
        )
    } else {
        circuit::feed(&mut running.inputs, input, &kept)
    };
    let targets = targets_for(running, &consumers, epoch);
    circuit::drain_targets(worker, &running.views, &consumers, &targets)
}

/// Drain target per consumer: in epoch mode, `epoch`; in event-time mode, the
/// minimum of the DD frontiers of the view plan's sources.
fn targets_for(running: &Running, consumers: &[ViewId], epoch: u64) -> HashMap<ViewId, u64> {
    consumers
        .iter()
        .map(|view| {
            let target = if running.event_time {
                circuit::sources(&running.views[view].plan)
                    .iter()
                    .map(|src| *running.frontier_now.get(src).unwrap_or(&0))
                    .min()
                    .unwrap_or(0)
            } else {
                epoch
            };
            (*view, target)
        })
        .collect()
}
