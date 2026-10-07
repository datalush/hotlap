//! Plan evaluation: recompute a view's full relation from accumulated inputs.
//!
//! Each stateful operator is instantiated fresh per evaluation, so applying it
//! to the full input relation yields the full output relation (not a delta). The
//! caller diffs successive relations to obtain the changelog.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::compute::{cast, concat, concat_batches};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;
use crate::ops::{GroupCount, Join, TumbleCount, filter, project};
use crate::zset::{consolidate, int64_diffs};

use hotlap_core::plan::sources;
use hotlap_core::{InputId, Plan};

/// Evaluates `plan` against the accumulated inputs.
///
/// Returns `None` while at least one source the plan reads has received no data,
/// so its schema (and thus any derived output) is still unknown.
pub(crate) fn eval_plan(
    plan: &Plan,
    inputs: &HashMap<InputId, ZSetBatch>,
    watermarks: &HashMap<InputId, i64>,
) -> Result<Option<ZSetBatch>, EngineError> {
    match plan {
        Plan::Source(id) => Ok(inputs.get(id).cloned()),
        Plan::Filter { input, pred } => match eval_plan(input, inputs, watermarks)? {
            None => Ok(None),
            Some(z) => Ok(Some(filter(&z, &pred.eval(&z.batch)?)?)),
        },
        Plan::Project { input, cols } => match eval_plan(input, inputs, watermarks)? {
            None => Ok(None),
            Some(z) => Ok(Some(project(&z, cols)?)),
        },
        Plan::GroupCount { input, key } => match eval_plan(input, inputs, watermarks)? {
            None => Ok(None),
            Some(z) => Ok(Some(group_count(&z, key)?)),
        },
        Plan::Join {
            left,
            right,
            left_key,
            right_key,
        } => match (
            eval_plan(left, inputs, watermarks)?,
            eval_plan(right, inputs, watermarks)?,
        ) {
            (Some(l), Some(r)) => Ok(Some(Join::new(left_key, right_key).apply(&l, &r)?)),
            _ => Ok(None),
        },
        Plan::TumbleCount {
            input,
            key,
            time_col,
            size,
        } => match eval_plan(input, inputs, watermarks)? {
            None => Ok(None),
            Some(z) => Ok(Some(tumble_count(
                &z,
                key,
                *time_col,
                *size,
                plan_watermark(input, watermarks),
            )?)),
        },
    }
}

/// Reduces `z` to `(key..., count)` rows by keying an arrangement and counting.
fn group_count(z: &ZSetBatch, key: &[usize]) -> Result<ZSetBatch, EngineError> {
    let mut arrangement = KeyedArrangement::new(z.schema(), key)?;
    let keys = KeyConverter::new(z.schema().as_ref(), key)?;
    arrangement.apply(z, &keys)?;
    GroupCount::new().apply(&arrangement)
}

/// Buckets every row of `z` at watermark zero, then closes windows up to `wm`.
///
/// Bucketing at zero keeps rows that a smaller watermark would have dropped as
/// late, since lateness is already enforced when the input is pushed.
fn tumble_count(
    z: &ZSetBatch,
    key: &[usize],
    time_col: usize,
    size: i64,
    wm: i64,
) -> Result<ZSetBatch, EngineError> {
    let mut window = TumbleCount::new(key, time_col, size);
    window.apply(z, 0)?;
    let empty = ZSetBatch::empty(z.schema());
    window.apply(&empty, wm)
}

/// The watermark a window plan closes against: the minimum of its sources'.
pub(crate) fn plan_watermark(plan: &Plan, watermarks: &HashMap<InputId, i64>) -> i64 {
    sources(plan)
        .iter()
        .map(|id| watermarks.get(id).copied().unwrap_or(0))
        .min()
        .unwrap_or(0)
}

/// Concatenates two Z-sets over the same schema.
pub(crate) fn merge(a: &ZSetBatch, b: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
    if a.schema() != b.schema() {
        return Err(EngineError::Unsupported(
            "input schema changed between pushes".to_string(),
        ));
    }
    let batch = concat_batches(&a.schema(), [&a.batch, &b.batch])?;
    let diff = concat(&[a.diff.as_ref(), b.diff.as_ref()])?;
    Ok(ZSetBatch::new(batch, diff)?)
}

/// Returns the changelog between a drained snapshot and the current one.
pub(crate) fn delta(
    current: Option<&ZSetBatch>,
    drained: Option<&ZSetBatch>,
) -> Result<Option<ZSetBatch>, EngineError> {
    match (current, drained) {
        (None, _) => Ok(None),
        (Some(current), None) => Ok(Some(current.clone())),
        (Some(current), Some(drained)) => Ok(Some(subtract(current, drained)?)),
    }
}

/// Returns `current - previous` as a consolidated Z-set.
fn subtract(current: &ZSetBatch, previous: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
    let batch = concat_batches(&current.schema(), [&current.batch, &previous.batch])?;
    let retracted = negate(previous.diff())?;
    let diff = concat(&[current.diff.as_ref(), retracted.as_ref()])?;
    consolidate(&ZSetBatch::new(batch, diff)?)
}

/// Negates an integer diff column, preserving its length.
fn negate(diff: &ArrayRef) -> Result<ArrayRef, EngineError> {
    let ints = int64_diffs(diff)?;
    let negated: Int64Array = ints.iter().map(|value| value.map(|v| -v)).collect();
    Ok(Arc::new(negated))
}

/// Reads an event-time column as non-negative `i64`; nulls map to zero.
pub(crate) fn time_values(batch: &RecordBatch, col: usize) -> Result<Int64Array, EngineError> {
    let column = batch.columns().get(col).ok_or_else(|| {
        EngineError::Unsupported(format!("time column {col} out of range"))
    })?;
    let casted = cast(column.as_ref(), &DataType::Int64)?;
    let ints = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| EngineError::Infrastructure("int64 cast produced wrong type".to_string()))?;
    Ok(Int64Array::from(
        ints.iter()
            .map(|value| value.unwrap_or(0).max(0))
            .collect::<Vec<_>>(),
    ))
}
