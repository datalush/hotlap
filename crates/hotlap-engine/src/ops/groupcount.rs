use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{concat, concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;
use crate::zset::{consolidate, int64_diffs};

/// Stateful incremental `groupcount` over a keyed arrangement.
///
/// The reducer sums the diffs of every `(key, payload)` entry per key and turns
/// that relation into a per-epoch changelog. Each `apply` emits one row per key
/// whose count changed: `(key..., count)` with a signed diff. A key that leaves
/// the arrangement (crosses to zero) emits its old count with a `-1` diff only;
/// a new key emits its count with a `+1`; a changed key emits the old count with
/// `-1` and the new count with `+1`.
#[derive(Default)]
pub struct GroupCount {
    previous: Option<ZSetBatch>,
}

impl GroupCount {
    /// Creates a reducer with no prior state; the first `apply` emits the full
    /// snapshot of the current relation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reduces `arrangement` and returns the changelog since the previous call.
    pub fn apply(&mut self, arrangement: &KeyedArrangement) -> Result<ZSetBatch, EngineError> {
        let current = snapshot(arrangement)?;
        let delta = match &self.previous {
            None => current.clone(),
            Some(previous) => subtract(&current, previous)?,
        };
        self.previous = Some(current);
        Ok(delta)
    }
}

/// Reduces the arrangement's current state to one `(key..., count)` row per key.
fn snapshot(arrangement: &KeyedArrangement) -> Result<ZSetBatch, EngineError> {
    let materialized = arrangement.to_zset()?;
    let key_indices = arrangement.key_indices();
    let converter = KeyConverter::new(materialized.schema().as_ref(), key_indices)?;
    let key_rows = converter.convert(materialized.batch.columns())?;
    let diffs = int64_diffs(&materialized.diff)?;

    // `to_zset` orders rows by key bytes then payload bytes, so equal keys are
    // adjacent and one scan suffices to group them.
    let mut groups: Vec<(usize, i64)> = Vec::new();
    for index in 0..key_rows.num_rows() {
        let diff = diffs.value(index);
        match groups.last_mut() {
            Some(group) if key_rows.row(group.0) == key_rows.row(index) => group.1 += diff,
            _ => groups.push((index, diff)),
        }
    }
    groups.retain(|(_, sum)| *sum != 0);
    build_counts(&materialized, key_indices, &groups)
}

/// Returns `current - previous` as a consolidated Z-set of `(key, count)` rows.
fn subtract(current: &ZSetBatch, previous: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
    let batch = concat_batches(&current.schema(), [&current.batch, &previous.batch])?;
    let retracted = negate(previous.diff())?;
    let diff = concat(&[current.diff().as_ref(), retracted.as_ref()])?;
    consolidate(&ZSetBatch::new(batch, diff)?)
}

/// Negates an integer diff column, preserving its length.
fn negate(diff: &ArrayRef) -> Result<ArrayRef, EngineError> {
    let ints = int64_diffs(diff)?;
    let negated: Int64Array = ints.iter().map(|value| value.map(|v| -v)).collect();
    Ok(Arc::new(negated))
}

/// Builds `(key columns, count)` rows plus a `+1` diff for every kept group.
fn build_counts(
    source: &ZSetBatch,
    key_indices: &[usize],
    groups: &[(usize, i64)],
) -> Result<ZSetBatch, EngineError> {
    let positions: UInt32Array = UInt32Array::from(
        groups
            .iter()
            .map(|(index, _)| *index as u32)
            .collect::<Vec<u32>>(),
    );

    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for &index in key_indices {
        fields.push(source.schema().field(index).clone());
        columns.push(take(source.batch.column(index).as_ref(), &positions, None)?);
    }
    fields.push(Field::new("count", DataType::Int64, false));
    let counts: Vec<i64> = groups.iter().map(|(_, sum)| *sum).collect();
    columns.push(Arc::new(Int64Array::from(counts)));

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    let diff: ArrayRef = Arc::new(Int64Array::from(vec![1i64; groups.len()]));
    ZSetBatch::new(batch, diff)
}
