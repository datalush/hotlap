use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{concat, concat_batches, take};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;
use crate::zset::{consolidate, int64_diffs};

/// Stateful incremental inner equi-join of two keyed streams.
///
/// Both sides accumulate in [`KeyedArrangement`]s keyed by their join columns,
/// so retractions update the stored state. Each `apply` feeds one delta per side,
/// recomputes the join, and emits the changelog against the previous relation.
///
/// Output is `left || right` plus the signed `diff` (the product of both diffs).
/// Complexity: `apply` recomputes from both arrangements, `O(|L| * |R|)` per
/// delta. TODO: incremental per-key join (out of 7c scope).
pub struct Join {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left: Option<KeyedArrangement>,
    right: Option<KeyedArrangement>,
    previous: Option<ZSetBatch>,
}

impl Join {
    /// Creates a join over the join column indices `left_keys` and `right_keys`.
    /// Side schemas are learned from the first `apply`.
    pub fn new(left_keys: &[usize], right_keys: &[usize]) -> Self {
        Self {
            left_keys: left_keys.to_vec(),
            right_keys: right_keys.to_vec(),
            left: None,
            right: None,
            previous: None,
        }
    }

    /// Applies one delta per side and returns the join changelog since the last
    /// call. The first call emits the full join snapshot.
    pub fn apply(&mut self, left: &ZSetBatch, right: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
        if self.left_keys.len() != self.right_keys.len() {
            return Err(EngineError::Unsupported(
                "join sides must share the same key arity".to_string(),
            ));
        }
        self.accumulate(left, right)?;
        let current = snapshot(
            self.left.as_ref().expect("left arrangement initialized"),
            &self.left_keys,
            self.right.as_ref().expect("right arrangement initialized"),
            &self.right_keys,
        )?;
        let delta = match &self.previous {
            None => current.clone(),
            Some(previous) => subtract(&current, previous)?,
        };
        self.previous = Some(current);
        Ok(delta)
    }

    /// Adds each side's delta to its arrangement, creating the arrangements from
    /// the incoming schemas on the first call.
    fn accumulate(&mut self, left: &ZSetBatch, right: &ZSetBatch) -> Result<(), EngineError> {
        if self.left.is_none() {
            self.left = Some(KeyedArrangement::new(left.schema(), &self.left_keys)?);
        }
        if self.right.is_none() {
            self.right = Some(KeyedArrangement::new(right.schema(), &self.right_keys)?);
        }
        let left_keys = KeyConverter::new(left.schema().as_ref(), &self.left_keys)?;
        let right_keys = KeyConverter::new(right.schema().as_ref(), &self.right_keys)?;
        self.left
            .as_mut()
            .expect("left arrangement initialized")
            .apply(left, &left_keys)?;
        self.right
            .as_mut()
            .expect("right arrangement initialized")
            .apply(right, &right_keys)?;
        Ok(())
    }
}

/// Materializes both arrangements and joins their current relations.
fn snapshot(
    left: &KeyedArrangement,
    left_keys: &[usize],
    right: &KeyedArrangement,
    right_keys: &[usize],
) -> Result<ZSetBatch, EngineError> {
    let left = left.to_zset()?;
    let right = right.to_zset()?;
    equi_join(&left, left_keys, &right, right_keys)
}

/// Inner equi-joins two Z-sets on the given key columns.
///
/// Matching uses `arrow::row` bytes, so equal keys compare byte-wise. The result
/// is consolidated by full joined row, summing the sides' diff products.
fn equi_join(
    left: &ZSetBatch,
    left_keys: &[usize],
    right: &ZSetBatch,
    right_keys: &[usize],
) -> Result<ZSetBatch, EngineError> {
    let left_rows =
        KeyConverter::new(left.schema().as_ref(), left_keys)?.convert(left.batch.columns())?;
    let right_rows =
        KeyConverter::new(right.schema().as_ref(), right_keys)?.convert(right.batch.columns())?;

    let mut buckets: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for index in 0..left_rows.num_rows() {
        buckets
            .entry(left_rows.row(index).as_ref().to_vec())
            .or_default()
            .push(index);
    }
    let mut matches: Vec<(usize, usize)> = Vec::new();
    for index in 0..right_rows.num_rows() {
        if let Some(left_indices) = buckets.get(right_rows.row(index).as_ref()) {
            matches.extend(left_indices.iter().map(|&left_index| (left_index, index)));
        }
    }
    materialize(left, right, &matches)
}

/// Builds the joined Z-set for the `(left index, right index)` match pairs.
fn materialize(
    left: &ZSetBatch,
    right: &ZSetBatch,
    matches: &[(usize, usize)],
) -> Result<ZSetBatch, EngineError> {
    let left_indices = UInt32Array::from(
        matches
            .iter()
            .map(|&(left_index, _)| left_index as u32)
            .collect::<Vec<u32>>(),
    );
    let right_indices = UInt32Array::from(
        matches
            .iter()
            .map(|&(_, right_index)| right_index as u32)
            .collect::<Vec<u32>>(),
    );
    let left_taken = take_batch(&left.batch, &left_indices)?;
    let right_taken = take_batch(&right.batch, &right_indices)?;

    let mut fields: Vec<Field> = left
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.extend(
        right
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone()),
    );
    let mut columns: Vec<ArrayRef> = left_taken.columns().to_vec();
    columns.extend(right_taken.columns().iter().cloned());
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;

    let left_diffs = int64_diffs(&left.diff)?;
    let right_diffs = int64_diffs(&right.diff)?;
    let diffs: Vec<i64> = matches
        .iter()
        .map(|&(left_index, right_index)| {
            left_diffs.value(left_index) * right_diffs.value(right_index)
        })
        .collect();
    consolidate(&ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs)))?)
}

/// Applies `indices` to every column of `batch`.
fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch, EngineError> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        columns.push(take(column.as_ref(), indices, None)?);
    }
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Returns `current - previous` as a consolidated Z-set.
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
