use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;
use crate::zset::int64_diffs;

/// Reduces an arrangement to per-key counts as a Z-set of `(key..., count)`.
///
/// A key's count is the sum of the diffs of every `(key, payload)` entry it
/// owns, so retractions lower it and a key that crosses to zero disappears.
/// Each surviving key emits one row with a `+1` diff; incremental callers diff
/// consecutive outputs to recover the retraction/insertion deltas.
pub fn group_count(arrangement: &KeyedArrangement) -> Result<ZSetBatch, EngineError> {
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
