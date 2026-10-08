use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{cast, sort_to_indices, take};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows};

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::full_converter;

/// Sorts the rows of `zset` by every column, treating the whole row as identity.
pub fn sort_rows(zset: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
    let order = order_rows(&zset.batch)?;
    take_zset(zset, &order)
}

/// Sums the diffs of identical full rows, dropping rows whose sum is zero.
///
/// A Z-set's identity is the whole row, so all columns participate in grouping.
/// The output is sorted by the encoded rows, which makes it deterministic.
pub fn consolidate(zset: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
    let converter = full_converter(zset.schema().as_ref())?;
    consolidate_with(&converter, zset)
}

/// Consolidates `zset` using a caller-provided converter for its full schema.
///
/// Operators whose schema is frozen cache the converter once and call this per
/// push instead of rebuilding it in [`consolidate`] every time.
pub(crate) fn consolidate_with(
    converter: &RowConverter,
    zset: &ZSetBatch,
) -> Result<ZSetBatch, EngineError> {
    let rows = encode_with(converter, &zset.batch)?;
    let order = order_rows_from(&rows)?;
    let diffs = int64_diffs(&zset.diff)?;

    let mut positions = Vec::new();
    let mut sums = Vec::new();
    for (position, sum) in grouped_sums(&rows, &order, &diffs)? {
        if sum != 0 {
            positions.push(position as u32);
            sums.push(sum);
        }
    }

    let indices = UInt32Array::from(positions);
    let batch = take_batch(&zset.batch, &indices)?;
    let diff: ArrayRef = Arc::new(Int64Array::from(sums));
    Ok(ZSetBatch::new(batch, diff)?)
}

/// Encodes every column of `batch` into byte-comparable rows.
pub(crate) fn full_rows(batch: &RecordBatch) -> Result<Rows, EngineError> {
    let converter = full_converter(batch.schema().as_ref())?;
    encode_with(&converter, batch)
}

/// Encodes every column of `batch` using an existing full-row converter.
pub(crate) fn encode_with(
    converter: &RowConverter,
    batch: &RecordBatch,
) -> Result<Rows, EngineError> {
    crate::work::record(batch.num_rows());
    Ok(converter.convert_columns(batch.columns())?)
}

/// Returns the order that sorts the full encoded rows ascending.
fn order_rows(batch: &RecordBatch) -> Result<UInt32Array, EngineError> {
    let rows = full_rows(batch)?;
    order_rows_from(&rows)
}

/// Sorts the byte-comparable rows produced by `arrow::row`.
fn order_rows_from(rows: &Rows) -> Result<UInt32Array, EngineError> {
    let binary = rows.clone().try_into_binary()?;
    Ok(sort_to_indices(&binary, None, None)?)
}

/// Coerces the diff column to signed 64-bit integers.
pub(crate) fn int64_diffs(diff: &ArrayRef) -> Result<Int64Array, EngineError> {
    if !diff.data_type().is_integer() {
        return Err(EngineError::Unsupported(format!(
            "diff column must be integer, found {:?}",
            diff.data_type()
        )));
    }
    let casted = cast(diff.as_ref(), &DataType::Int64)?;
    casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| EngineError::Infrastructure("int64 cast produced wrong type".to_string()))
}

/// Sums diffs of adjacent equal rows, returning the kept source position and sum.
///
/// Errors with `Infrastructure` if the running sum overflows `i64`.
fn grouped_sums(
    rows: &Rows,
    order: &UInt32Array,
    diffs: &Int64Array,
) -> Result<Vec<(usize, i64)>, EngineError> {
    let mut groups: Vec<(usize, i64)> = Vec::new();
    for &value in order.values() {
        let position = value as usize;
        let sum = diffs.value(position);
        match groups.last_mut() {
            Some(group) if rows.row(group.0) == rows.row(position) => {
                group.1 = group.1.checked_add(sum).ok_or_else(|| {
                    EngineError::Infrastructure("consolidated diff sum overflowed i64".to_string())
                })?;
            }
            _ => groups.push((position, sum)),
        }
    }
    Ok(groups)
}

/// Applies the given row order to all columns of `batch`.
fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch, EngineError> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        columns.push(take(column.as_ref(), indices, None)?);
    }
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Reorders both the data columns and the diff column of `zset`.
fn take_zset(zset: &ZSetBatch, indices: &UInt32Array) -> Result<ZSetBatch, EngineError> {
    let batch = take_batch(&zset.batch, indices)?;
    let diff = take(zset.diff.as_ref(), indices, None)?;
    Ok(ZSetBatch::new(batch, diff)?)
}
