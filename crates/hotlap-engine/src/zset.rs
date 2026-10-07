use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{cast, sort_to_indices, take};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};

use crate::batch::ZSetBatch;
use crate::error::EngineError;

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
    let rows = full_rows(&zset.batch)?;
    let order = order_rows_from(&rows)?;
    let diffs = int64_diffs(&zset.diff)?;

    let mut positions = Vec::new();
    let mut sums = Vec::new();
    for (position, sum) in grouped_sums(&rows, &order, &diffs) {
        if sum != 0 {
            positions.push(position as u32);
            sums.push(sum);
        }
    }

    let indices = UInt32Array::from(positions);
    let batch = take_batch(&zset.batch, &indices)?;
    let diff: ArrayRef = Arc::new(Int64Array::from(sums));
    ZSetBatch::new(batch, diff)
}

/// Encodes every column of `batch` into byte-comparable rows.
fn full_rows(batch: &RecordBatch) -> Result<Rows, EngineError> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| SortField::new(field.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields)?;
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
fn int64_diffs(diff: &ArrayRef) -> Result<Int64Array, EngineError> {
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
fn grouped_sums(rows: &Rows, order: &UInt32Array, diffs: &Int64Array) -> Vec<(usize, i64)> {
    let mut groups: Vec<(usize, i64)> = Vec::new();
    for &value in order.values() {
        let position = value as usize;
        let sum = diffs.value(position);
        match groups.last_mut() {
            Some(group) if rows.row(group.0) == rows.row(position) => group.1 += sum,
            _ => groups.push((position, sum)),
        }
    }
    groups
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
    ZSetBatch::new(batch, diff)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{consolidate, sort_rows};
    use crate::batch::ZSetBatch;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]))
    }

    fn zset(keys: Vec<i64>, values: Vec<&str>, diff: Vec<i64>) -> ZSetBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(StringArray::from(values)),
        ];
        let batch = RecordBatch::try_new(schema(), columns).unwrap();
        ZSetBatch::new(batch, Arc::new(Int64Array::from(diff))).unwrap()
    }

    fn diffs(zset: &ZSetBatch) -> Vec<i64> {
        let array = zset
            .diff
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 diff");
        (0..array.len()).map(|index| array.value(index)).collect()
    }

    fn key_values(zset: &ZSetBatch) -> Vec<i64> {
        let array = zset
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 key");
        (0..array.len()).map(|index| array.value(index)).collect()
    }

    #[test]
    fn consolidate_sums_duplicates_and_drops_zeros() {
        // +1 +1 -1 for a repeated full row leaves +1; a zero-sum row disappears.
        let input = zset(
            vec![1, 1, 1, 2, 2],
            vec!["a", "a", "a", "b", "b"],
            vec![1, 1, -1, 1, -1],
        );
        let out = consolidate(&input).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(key_values(&out), vec![1]);
        assert_eq!(diffs(&out), vec![1]);
    }

    #[test]
    fn consolidate_keeps_same_key_with_different_payload() {
        // Same key, different payload means different full rows: both survive.
        let input = zset(vec![1, 1], vec!["a", "b"], vec![1, 1]);
        let out = consolidate(&input).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(key_values(&out), vec![1, 1]);
        assert_eq!(diffs(&out), vec![1, 1]);
    }

    #[test]
    fn consolidate_is_deterministic() {
        let input = zset(vec![2, 1, 2], vec!["b", "a", "b"], vec![1, 1, 1]);
        let first = consolidate(&input).unwrap();
        let second = consolidate(&input).unwrap();
        assert_eq!(key_values(&first), key_values(&second));
        assert_eq!(diffs(&first), diffs(&second));
    }

    #[test]
    fn sort_rows_orders_by_key() {
        let input = zset(vec![3, 1, 2], vec!["c", "a", "b"], vec![1, 2, 3]);
        let out = sort_rows(&input).unwrap();
        assert_eq!(key_values(&out), vec![1, 2, 3]);
        assert_eq!(diffs(&out), vec![2, 3, 1]);
    }
}
