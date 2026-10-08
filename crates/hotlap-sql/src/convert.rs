//! Engine `ZSetBatch` -> Arrow `RecordBatch` using a view schema.

use arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap::ZSetBatch;

use crate::error::SqlError;

/// Reject schemas with columns the kernel cannot represent.
pub fn ensure_kernel_types(schema: &Schema) -> Result<(), SqlError> {
    for f in schema.fields() {
        match f.data_type() {
            DataType::Int64 | DataType::Utf8 | DataType::Boolean => {}
            other => {
                return Err(SqlError::Unsupported(format!(
                    "column `{}` has type {other:?}, not representable by the kernel",
                    f.name()
                )));
            }
        }
    }
    Ok(())
}

/// Build one Arrow batch from a consolidated Z-set, keeping rows with a
/// non-zero multiplicity and dropping the rest.
pub fn zset_to_batch(schema: &SchemaRef, zset: &ZSetBatch) -> Result<RecordBatch, SqlError> {
    ensure_kernel_types(schema)?;
    if zset.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let indices = nonzero_indices(zset.diff())?;
    let columns: Vec<ArrayRef> = zset
        .batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(bad))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(bad)
}

/// Indices of the rows whose multiplicity is non-zero.
fn nonzero_indices(diff: &ArrayRef) -> Result<UInt32Array, SqlError> {
    let casted = cast(diff.as_ref(), &DataType::Int64).map_err(bad)?;
    let values = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| SqlError::Unsupported("diff column is not int64".into()))?;
    let indices: Vec<u32> = (0..values.len())
        .filter(|&index| values.value(index) != 0)
        .map(|index| index as u32)
        .collect();
    Ok(UInt32Array::from(indices))
}

fn bad(error: impl std::fmt::Display) -> SqlError {
    SqlError::Unsupported(format!("record batch: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use hotlap::ZSetBatch;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]))
    }

    fn zset(keys: &[i64], values: &[&str], diff: &[i64]) -> ZSetBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(StringArray::from(values.to_vec())),
        ];
        let batch = RecordBatch::try_new(schema(), columns).unwrap();
        ZSetBatch::new(batch, Arc::new(Int64Array::from(diff.to_vec()))).unwrap()
    }

    #[test]
    fn zset_to_batch_keeps_nonzero_rows() {
        let batch = zset_to_batch(&schema(), &zset(&[1, 2], &["a", "b"], &[1, 0])).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );
    }

    #[test]
    fn empty_zset_yields_schema_only_batch() {
        let empty = ZSetBatch::empty(schema());
        let batch = zset_to_batch(&schema(), &empty).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn rejects_unsupported_type() {
        let bad = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        assert!(ensure_kernel_types(&bad).is_err());
    }
}
