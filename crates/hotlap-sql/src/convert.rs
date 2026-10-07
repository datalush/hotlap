//! Kernel `Row`/`Scalar` -> Arrow `RecordBatch` using a view schema.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap::{Row, Scalar};

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
                )))
            }
        }
    }
    Ok(())
}

/// Build one Arrow batch from kernel rows, using `schema` for column types.
pub fn rows_to_batch(schema: &SchemaRef, rows: &[Row]) -> Result<RecordBatch, SqlError> {
    ensure_kernel_types(schema)?;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (col, field) in schema.fields().iter().enumerate() {
        arrays.push(build_column(field.data_type(), col, rows)?);
    }
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| SqlError::Unsupported(format!("record batch: {e}")))
}

fn build_column(dt: &DataType, col: usize, rows: &[Row]) -> Result<ArrayRef, SqlError> {
    match dt {
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for r in rows {
                match r.0.get(col) {
                    Some(Scalar::I64(v)) => b.append_value(*v),
                    Some(Scalar::Null) | None => b.append_null(),
                    other => return Err(mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
            for r in rows {
                match r.0.get(col) {
                    Some(Scalar::Str(v)) => b.append_value(v),
                    Some(Scalar::Null) | None => b.append_null(),
                    other => return Err(mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for r in rows {
                match r.0.get(col) {
                    Some(Scalar::Bool(v)) => b.append_value(*v),
                    Some(Scalar::Null) | None => b.append_null(),
                    other => return Err(mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(SqlError::Unsupported(format!(
            "unsupported column type {other:?}"
        ))),
    }
}

fn mismatch(dt: &DataType, scalar: Option<&Scalar>) -> SqlError {
    SqlError::Unsupported(format!("value {scalar:?} does not match column type {dt:?}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use hotlap::{Row, Scalar};

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn rows_to_batch_maps_values_and_nulls() {
        let rows = vec![
            Row(vec![Scalar::I64(1), Scalar::Str("a".into())]),
            Row(vec![Scalar::Null, Scalar::Null]),
        ];
        let b = rows_to_batch(&schema(), &rows).unwrap();
        assert_eq!(b.num_rows(), 2);
        assert_eq!(
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert!(b.column(0).is_null(1));
        assert_eq!(
            b.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );
    }

    #[test]
    fn rejects_unsupported_type() {
        let bad = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        assert!(ensure_kernel_types(&bad).is_err());
    }
}
