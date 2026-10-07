//! Arrow -> kernel conversion. The kernel's scalar model is Null/I64/Str/Bool.

use arrow::array::{Array, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use hotlap::{ChangeBatch, Row, Scalar};

use crate::error::ConnectorError;

/// Reject schemas with columns the kernel cannot represent.
pub fn ensure_supported(schema: &Schema) -> Result<(), ConnectorError> {
    for field in schema.fields() {
        match field.data_type() {
            DataType::Int64 | DataType::Utf8 | DataType::Boolean => {}
            // Only millisecond timestamps are representable as epoch milliseconds;
            // any other unit is rejected explicitly rather than coerced.
            DataType::Timestamp(TimeUnit::Millisecond, _) => {}
            other => {
                return Err(ConnectorError::Unsupported(format!(
                    "column `{}` has unsupported type {other:?}",
                    field.name()
                )));
            }
        }
    }
    Ok(())
}

/// Convert an Arrow batch into a kernel change batch (every row is an insertion).
pub fn to_change_batch(batch: &RecordBatch) -> Result<ChangeBatch, ConnectorError> {
    ensure_supported(batch.schema().as_ref())?;
    let mut out = ChangeBatch::default();
    for row in 0..batch.num_rows() {
        let mut cols = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
            cols.push(scalar_at(col.as_ref(), row)?);
        }
        out.push(Row(cols), 1);
    }
    Ok(out)
}

fn scalar_at(array: &dyn Array, row: usize) -> Result<Scalar, ConnectorError> {
    if array.is_null(row) {
        return Ok(Scalar::Null);
    }
    match array.data_type() {
        DataType::Int64 => Ok(Scalar::I64(downcast::<Int64Array>(array)?.value(row))),
        DataType::Utf8 => Ok(Scalar::Str(
            downcast::<StringArray>(array)?.value(row).to_string(),
        )),
        DataType::Boolean => Ok(Scalar::Bool(downcast::<BooleanArray>(array)?.value(row))),
        // Millisecond timestamps map to `I64` holding milliseconds since the
        // epoch; the timezone, if any, does not change the underlying value.
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(Scalar::I64(
            downcast::<TimestampMillisecondArray>(array)?.value(row),
        )),
        other => Err(ConnectorError::Unsupported(format!(
            "unsupported column type {other:?}"
        ))),
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, ConnectorError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| ConnectorError::Arrow("array downcast failed".into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use hotlap::{Row, Scalar};

    use super::*;

    fn batch(schema: Arc<Schema>, cols: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(schema, cols).unwrap()
    }

    #[test]
    fn maps_i64_str_bool_and_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Boolean, true),
        ]));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
        ];
        let cb = to_change_batch(&batch(schema, cols)).unwrap();
        assert_eq!(cb.rows.len(), 2);
        assert_eq!(
            cb.rows[0],
            (
                Row(vec![
                    Scalar::I64(1),
                    Scalar::Str("x".into()),
                    Scalar::Bool(true)
                ]),
                1
            )
        );
        assert_eq!(
            cb.rows[1],
            (
                Row(vec![Scalar::Null, Scalar::Str("y".into()), Scalar::Null]),
                1
            )
        );
    }

    #[test]
    fn rejects_unsupported_type() {
        let schema = Schema::new(vec![Field::new("f", DataType::Float64, true)]);
        let err = ensure_supported(&schema).unwrap_err();
        assert!(matches!(err, ConnectorError::Unsupported(_)));
    }

    #[test]
    fn maps_millisecond_timestamp_to_i64() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let cols: Vec<ArrayRef> = vec![Arc::new(TimestampMillisecondArray::from(vec![1000, 2000]))];
        let cb = to_change_batch(&batch(schema, cols)).unwrap();
        assert_eq!(cb.rows[0], (Row(vec![Scalar::I64(1000)]), 1));
        assert_eq!(cb.rows[1], (Row(vec![Scalar::I64(2000)]), 1));
    }

    #[test]
    fn rejects_non_millisecond_timestamp() {
        let schema = Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]);
        let err = ensure_supported(&schema).unwrap_err();
        assert!(matches!(err, ConnectorError::Unsupported(_)));
    }
}
