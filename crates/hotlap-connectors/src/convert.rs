//! Arrow batch -> Z-set conversion for the engine boundary.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use hotlap::ZSetBatch;

use crate::error::ConnectorError;

/// Reject schemas with columns the engine cannot represent.
pub fn ensure_supported(schema: &Schema) -> Result<(), ConnectorError> {
    for field in schema.fields() {
        match field.data_type() {
            DataType::Int64
            | DataType::Int32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Boolean => {}
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

/// Wrap an Arrow batch as a Z-set: every row is an insertion (`diff = +1`).
pub fn to_zset(batch: &RecordBatch) -> Result<ZSetBatch, ConnectorError> {
    ensure_supported(batch.schema().as_ref())?;
    let diff: ArrayRef = Arc::new(Int64Array::from(vec![1i64; batch.num_rows()]));
    ZSetBatch::new(batch.clone(), diff)
        .map_err(|error| ConnectorError::Unsupported(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn batch(schema: Arc<Schema>, cols: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(schema, cols).unwrap()
    }

    #[test]
    fn wraps_with_unit_diffs() {
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
        let z = to_zset(&batch(schema, cols)).unwrap();
        assert_eq!(z.len(), 2);
        assert_eq!(z.batch.num_columns(), 3);
        let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(diffs.values(), &[1, 1]);
        assert_eq!(z.batch.column(0).len(), 2);
    }

    #[test]
    fn rejects_unsupported_type() {
        let schema = Schema::new(vec![Field::new("f", DataType::UInt64, true)]);
        let err = ensure_supported(&schema).unwrap_err();
        assert!(matches!(err, ConnectorError::Unsupported(_)));
    }

    #[test]
    fn accepts_int32_and_float64() {
        let schema = Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("f", DataType::Float64, true),
        ]);
        assert!(ensure_supported(&schema).is_ok());
    }

    #[test]
    fn accepts_millisecond_timestamp() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let cols: Vec<ArrayRef> = vec![Arc::new(arrow::array::TimestampMillisecondArray::from(
            vec![1000, 2000],
        ))];
        let z = to_zset(&batch(schema, cols)).unwrap();
        assert_eq!(z.len(), 2);
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
