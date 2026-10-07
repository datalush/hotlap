use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::row::{RowConverter, Rows, SortField};

use crate::error::EngineError;

/// Converts the key columns of a batch into the `arrow::row` byte format.
///
/// The wrapper caches the underlying [`RowConverter`] and the schema column
/// indices that make up the key, so callers only pass a full column set.
pub struct KeyConverter {
    inner: RowConverter,
    column_indices: Vec<usize>,
}

impl KeyConverter {
    /// Builds a converter over the key columns named by `column_indices`.
    ///
    /// Types are resolved from `schema`; indices must address existing fields.
    pub fn new(schema: &Schema, column_indices: &[usize]) -> Result<Self, EngineError> {
        let mut fields = Vec::with_capacity(column_indices.len());
        for &index in column_indices {
            let field = schema.fields().get(index).ok_or_else(|| {
                EngineError::Unsupported(format!("key column index {index} out of bounds"))
            })?;
            fields.push(SortField::new(field.data_type().clone()));
        }
        let inner = RowConverter::new(fields)?;
        Ok(Self {
            inner,
            column_indices: column_indices.to_vec(),
        })
    }

    /// Schema column indices treated as keys by this converter.
    pub fn column_indices(&self) -> &[usize] {
        &self.column_indices
    }

    /// Encodes the key columns of `columns` into comparable row bytes.
    pub fn convert(&self, columns: &[ArrayRef]) -> Result<Rows, EngineError> {
        let mut keys = Vec::with_capacity(self.column_indices.len());
        for &index in &self.column_indices {
            let column = columns.get(index).ok_or_else(|| {
                EngineError::Unsupported(format!("key column index {index} missing from columns"))
            })?;
            keys.push(column.clone());
        }
        Ok(self.inner.convert_columns(&keys)?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::batch::ZSetBatch;
    use crate::keys::KeyConverter;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(2), None, Some(1)])),
            Arc::new(StringArray::from(vec![Some("b"), Some("a"), Some("a")])),
        ];
        RecordBatch::try_new(schema, cols).unwrap()
    }

    #[test]
    fn keys_order_with_nulls() {
        // Nulls sort deterministically and do not panic; key order is lexicographic.
        let b = batch();
        let kc = KeyConverter::new(b.schema().as_ref(), &[0, 1]).unwrap();
        let rows = kc.convert(b.columns()).unwrap();
        // (None,"a") < (1,"a") < (2,"b")
        assert_eq!(rows.num_rows(), 3);
        let mut idx: Vec<usize> = (0..3).collect();
        idx.sort_by(|a, b| rows.row(*a).cmp(&rows.row(*b)));
        assert_eq!(idx, vec![1, 2, 0]);
    }

    #[test]
    fn zset_batch_holds_diff() {
        let b = batch();
        let diff: ArrayRef = Arc::new(Int64Array::from(vec![1i64, -1, 1]));
        let z = ZSetBatch { batch: b, diff };
        assert_eq!(z.len(), 3);
    }
}
