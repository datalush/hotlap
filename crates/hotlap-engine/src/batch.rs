use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::error::EngineError;

/// A Z-set represented as a `RecordBatch` plus a signed multiplicity column.
///
/// Each row of `batch` carries the multiplicity stored at the same position
/// in `diff`. The `diff` column must have as many rows as `batch`.
#[derive(Clone)]
pub struct ZSetBatch {
    /// The data rows of the Z-set.
    pub batch: RecordBatch,
    /// The signed multiplicity for every row in `batch`.
    pub diff: ArrayRef,
}

impl ZSetBatch {
    /// Builds a Z-set, rejecting a `diff` column whose length differs.
    pub fn new(batch: RecordBatch, diff: ArrayRef) -> Result<Self, EngineError> {
        if batch.num_rows() != diff.len() {
            return Err(EngineError::Unsupported(format!(
                "diff length {} does not match batch rows {}",
                diff.len(),
                batch.num_rows()
            )));
        }
        Ok(Self { batch, diff })
    }

    /// Number of rows in the Z-set.
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns `true` when the Z-set holds no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Schema of the underlying data rows.
    pub fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    /// Multiplicity bytes for every row.
    pub fn diff(&self) -> &ArrayRef {
        &self.diff
    }
}
