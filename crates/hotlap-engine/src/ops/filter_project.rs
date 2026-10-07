use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::{filter as arrow_filter, filter_record_batch};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::batch::ZSetBatch;
use crate::error::EngineError;

/// Keeps the rows of `zset` whose boolean `predicate` is true.
///
/// Both the data columns and the `diff` column are filtered together, so
/// negative diffs (retractions) survive whenever their row passes the filter.
pub fn filter(zset: &ZSetBatch, predicate: &BooleanArray) -> Result<ZSetBatch, EngineError> {
    if predicate.len() != zset.len() {
        return Err(EngineError::Unsupported(format!(
            "predicate length {} does not match batch rows {}",
            predicate.len(),
            zset.len()
        )));
    }
    let batch = filter_record_batch(&zset.batch, predicate)?;
    let diff = arrow_filter(zset.diff.as_ref(), predicate)?;
    Ok(ZSetBatch::new(batch, diff)?)
}

/// Selects the columns named by `cols`, keeping the `diff` column unchanged.
///
/// Columns may be reordered (and repeated) freely; row count and row identity
/// are preserved, so the output is the projected Z-set. Columns are shared by
/// `ArrayRef::clone`, so projection is O(number of selected columns).
pub fn project(zset: &ZSetBatch, cols: &[usize]) -> Result<ZSetBatch, EngineError> {
    let width = zset.schema().fields().len();
    if let Some(&bad) = cols.iter().find(|&&index| index >= width) {
        return Err(EngineError::Unsupported(format!(
            "project column index {bad} out of bounds for width {width}"
        )));
    }
    let fields: Vec<_> = cols
        .iter()
        .map(|&index| zset.schema().field(index).clone())
        .collect();
    let columns = cols
        .iter()
        .map(|&index| zset.batch.column(index).clone())
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    Ok(ZSetBatch::new(batch, zset.diff.clone())?)
}
