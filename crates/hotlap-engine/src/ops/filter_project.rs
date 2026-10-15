use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::{FilterBuilder, filter as arrow_filter, filter_record_batch};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::batch::ZSetBatch;
use crate::error::EngineError;

#[cfg(test)]
thread_local! {
    static FUSED_FILTER_PROJECT_KERNELS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_fused_filter_project_kernel_count() {
    FUSED_FILTER_PROJECT_KERNELS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn fused_filter_project_kernel_count() -> u64 {
    FUSED_FILTER_PROJECT_KERNELS.with(std::cell::Cell::get)
}

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
    if cols.is_empty() {
        return Err(EngineError::Unsupported(
            "project requires at least one column".to_string(),
        ));
    }
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

/// Applies a filter mask and projection without filtering columns discarded by
/// the projection. This is deliberately private to the graph fast path: callers
/// must establish that the old full filter cannot fail before using it.
pub(crate) fn filter_project(
    zset: &ZSetBatch,
    predicate: &BooleanArray,
    cols: &[usize],
) -> Result<ZSetBatch, EngineError> {
    if predicate.len() != zset.len() {
        return Err(EngineError::Unsupported(format!(
            "predicate length {} does not match batch rows {}",
            predicate.len(),
            zset.len()
        )));
    }

    let width = zset.schema().fields().len();
    if cols.is_empty() || cols.iter().any(|&index| index >= width) {
        let filtered = filter(zset, predicate)?;
        return project(&filtered, cols);
    }

    // Filtering every input column first is the legacy error behavior. Keep
    // nested/encoded arrays on that path: even a valid container can recursively
    // filter a child whose data-dependent Arrow error is observable. The fast
    // path therefore admits only the scalar kernels listed below, across all
    // input columns (including those the projection discards).
    if zset
        .batch
        .columns()
        .iter()
        .any(|column| !filter_kernel_supported(column.data_type()))
    {
        let filtered = filter(zset, predicate)?;
        return project(&filtered, cols);
    }

    #[cfg(test)]
    FUSED_FILTER_PROJECT_KERNELS.with(|count| count.set(count.get() + 1));

    // Match Arrow's filter_record_batch policy for the data columns: it prepares
    // the predicate when filtering multiple columns. The legacy diff filter is
    // arrow_filter, so keep that separate to preserve its predicate strategy.
    let mut filter_builder = FilterBuilder::new(predicate);
    if zset.batch.num_columns() > 1 {
        filter_builder = filter_builder.optimize();
    }
    let filter = filter_builder.build();
    let mut first_output_position: Vec<Option<usize>> = vec![None; width];
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for &index in cols {
        if let Some(first_position) = first_output_position[index] {
            columns.push(columns[first_position].clone());
        } else {
            let array = filter.filter(zset.batch.column(index).as_ref())?;
            first_output_position[index] = Some(columns.len());
            columns.push(array);
        }
    }
    let fields: Vec<_> = cols
        .iter()
        .map(|&index| zset.schema().field(index).clone())
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    let diff = arrow_filter(zset.diff.as_ref(), predicate)?;
    Ok(ZSetBatch::new(batch, diff)?)
}

fn filter_kernel_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::BinaryView
            | DataType::Utf8View
    )
}

#[cfg(test)]
#[path = "filter_project/tests.rs"]
mod tests;
