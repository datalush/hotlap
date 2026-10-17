//! Builds the `key ++ aggs` upsert batch for a set of changed groups.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, new_empty_array};
use arrow::compute::concat;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{Row, RowConverter};

use hotlap_core::plan::{AggSpec, aggregate_output_type};

use super::columns::{ColumnBuilder, OutputCell};
use crate::batch::ZSetBatch;
use crate::core::ipc::typed_keys::TypedKeyRow;
use crate::error::EngineError;

/// One group whose aggregate row must be retracted and/or inserted.
pub(crate) struct Change {
    /// Encoded key bytes.
    pub key: Vec<u8>,
    /// Prior materialized output row, retracted with diff `-1`.
    pub previous: Option<Vec<OutputCell>>,
    /// New materialized output row, inserted with diff `+1`.
    pub current: Option<Vec<OutputCell>>,
}

/// Builds the changelog for `changes` under the frozen input `schema`.
pub(crate) fn materialize(
    key: &[usize],
    aggs: &[AggSpec],
    schema: Option<&SchemaRef>,
    converter: Option<&RowConverter>,
    native_keys: Option<&std::collections::HashMap<Vec<u8>, TypedKeyRow>>,
    changes: &[Change],
) -> Result<ZSetBatch, EngineError> {
    let schema =
        schema.ok_or_else(|| EngineError::Infrastructure("group schema missing".to_string()))?;
    let fields = Arc::new(Schema::new(output_fields(key, aggs, schema)?));
    if changes.is_empty() {
        return empty_output(&fields, key, aggs, schema);
    }
    let converter = converter
        .ok_or_else(|| EngineError::Infrastructure("group converter missing".to_string()))?;
    let emitted = flatten(changes);
    let mut columns = if key.is_empty() {
        Vec::new()
    } else if let Some(native_keys) = native_keys {
        (0..key.len())
            .map(|column| {
                let arrays = emitted
                    .iter()
                    .map(|(bytes, _, _)| {
                        native_keys
                            .get(*bytes)
                            .map(|row| row.batch().column(column).as_ref())
                            .ok_or_else(|| EngineError::Infrastructure("native group key representative missing during output materialization".into()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if arrays.is_empty() {
                    Ok(new_empty_array(fields.field(column).data_type()))
                } else {
                    concat(&arrays).map_err(Into::into)
                }
            })
            .collect::<Result<Vec<ArrayRef>, EngineError>>()?
    } else {
        let mut rows: Vec<Row<'_>> = Vec::with_capacity(emitted.len());
        let parser = converter.parser();
        for (bytes, _, _) in &emitted {
            rows.push(parser.parse(bytes));
        }
        converter.convert_rows(rows)?
    };
    let mut builders: Vec<ColumnBuilder> = aggs
        .iter()
        .map(|agg| ColumnBuilder::new(agg, schema))
        .collect::<Result<_, _>>()?;
    for (_, cells, _) in &emitted {
        for (builder, cell) in builders.iter_mut().zip(cells.iter()) {
            builder.push_cell(cell)?;
        }
    }
    for builder in builders {
        columns.push(builder.finish());
    }
    let batch = RecordBatch::try_new(fields, columns)?;
    let diff: ArrayRef = Arc::new(Int64Array::from(
        emitted.iter().map(|(_, _, diff)| *diff).collect::<Vec<_>>(),
    ));
    Ok(ZSetBatch::new(batch, diff)?)
}

/// Expands changes into `(key, cells, diff)` rows: old `-1`, new `+1`.
fn flatten(changes: &[Change]) -> Vec<(&[u8], &[OutputCell], i64)> {
    let mut emitted = Vec::new();
    for change in changes {
        if let Some(cells) = &change.previous {
            emitted.push((change.key.as_slice(), cells.as_slice(), -1));
        }
        if let Some(cells) = &change.current {
            emitted.push((change.key.as_slice(), cells.as_slice(), 1));
        }
    }
    emitted
}

/// Output fields: the key columns followed by one field per aggregate.
fn output_fields(
    key: &[usize],
    aggs: &[AggSpec],
    schema: &SchemaRef,
) -> Result<Vec<Field>, EngineError> {
    let mut fields: Vec<Field> = key.iter().map(|&i| schema.field(i).clone()).collect();
    for agg in aggs {
        let input = agg.input.map(|i| schema.field(i).data_type());
        let ty = aggregate_output_type(agg.func, input)?;
        fields.push(Field::new(agg.func.output_name(), ty, true));
    }
    Ok(fields)
}

/// An empty changelog carrying the output schema only.
fn empty_output(
    fields: &Arc<Schema>,
    key: &[usize],
    aggs: &[AggSpec],
    schema: &SchemaRef,
) -> Result<ZSetBatch, EngineError> {
    let mut columns: Vec<ArrayRef> = key
        .iter()
        .map(|&i| new_empty_array(schema.field(i).data_type()))
        .collect();
    for agg in aggs {
        let input = agg.input.map(|i| schema.field(i).data_type());
        columns.push(new_empty_array(&aggregate_output_type(agg.func, input)?));
    }
    let batch = RecordBatch::try_new(fields.clone(), columns)?;
    let diff: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    Ok(ZSetBatch::new(batch, diff)?)
}
