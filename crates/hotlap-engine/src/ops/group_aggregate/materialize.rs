//! Builds the `key ++ aggs` upsert batch for a set of changed groups.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, new_empty_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{Row, RowConverter};

use hotlap_core::plan::{AggSpec, aggregate_output_type};
use hotlap_core::snapshot::{AggValue, GroupEntry};

use crate::batch::ZSetBatch;
use crate::error::EngineError;

/// One group whose aggregate row must be retracted and/or inserted.
pub(crate) struct Change {
    /// Encoded key bytes.
    pub key: Vec<u8>,
    /// Prior aggregate row, retracted with diff `-1`.
    pub previous: Option<GroupEntry>,
    /// New aggregate row, inserted with diff `+1`.
    pub current: Option<GroupEntry>,
}

/// Builds the changelog for `changes` under the frozen input `schema`.
pub(crate) fn materialize(
    key: &[usize],
    aggs: &[AggSpec],
    schema: Option<&SchemaRef>,
    converter: Option<&RowConverter>,
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
    let mut rows: Vec<Row<'_>> = Vec::with_capacity(emitted.len());
    let parser = converter.parser();
    for (bytes, _, _) in &emitted {
        rows.push(parser.parse(bytes));
    }
    let mut columns = converter.convert_rows(rows)?;
    let mut builders: Vec<ColumnBuilder> = aggs
        .iter()
        .map(|agg| ColumnBuilder::new(agg, schema))
        .collect::<Result<_, _>>()?;
    for (_, entry, _) in &emitted {
        for (builder, value) in builders.iter_mut().zip(entry.values.iter()) {
            builder.push(value)?;
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

/// Expands changes into `(key, entry, diff)` rows: old `-1`, new `+1`.
fn flatten(changes: &[Change]) -> Vec<(&[u8], &GroupEntry, i64)> {
    let mut emitted = Vec::new();
    for change in changes {
        if let Some(entry) = &change.previous {
            emitted.push((change.key.as_slice(), entry, -1));
        }
        if let Some(entry) = &change.current {
            emitted.push((change.key.as_slice(), entry, 1));
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

/// Accumulates one output column, fixing its Arrow type up front.
enum ColumnBuilder {
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
}

impl ColumnBuilder {
    /// Picks the builder matching `agg`'s resolved output type.
    fn new(agg: &AggSpec, schema: &SchemaRef) -> Result<Self, EngineError> {
        let input = agg.input.map(|i| schema.field(i).data_type());
        match aggregate_output_type(agg.func, input)? {
            DataType::Int64 => Ok(ColumnBuilder::Int(Vec::new())),
            DataType::Float64 => Ok(ColumnBuilder::Float(Vec::new())),
            other => Err(EngineError::Unsupported(format!(
                "aggregate output type {other:?} is not supported"
            ))),
        }
    }

    /// Appends one accumulated value, mapping empty sums/averages to null.
    fn push(&mut self, value: &AggValue) -> Result<(), EngineError> {
        match (self, value) {
            (ColumnBuilder::Int(out), AggValue::Count(count)) => out.push(Some(*count)),
            (ColumnBuilder::Int(out), AggValue::SumInteger { sum, count }) => {
                out.push(sum_cell(*count, *sum)?);
            }
            (ColumnBuilder::Float(out), AggValue::SumFloat { sum, count }) => {
                out.push(if *count == 0 { None } else { Some(*sum) });
            }
            (ColumnBuilder::Float(out), AggValue::Avg { sum, count }) => {
                out.push(if *count == 0 {
                    None
                } else {
                    Some(*sum / *count as f64)
                });
            }
            _ => {
                return Err(EngineError::Infrastructure(
                    "aggregate output type mismatch".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Freezes the column into an Arrow array.
    fn finish(self) -> ArrayRef {
        match self {
            ColumnBuilder::Int(values) => Arc::new(Int64Array::from(values)),
            ColumnBuilder::Float(values) => Arc::new(Float64Array::from(values)),
        }
    }
}

/// The `Int64` sum cell: null when empty, checked for range otherwise.
fn sum_cell(count: i64, sum: i128) -> Result<Option<i64>, EngineError> {
    if count == 0 {
        return Ok(None);
    }
    let value = i64::try_from(sum)
        .map_err(|_| EngineError::Infrastructure("group sum out of range for Int64".to_string()))?;
    Ok(Some(value))
}
