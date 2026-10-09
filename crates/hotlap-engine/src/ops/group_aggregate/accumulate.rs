//! Accumulator initialization and validation for grouped aggregates.

use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};

use hotlap_core::plan::{AggFunc, AggSpec, aggregate_output_type};
use hotlap_core::snapshot::{AggValue, GroupEntry, OrderedMultiset};

use crate::error::EngineError;

/// Validates every aggregate against the reducer's input `schema`.
pub(crate) fn validate_aggs(aggs: &[AggSpec], schema: &Schema) -> Result<(), EngineError> {
    if aggs.is_empty() {
        return Err(EngineError::Unsupported(
            "at least one aggregate is required".to_string(),
        ));
    }
    for agg in aggs {
        match agg.func {
            AggFunc::Count => {
                if let Some(index) = agg.input {
                    field(schema, index)?;
                }
            }
            AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
                let index = agg
                    .input
                    .ok_or_else(|| unsupported(agg, "requires an input column"))?;
                let ty = field(schema, index)?;
                aggregate_output_type(agg.func, Some(ty.data_type()))?;
            }
        }
    }
    Ok(())
}

/// Builds a zeroed accumulator set matching `aggs` and the input `schema`.
pub(crate) fn empty_entry(aggs: &[AggSpec], schema: &SchemaRef) -> Result<GroupEntry, EngineError> {
    let mut values = Vec::with_capacity(aggs.len());
    for agg in aggs {
        values.push(zeroed(agg, schema)?);
    }
    Ok(GroupEntry { rows: 0, values })
}

/// The zero accumulator matching `agg`'s function and input column type.
fn zeroed(agg: &AggSpec, schema: &SchemaRef) -> Result<AggValue, EngineError> {
    match agg.func {
        AggFunc::Count => Ok(AggValue::Count(0)),
        AggFunc::Sum => match input_type(agg, schema)? {
            DataType::Int32 | DataType::Int64 => Ok(AggValue::SumInteger { sum: 0, count: 0 }),
            DataType::Float64 => Ok(AggValue::SumFloat {
                sum: 0.0,
                count: 0,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            }),
            other => Err(unsupported(agg, &format!("input type {other:?}"))),
        },
        AggFunc::Avg => Ok(AggValue::Avg {
            sum: 0.0,
            count: 0,
            nan: 0,
            pos_inf: 0,
            neg_inf: 0,
        }),
        AggFunc::Min => Ok(AggValue::Min(OrderedMultiset::default())),
        AggFunc::Max => Ok(AggValue::Max(OrderedMultiset::default())),
    }
}

/// The declared data type of the aggregate's input column.
fn input_type(agg: &AggSpec, schema: &SchemaRef) -> Result<DataType, EngineError> {
    let index = agg
        .input
        .ok_or_else(|| unsupported(agg, "requires an input column"))?;
    Ok(field(schema, index)?.data_type().clone())
}

fn field(schema: &Schema, index: usize) -> Result<&FieldRef, EngineError> {
    schema.fields().get(index).ok_or_else(|| {
        EngineError::Unsupported(format!("group aggregate column {index} out of range"))
    })
}

fn unsupported(agg: &AggSpec, why: &str) -> EngineError {
    EngineError::Unsupported(format!("{:?} {why}", agg.func))
}
