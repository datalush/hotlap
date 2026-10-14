//! Immutable, schema-resolved column demand for maintained relation metadata.

use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use hotlap_core::plan::{AggFunc, AggSpec};
use hotlap_core::plan_normalize::{ColumnMapping, DenseReason};

/// Per-physical-relation inputs needed by future maintained row statistics.
///
/// This value is compiled once from a typed normalized plan. It intentionally
/// contains no mutable row state or arithmetic; each physical arrangement
/// owns its own instance even when another arrangement has an equal layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Demand {
    pub(crate) left_join_key: Vec<usize>,
    pub(crate) right_join_key: Vec<usize>,
    /// Positions in the complete canonical join key; order and duplicates
    /// preserve the requested projected group-key mapping.
    pub(crate) group_key: Vec<usize>,
    /// Sorted unique source-local columns required for COUNT(expr)/integer SUM.
    pub(crate) left_columns: Vec<usize>,
    pub(crate) right_columns: Vec<usize>,
    /// Exact resolved types parallel to the selected source-local columns.
    pub(crate) left_column_types: Vec<DataType>,
    pub(crate) right_column_types: Vec<DataType>,
    pub(crate) left_column_sums: Vec<bool>,
    pub(crate) right_column_sums: Vec<bool>,
}

/// Resolves only supported maintained-stat demands. Other valid aggregates
/// remain eligible for the existing Dense path, rather than being rejected.
pub(crate) fn compile_demand(
    mapping: &ColumnMapping,
    aggregates: &[AggSpec],
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Result<Demand, DenseReason> {
    if mapping.aggregate_inputs.len() != mapping.aggregate_input_types.len()
        || mapping.aggregate_inputs.len() != aggregates.len()
    {
        return Err(DenseReason::UnsupportedInputShape);
    }
    for &column in &mapping.left_join_key {
        if column >= left_schema.fields().len() {
            return Err(DenseReason::ColumnOutOfRange {
                input: mapping.left_source,
                column,
            });
        }
    }
    for &column in &mapping.right_join_key {
        if column >= right_schema.fields().len() {
            return Err(DenseReason::ColumnOutOfRange {
                input: mapping.right_source,
                column,
            });
        }
    }
    let mut left_columns = Vec::new();
    let mut right_columns = Vec::new();
    for ((input, data_type), aggregate) in mapping
        .aggregate_inputs
        .iter()
        .zip(&mapping.aggregate_input_types)
        .zip(aggregates)
    {
        if !matches!(aggregate.func, AggFunc::Count | AggFunc::Sum) {
            continue;
        }
        if aggregate.func == AggFunc::Count && aggregate.input.is_none() {
            continue;
        }
        let Some(column) = input else { continue };
        let Some(data_type) = data_type else {
            return Err(DenseReason::UnsupportedInputShape);
        };
        if aggregate.func == AggFunc::Sum && !matches!(data_type, DataType::Int32 | DataType::Int64)
        {
            continue;
        }
        let left_width = left_schema.fields().len();
        if *column >= left_width + right_schema.fields().len() {
            return Err(DenseReason::AggregateInputOutOfRange(*column));
        }
        if *column < left_width {
            let actual = left_schema.field(*column).data_type();
            if actual != data_type {
                return Err(DenseReason::UnsupportedAggregateType {
                    func: aggregate.func,
                    data_type: actual.clone(),
                });
            }
            left_columns.push((*column, data_type.clone(), aggregate.func == AggFunc::Sum));
        } else {
            let local = column - left_width;
            if local >= right_schema.fields().len() {
                return Err(DenseReason::AggregateInputOutOfRange(*column));
            }
            let actual = right_schema.field(local).data_type();
            if actual != data_type {
                return Err(DenseReason::UnsupportedAggregateType {
                    func: aggregate.func,
                    data_type: actual.clone(),
                });
            }
            right_columns.push((local, data_type.clone(), aggregate.func == AggFunc::Sum));
        }
    }
    let (left_columns, left_column_types, left_column_sums) = sorted_unique_columns(left_columns)?;
    let (right_columns, right_column_types, right_column_sums) =
        sorted_unique_columns(right_columns)?;
    Ok(Demand {
        left_join_key: mapping.left_join_key.clone(),
        right_join_key: mapping.right_join_key.clone(),
        group_key: mapping.group_key.clone(),
        left_columns,
        right_columns,
        left_column_types,
        right_column_types,
        left_column_sums,
        right_column_sums,
    })
}

fn sorted_unique_columns(
    mut selected: Vec<(usize, DataType, bool)>,
) -> Result<(Vec<usize>, Vec<DataType>, Vec<bool>), DenseReason> {
    selected.sort_unstable_by_key(|(column, _, _)| *column);
    let mut columns = Vec::with_capacity(selected.len());
    let mut types = Vec::with_capacity(selected.len());
    let mut sums = Vec::with_capacity(selected.len());
    for (column, data_type, sum) in selected {
        if columns.last() == Some(&column) {
            if types.last() != Some(&data_type) {
                return Err(DenseReason::UnsupportedInputShape);
            }
            *sums.last_mut().expect("existing demand has sum flag") |= sum;
            continue;
        }
        columns.push(column);
        types.push(data_type);
        sums.push(sum);
    }
    Ok((columns, types, sums))
}
