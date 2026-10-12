//! Typed logical recognition for the bounded factorized join/aggregate shape.

use std::collections::HashMap;

use arrow::datatypes::{DataType, SchemaRef};

use crate::InputId;
use crate::plan::{AggFunc, Plan};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DenseReason {
    NotGroupAggregate,
    UnsupportedInputShape,
    RepeatedSource(InputId),
    MissingSourceSchema(InputId),
    ColumnOutOfRange { input: InputId, column: usize },
    JoinKeyArityMismatch,
    EmptyJoinKey,
    UnsupportedJoinType { left: DataType, right: DataType },
    EmptyGroupKey,
    GroupKeyNotJoinKey(usize),
    UnsupportedAggregate(AggFunc),
    MissingAggregateInput(AggFunc),
    AggregateInputOutOfRange(usize),
    UnsupportedAggregateType { func: AggFunc, data_type: DataType },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnMapping {
    pub left_source: InputId,
    pub right_source: InputId,
    /// Join key positions local to the projected left join input.
    pub left_join_key: Vec<usize>,
    /// Join key positions local to the projected right join input.
    pub right_join_key: Vec<usize>,
    /// Group key positions in the full join key, preserving output order and duplicates.
    pub group_key: Vec<usize>,
    /// Aggregate inputs normalized to the left||right join output positions.
    pub aggregate_inputs: Vec<Option<usize>>,
    pub join_key_types: Vec<DataType>,
    /// `None` for COUNT(*); otherwise the exact resolved Arrow input type.
    pub aggregate_input_types: Vec<Option<DataType>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Recognition {
    Eligible(ColumnMapping),
    Dense(DenseReason),
}

#[derive(Clone)]
struct Relation {
    id: InputId,
    /// Output-position to physical source-column mapping; duplicates preserved.
    columns: Vec<usize>,
}

/// Recognize without rewriting or physically specializing the supplied plan.
pub fn recognize(plan: &Plan, schemas: &HashMap<InputId, SchemaRef>) -> Recognition {
    let Plan::GroupAggregate { input, key, aggs } = plan else {
        return Recognition::Dense(DenseReason::NotGroupAggregate);
    };
    if key.is_empty() {
        return Recognition::Dense(DenseReason::EmptyGroupKey);
    }
    let (join, outer_project) = match input.as_ref() {
        Plan::Join { .. } => (input.as_ref(), None),
        Plan::Project { input, cols } if matches!(input.as_ref(), Plan::Join { .. }) => {
            (input.as_ref(), Some(cols.as_slice()))
        }
        _ => return Recognition::Dense(DenseReason::UnsupportedInputShape),
    };
    let Plan::Join {
        left,
        right,
        left_key,
        right_key,
    } = join
    else {
        unreachable!()
    };
    let left = match relation(left, schemas) {
        Ok(r) => r,
        Err(e) => return Recognition::Dense(e),
    };
    let right = match relation(right, schemas) {
        Ok(r) => r,
        Err(e) => return Recognition::Dense(e),
    };
    if left.id == right.id {
        return Recognition::Dense(DenseReason::RepeatedSource(left.id));
    }
    if left_key.len() != right_key.len() {
        return Recognition::Dense(DenseReason::JoinKeyArityMismatch);
    }
    if left_key.is_empty() {
        return Recognition::Dense(DenseReason::EmptyJoinKey);
    }
    let (left_schema, right_schema) = (&schemas[&left.id], &schemas[&right.id]);
    let mut left_join_key = Vec::new();
    let mut right_join_key = Vec::new();
    let mut join_key_types = Vec::new();
    for (&lk, &rk) in left_key.iter().zip(right_key) {
        let Some(&l) = left.columns.get(lk) else {
            return Recognition::Dense(DenseReason::ColumnOutOfRange {
                input: left.id,
                column: lk,
            });
        };
        let Some(&r) = right.columns.get(rk) else {
            return Recognition::Dense(DenseReason::ColumnOutOfRange {
                input: right.id,
                column: rk,
            });
        };
        let lt = left_schema.field(l).data_type();
        let rt = right_schema.field(r).data_type();
        if lt != rt || !matches!(lt, DataType::Int32 | DataType::Int64) {
            return Recognition::Dense(DenseReason::UnsupportedJoinType {
                left: lt.clone(),
                right: rt.clone(),
            });
        }
        // Join indices remain local to the joined side. Provenance is used
        // only to resolve source identity and type, never as a concat offset.
        left_join_key.push(lk);
        right_join_key.push(rk);
        join_key_types.push(lt.clone());
    }
    // Normalize group/aggregate positions through the optional outer projection.
    let join_width = left.columns.len() + right.columns.len();
    let output_columns: Vec<usize> = match outer_project {
        Some(cols) => {
            if let Some(&column) = cols.iter().find(|&&column| column >= join_width) {
                return Recognition::Dense(DenseReason::ColumnOutOfRange {
                    input: right.id,
                    column: column.saturating_sub(left.columns.len()),
                });
            }
            cols.to_vec()
        }
        None => (0..join_width).collect(),
    };
    let mut group_key = Vec::new();
    for &group_col in key {
        let Some(&joined_col) = output_columns.get(group_col) else {
            return Recognition::Dense(DenseReason::ColumnOutOfRange {
                input: left.id,
                column: group_col,
            });
        };
        // Equality keys must be named from the left output, but grouping may
        // use the corresponding key from either join side.
        let position = if joined_col < left.columns.len() {
            left_join_key.iter().position(|&col| col == joined_col)
        } else {
            right_join_key
                .iter()
                .position(|&col| col == joined_col - left.columns.len())
        };
        let Some(position) = position else {
            return Recognition::Dense(DenseReason::GroupKeyNotJoinKey(group_col));
        };
        group_key.push(position);
    }
    let mut aggregate_inputs = Vec::new();
    let mut aggregate_input_types = Vec::new();
    for agg in aggs {
        if !matches!(agg.func, AggFunc::Count | AggFunc::Sum | AggFunc::Avg) {
            return Recognition::Dense(DenseReason::UnsupportedAggregate(agg.func));
        }
        let Some(input_col) = agg.input else {
            if agg.func != AggFunc::Count {
                return Recognition::Dense(DenseReason::MissingAggregateInput(agg.func));
            }
            aggregate_inputs.push(None);
            aggregate_input_types.push(None);
            continue;
        };
        let Some(&joined_col) = output_columns.get(input_col) else {
            return Recognition::Dense(DenseReason::AggregateInputOutOfRange(input_col));
        };
        let (schema, source_column) = if joined_col < left.columns.len() {
            (left_schema, left.columns[joined_col])
        } else {
            let pos = joined_col - left.columns.len();
            (right_schema, right.columns[pos])
        };
        let ty = schema.field(source_column).data_type();
        if matches!(agg.func, AggFunc::Sum | AggFunc::Avg)
            && !matches!(ty, DataType::Int32 | DataType::Int64)
        {
            return Recognition::Dense(DenseReason::UnsupportedAggregateType {
                func: agg.func,
                data_type: ty.clone(),
            });
        }
        // Return the normalized join-concatenation index, not the immediate
        // GroupAggregate input index (which may be projected/reordered).
        aggregate_inputs.push(Some(joined_col));
        aggregate_input_types.push(Some(ty.clone()));
    }
    Recognition::Eligible(ColumnMapping {
        left_source: left.id,
        right_source: right.id,
        left_join_key,
        right_join_key,
        group_key,
        aggregate_inputs,
        join_key_types,
        aggregate_input_types,
    })
}

fn relation(plan: &Plan, schemas: &HashMap<InputId, SchemaRef>) -> Result<Relation, DenseReason> {
    let (id, selected) = match plan {
        Plan::Source(id) => (*id, None),
        Plan::Project { input, cols } => match input.as_ref() {
            Plan::Source(id) => (*id, Some(cols.as_slice())),
            _ => return Err(DenseReason::UnsupportedInputShape),
        },
        _ => return Err(DenseReason::UnsupportedInputShape),
    };
    let schema = schemas
        .get(&id)
        .ok_or(DenseReason::MissingSourceSchema(id))?;
    let columns = selected
        .map(<[usize]>::to_vec)
        .unwrap_or_else(|| (0..schema.fields().len()).collect());
    if let Some(&column) = columns.iter().find(|&&col| col >= schema.fields().len()) {
        return Err(DenseReason::ColumnOutOfRange { input: id, column });
    }
    Ok(Relation { id, columns })
}

#[cfg(test)]
#[path = "plan_normalize/tests.rs"]
mod tests;
