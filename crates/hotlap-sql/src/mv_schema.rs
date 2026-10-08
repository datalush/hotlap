//! Derive a materialized view's Arrow schema from its kernel plan.
//!
//! The kernel defines the output order: `key ++ [window_start, count]` for a
//! tumbling-window count and `key ++ [count]` for a plain group count. The
//! schema must follow that order so the MV table provider decodes rows back
//! into the columns the query over the view expects.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap::{Plan, aggregate_output_type};

use crate::error::SqlError;

/// Build the view schema for `plan` by resolving key indices against `source`.
pub fn mv_schema(plan: &Plan, source: &Schema) -> Result<SchemaRef, SqlError> {
    Ok(Arc::new(Schema::new(output_fields(plan, source)?)))
}

/// The output fields of `plan`, following the kernel's column order.
fn output_fields(plan: &Plan, source: &Schema) -> Result<Vec<Field>, SqlError> {
    match plan {
        Plan::Source(_) => Ok(source.fields().iter().map(|f| f.as_ref().clone()).collect()),
        Plan::Filter { input, .. } => output_fields(input, source),
        Plan::Project { input, cols } => {
            let input = output_fields(input, source)?;
            cols.iter().map(|&i| field_at(&input, i)).collect()
        }
        Plan::GroupAggregate { input, key, aggs } => {
            let input = output_fields(input, source)?;
            let mut fields = key_fields(&input, key)?;
            for agg in aggs {
                let input_ty = match agg.input {
                    Some(index) => Some(field_at(&input, index)?.data_type().clone()),
                    None => None,
                };
                let ty = aggregate_output_type(agg.func, input_ty.as_ref())
                    .map_err(|e| SqlError::Unsupported(e.to_string()))?;
                fields.push(Field::new(agg.func.output_name(), ty, true));
            }
            Ok(fields)
        }
        Plan::TumbleCount { input, key, .. } => {
            let input = output_fields(input, source)?;
            let mut fields = key_fields(&input, key)?;
            fields.push(int_field("window_start"));
            fields.push(int_field("count"));
            Ok(fields)
        }
        Plan::Join { left, right, .. } => {
            let mut fields = output_fields(left, source)?;
            fields.extend(output_fields(right, source)?);
            Ok(fields)
        }
    }
}

fn key_fields(input: &[Field], key: &[usize]) -> Result<Vec<Field>, SqlError> {
    key.iter().map(|&i| field_at(input, i)).collect()
}

fn field_at(fields: &[Field], index: usize) -> Result<Field, SqlError> {
    fields
        .get(index)
        .cloned()
        .ok_or_else(|| SqlError::Unsupported(format!("view schema column {index} out of range")))
}

fn int_field(name: &str) -> Field {
    Field::new(name, DataType::Int64, true)
}

#[cfg(test)]
mod tests {
    use hotlap::{AggSpec, InputId};

    use super::*;

    fn source() -> Schema {
        Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("_event_time", DataType::Int64, true),
        ])
    }

    fn names(schema: &Schema) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }

    #[test]
    fn tumble_order_is_key_window_count() {
        let plan = Plan::TumbleCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            time_col: 1,
            size: 10_000,
        };
        let schema = mv_schema(&plan, &source()).unwrap();
        assert_eq!(names(&schema), vec!["k", "window_start", "count"]);
    }

    #[test]
    fn group_order_is_key_then_aggregates() {
        let plan = Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![AggSpec::count(), AggSpec::sum(0), AggSpec::avg(0)],
        };
        let schema = mv_schema(&plan, &source()).unwrap();
        assert_eq!(names(&schema), vec!["k", "count", "sum", "avg"]);
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        assert_eq!(schema.field(3).data_type(), &DataType::Float64);
    }

    #[test]
    fn project_uses_source_column_names() {
        let plan = Plan::Project {
            input: Box::new(Plan::Source(InputId(0))),
            cols: vec![1, 0],
        };
        let schema = mv_schema(&plan, &source()).unwrap();
        assert_eq!(names(&schema), vec!["_event_time", "k"]);
    }
}
