//! Per-source schema derivation: each `Source` resolves its own Arrow schema.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap::{InputId, Plan};
use hotlap_sql::bindings::InputSchemas;
use hotlap_sql::mv_schema::mv_schema;

fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, ty)| Field::new(*name, ty.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

fn names(schema: &Schema) -> Vec<&str> {
    schema.fields().iter().map(|f| f.name().as_str()).collect()
}

#[test]
fn join_concatenates_branch_fields_in_order() {
    let left = schema(&[("k", DataType::Int64), ("v", DataType::Float64)]);
    let right = schema(&[("k", DataType::Float64), ("w", DataType::Int64)]);
    let inputs = InputSchemas::from([(InputId(1), left), (InputId(2), right)]);
    let plan = Plan::Join {
        left: Box::new(Plan::Source(InputId(1))),
        right: Box::new(Plan::Source(InputId(2))),
        left_key: vec![0],
        right_key: vec![0],
    };
    let out = mv_schema(&plan, &inputs).unwrap();
    assert_eq!(names(&out), vec!["k", "v", "k", "w"]);
    assert_eq!(out.field(0).data_type(), &DataType::Int64);
    assert_eq!(out.field(1).data_type(), &DataType::Float64);
    assert_eq!(out.field(2).data_type(), &DataType::Float64);
    assert_eq!(out.field(3).data_type(), &DataType::Int64);
}

#[test]
fn source_lookup_uses_its_own_input() {
    // Two inputs with different shapes: resolving the second must not fall
    // back to the first source's schema.
    let first = schema(&[("x", DataType::Int64)]);
    let second = schema(&[("y", DataType::Utf8), ("z", DataType::Int64)]);
    let inputs = InputSchemas::from([(InputId(3), first), (InputId(4), second)]);
    let out = mv_schema(&Plan::Source(InputId(4)), &inputs).unwrap();
    assert_eq!(names(&out), vec!["y", "z"]);
    assert_eq!(out.field(0).data_type(), &DataType::Utf8);
}

#[test]
fn join_keeps_branch_order_when_keys_sit_elsewhere() {
    // The join key is not column 0 on either side; the output still follows
    // the branch order, not the key order.
    let left = schema(&[("v", DataType::Float64), ("k", DataType::Int64)]);
    let right = schema(&[("k", DataType::Int64), ("w", DataType::Utf8)]);
    let inputs = InputSchemas::from([(InputId(1), left), (InputId(2), right)]);
    let plan = Plan::Join {
        left: Box::new(Plan::Source(InputId(1))),
        right: Box::new(Plan::Source(InputId(2))),
        left_key: vec![1],
        right_key: vec![0],
    };
    let out = mv_schema(&plan, &inputs).unwrap();
    assert_eq!(names(&out), vec!["v", "k", "k", "w"]);
    assert_eq!(out.field(0).data_type(), &DataType::Float64);
    assert_eq!(out.field(1).data_type(), &DataType::Int64);
    assert_eq!(out.field(2).data_type(), &DataType::Int64);
    assert_eq!(out.field(3).data_type(), &DataType::Utf8);
}

#[test]
fn projection_over_join_indexes_the_right_branch() {
    let left = schema(&[("k", DataType::Int64), ("v", DataType::Float64)]);
    let right = schema(&[("k", DataType::Int64), ("w", DataType::Utf8)]);
    let inputs = InputSchemas::from([(InputId(1), left), (InputId(2), right)]);
    let join = Plan::Join {
        left: Box::new(Plan::Source(InputId(1))),
        right: Box::new(Plan::Source(InputId(2))),
        left_key: vec![0],
        right_key: vec![0],
    };
    // Select the right branch's `w` (absolute index 3) before the left `v`
    // (index 1): names and types must follow the selected indices.
    let plan = Plan::Project {
        input: Box::new(join),
        cols: vec![3, 1],
    };
    let out = mv_schema(&plan, &inputs).unwrap();
    assert_eq!(names(&out), vec!["w", "v"]);
    assert_eq!(out.field(0).data_type(), &DataType::Utf8);
    assert_eq!(out.field(1).data_type(), &DataType::Float64);
}

#[test]
fn unbound_input_is_rejected() {
    let inputs = InputSchemas::new();
    assert!(
        mv_schema(&Plan::Source(InputId(0)), &inputs).is_err(),
        "an input with no bound schema must be an error"
    );
}
