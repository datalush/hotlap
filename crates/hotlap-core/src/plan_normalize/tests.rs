use super::*;
use crate::plan::AggSpec;
use arrow::datatypes::{Field, Schema};

fn schemas() -> HashMap<InputId, SchemaRef> {
    HashMap::from([
        (
            InputId(1),
            Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("v", DataType::Int32, true),
            ])
            .into(),
        ),
        (
            InputId(2),
            Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("w", DataType::Int64, true),
            ])
            .into(),
        ),
    ])
}

fn plan() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Project {
                input: Box::new(Plan::Source(InputId(1))),
                cols: vec![1, 0, 1],
            }),
            right: Box::new(Plan::Project {
                input: Box::new(Plan::Source(InputId(2))),
                cols: vec![1, 0],
            }),
            left_key: vec![1],
            right_key: vec![1],
        }),
        key: vec![1],
        aggs: vec![AggSpec::count(), AggSpec::sum(0), AggSpec::avg(1)],
    }
}

#[test]
fn recognizes_projected_join_columns_and_group_subset() {
    let Recognition::Eligible(mapping) = recognize(&plan(), &schemas()) else {
        panic!("expected recognized shape")
    };
    assert_eq!(mapping.left_source, InputId(1));
    assert_eq!(mapping.left_join_key, vec![1]);
    assert_eq!(mapping.right_join_key, vec![1]);
    assert_eq!(mapping.group_key, vec![0]);
    assert_eq!(mapping.aggregate_inputs, vec![None, Some(0), Some(1)]);
}

#[test]
fn rejects_float_aggregate_with_structured_reason() {
    let mut schemas = schemas();
    schemas.insert(
        InputId(2),
        Schema::new(vec![
            Field::new("w", DataType::Float64, true),
            Field::new("id", DataType::Int64, true),
        ])
        .into(),
    );
    let p = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(1))),
            right: Box::new(Plan::Source(InputId(2))),
            left_key: vec![0],
            right_key: vec![1],
        }),
        key: vec![0],
        aggs: vec![AggSpec::avg(2)],
    };
    assert!(matches!(
        recognize(&p, &schemas),
        Recognition::Dense(DenseReason::UnsupportedAggregateType {
            func: AggFunc::Avg,
            data_type: DataType::Float64
        })
    ));
}

#[test]
fn accepts_count_of_float_as_dense_compatible_non_null_count() {
    let mut schemas = schemas();
    schemas.insert(
        InputId(2),
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("w", DataType::Float64, true),
        ])
        .into(),
    );
    let mut p = plan();
    if let Plan::GroupAggregate { input, aggs, .. } = &mut p {
        if let Plan::Join { right, .. } = input.as_mut()
            && let Plan::Project { cols, .. } = right.as_mut()
        {
            *cols = vec![1, 0];
        }
        aggs[0] = AggSpec::count();
        aggs[1] = AggSpec::count();
        aggs[2] = AggSpec::count();
        aggs[2].input = Some(3);
    }
    let Recognition::Eligible(mapping) = recognize(&p, &schemas) else {
        panic!("COUNT(column) over Float64 remains eligible")
    };
    assert_eq!(mapping.aggregate_input_types[2], Some(DataType::Float64));
}

#[test]
fn composes_optional_join_projection_and_preserves_duplicate_group_fields() {
    let schemas = schemas();
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Join {
                left: Box::new(Plan::Project {
                    input: Box::new(Plan::Source(InputId(1))),
                    cols: vec![0, 1],
                }),
                right: Box::new(Plan::Project {
                    input: Box::new(Plan::Source(InputId(2))),
                    cols: vec![0, 1, 1],
                }),
                left_key: vec![0],
                right_key: vec![0],
            }),
            cols: vec![4, 0, 2, 0],
        }),
        key: vec![1, 3],
        aggs: vec![AggSpec::sum(0), AggSpec::sum(2)],
    };
    let Recognition::Eligible(mapping) = recognize(&plan, &schemas) else {
        panic!("expected eligibility")
    };
    assert_eq!(mapping.left_join_key, vec![0]);
    assert_eq!(mapping.right_join_key, vec![0]);
    assert_eq!(mapping.group_key, vec![0, 0]);
    assert_eq!(mapping.aggregate_inputs, vec![Some(4), Some(2)]);
    assert_eq!(
        mapping.aggregate_input_types,
        vec![Some(DataType::Int64), Some(DataType::Int64)]
    );
}

#[test]
fn rejects_invalid_side_projection_and_join_indices_with_typed_reasons() {
    let schemas = schemas();
    let mut invalid_key_plan = plan();
    if let Plan::GroupAggregate { input, .. } = &mut invalid_key_plan
        && let Plan::Join { left, .. } = input.as_mut()
        && let Plan::Project { cols, .. } = left.as_mut()
    {
        *cols = vec![0, 8];
    }
    assert!(matches!(
        recognize(&invalid_key_plan, &schemas),
        Recognition::Dense(DenseReason::ColumnOutOfRange {
            input: InputId(1),
            column: 8
        })
    ));

    let mut plan = plan();
    if let Plan::GroupAggregate { input, .. } = &mut plan
        && let Plan::Join { left_key, .. } = input.as_mut()
    {
        *left_key = vec![8];
    }
    assert!(matches!(
        recognize(&plan, &schemas),
        Recognition::Dense(DenseReason::ColumnOutOfRange {
            input: InputId(1),
            column: 8
        })
    ));
}

#[test]
fn unsupported_shapes_and_float_sum_avg_select_dense() {
    let schemas = schemas();
    let filtered = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Filter {
                input: Box::new(Plan::Source(InputId(1))),
                pred: crate::Predicate::IsNotNull(0),
            }),
            right: Box::new(Plan::Source(InputId(2))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    assert_eq!(
        recognize(&filtered, &schemas),
        Recognition::Dense(DenseReason::UnsupportedInputShape)
    );

    let mut float_schema = schemas.clone();
    float_schema.insert(
        InputId(1),
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Float64, true),
        ])
        .into(),
    );
    let mut float_plan = plan();
    if let Plan::GroupAggregate { aggs, .. } = &mut float_plan {
        aggs[1] = AggSpec::sum(2);
    }
    assert!(matches!(
        recognize(&float_plan, &float_schema),
        Recognition::Dense(DenseReason::UnsupportedAggregateType {
            func: AggFunc::Sum,
            data_type: DataType::Float64
        })
    ));
}

#[test]
fn group_key_not_in_full_equi_join_key_selects_dense() {
    let mut schemas = schemas();
    schemas.insert(
        InputId(1),
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("other", DataType::Int64, true),
            Field::new("v", DataType::Int32, true),
        ])
        .into(),
    );
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(1))),
            right: Box::new(Plan::Source(InputId(2))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![1],
        aggs: vec![AggSpec::count()],
    };
    assert!(matches!(
        recognize(&plan, &schemas),
        Recognition::Dense(DenseReason::GroupKeyNotJoinKey(1))
    ));
}

#[test]
fn rejects_self_join_mismatched_keys_and_malformed_outer_projection() {
    let schemas = schemas();
    let self_join = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(1))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    assert!(matches!(
        recognize(&self_join, &schemas),
        Recognition::Dense(DenseReason::RepeatedSource(InputId(1)))
    ));

    let mut arity = plan();
    if let Plan::GroupAggregate { input, .. } = &mut arity
        && let Plan::Join { right_key, .. } = input.as_mut()
    {
        right_key.clear();
    }
    assert_eq!(
        recognize(&arity, &schemas),
        Recognition::Dense(DenseReason::JoinKeyArityMismatch)
    );

    let mut empty_key = plan();
    if let Plan::GroupAggregate { input, .. } = &mut empty_key
        && let Plan::Join {
            left_key,
            right_key,
            ..
        } = input.as_mut()
    {
        left_key.clear();
        right_key.clear();
    }
    assert_eq!(
        recognize(&empty_key, &schemas),
        Recognition::Dense(DenseReason::EmptyJoinKey)
    );

    let invalid_projection = Plan::GroupAggregate {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Join {
                left: Box::new(Plan::Source(InputId(1))),
                right: Box::new(Plan::Source(InputId(2))),
                left_key: vec![0],
                right_key: vec![0],
            }),
            cols: vec![99],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    assert!(matches!(
        recognize(&invalid_projection, &schemas),
        Recognition::Dense(DenseReason::ColumnOutOfRange { .. })
    ));
}

#[test]
fn mismatched_join_key_types_select_dense() {
    let mut schemas = schemas();
    schemas.insert(
        InputId(1),
        Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::Int32, true),
        ])
        .into(),
    );
    assert!(matches!(
        recognize(&plan(), &schemas),
        Recognition::Dense(DenseReason::UnsupportedJoinType {
            left: DataType::Int32,
            right: DataType::Int64,
        })
    ));
}

#[test]
fn int32_join_keys_are_eligible() {
    let mut schemas = schemas();
    for id in [InputId(1), InputId(2)] {
        let value_type = if id == InputId(1) {
            DataType::Int32
        } else {
            DataType::Int64
        };
        schemas.insert(
            id,
            Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", value_type, false),
            ])
            .into(),
        );
    }
    let Recognition::Eligible(mapping) = recognize(&plan(), &schemas) else {
        panic!("matching Int32 keys should be eligible")
    };
    assert_eq!(mapping.join_key_types, vec![DataType::Int32]);
}

#[test]
fn missing_source_schema_has_typed_dense_reason() {
    let mut schemas = schemas();
    schemas.remove(&InputId(2));
    assert_eq!(
        recognize(&plan(), &schemas),
        Recognition::Dense(DenseReason::MissingSourceSchema(InputId(2)))
    );
}

#[test]
fn multiway_join_selects_dense() {
    let schemas = schemas();
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Join {
                left: Box::new(Plan::Source(InputId(1))),
                right: Box::new(Plan::Source(InputId(2))),
                left_key: vec![0],
                right_key: vec![0],
            }),
            right: Box::new(Plan::Source(InputId(3))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    assert_eq!(
        recognize(&plan, &schemas),
        Recognition::Dense(DenseReason::UnsupportedInputShape)
    );
}

#[test]
fn min_and_max_select_dense_for_first_scope() {
    let schemas = schemas();
    for agg in [AggSpec::min(0), AggSpec::max(0)] {
        let mut plan = plan();
        if let Plan::GroupAggregate { aggs, .. } = &mut plan {
            *aggs = vec![agg];
        }
        assert!(matches!(
            recognize(&plan, &schemas),
            Recognition::Dense(DenseReason::UnsupportedAggregate(func)) if func == agg.func
        ));
    }
}

#[test]
fn nonnullable_integer_keys_and_values_keep_their_resolved_types() {
    let mut schemas = schemas();
    schemas.insert(
        InputId(1),
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int32, false),
        ])
        .into(),
    );
    schemas.insert(
        InputId(2),
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("w", DataType::Int64, false),
        ])
        .into(),
    );
    let Recognition::Eligible(mapping) = recognize(&plan(), &schemas) else {
        panic!("nonnullable integer fields should be eligible")
    };
    assert_eq!(mapping.join_key_types, vec![DataType::Int64]);
    assert_eq!(
        mapping.aggregate_input_types,
        vec![None, Some(DataType::Int32), Some(DataType::Int64)]
    );
}
