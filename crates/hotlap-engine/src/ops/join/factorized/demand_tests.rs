use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use hotlap_core::InputId;
use hotlap_core::plan::{AggSpec, Plan};
use hotlap_core::plan_normalize::{Recognition, recognize};

use super::demand::{Demand, compile_demand};

fn schemas() -> std::collections::HashMap<InputId, arrow::datatypes::SchemaRef> {
    [
        (
            InputId(1),
            Arc::new(Schema::new(vec![
                Field::new("lk", DataType::Int64, false),
                Field::new("value", DataType::Int64, true),
            ])),
        ),
        (
            InputId(2),
            Arc::new(Schema::new(vec![
                Field::new("rk", DataType::Int64, false),
                Field::new("other", DataType::Int32, true),
            ])),
        ),
    ]
    .into_iter()
    .collect()
}

fn plan() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Join {
                left: Box::new(Plan::Source(InputId(1))),
                right: Box::new(Plan::Source(InputId(2))),
                left_key: vec![0],
                right_key: vec![0],
            }),
            cols: vec![3, 0, 2, 1],
        }),
        key: vec![1, 2],
        aggs: vec![AggSpec::count(), AggSpec::count(), AggSpec::sum(3)],
    }
}

fn multikey_schemas() -> std::collections::HashMap<InputId, arrow::datatypes::SchemaRef> {
    [
        (
            InputId(1),
            Arc::new(Schema::new(vec![
                Field::new("l0", DataType::Int64, false),
                Field::new("l1", DataType::Int64, false),
                Field::new("l2", DataType::Int64, false),
                Field::new("l_i64", DataType::Int64, true),
                Field::new("l_i32", DataType::Int32, true),
            ])),
        ),
        (
            InputId(2),
            Arc::new(Schema::new(vec![
                Field::new("r0", DataType::Int64, false),
                Field::new("r1", DataType::Int64, false),
                Field::new("r2", DataType::Int64, false),
                Field::new("r_null", DataType::Null, true),
                Field::new("r_i32", DataType::Int32, true),
            ])),
        ),
    ]
    .into_iter()
    .collect()
}

fn multikey_plan() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Join {
                left: Box::new(Plan::Source(InputId(1))),
                right: Box::new(Plan::Source(InputId(2))),
                left_key: vec![0, 1, 2],
                right_key: vec![0, 1, 2],
            }),
            // Reordered join keys, both payload sides and a Null-typed value.
            cols: vec![7, 0, 2, 3, 4, 8, 1, 9],
        }),
        // Full-key position 2, then 0, then 2 again: subset + reorder + repeat.
        key: vec![0, 1, 0],
        aggs: vec![
            AggSpec::count(),
            AggSpec::sum(4),
            AggSpec {
                func: hotlap_core::plan::AggFunc::Count,
                input: Some(3),
            },
            AggSpec::sum(3),
            AggSpec {
                func: hotlap_core::plan::AggFunc::Count,
                input: Some(5),
            },
            AggSpec {
                func: hotlap_core::plan::AggFunc::Count,
                input: Some(7),
            },
            AggSpec::sum(7),
        ],
    }
}

#[test]
fn compiles_stable_deduplicated_nullable_integer_demands_and_group_projection() {
    let schemas = schemas();
    let Recognition::Eligible(mapping) = recognize(&plan(), &schemas) else {
        panic!("expected normalized join aggregate");
    };
    assert_eq!(mapping.group_key, vec![0, 0]);
    let aggs = [AggSpec::count(), AggSpec::count(), AggSpec::sum(3)];
    let demand = compile_demand(
        &mapping,
        &aggs,
        &schemas[&InputId(1)],
        &schemas[&InputId(2)],
    )
    .expect("resolved integer demand");
    assert_eq!(
        demand,
        Demand {
            left_join_key: vec![0],
            right_join_key: vec![0],
            group_key: vec![0, 0],
            left_columns: vec![1],
            right_columns: vec![],
            left_column_types: vec![DataType::Int64],
            right_column_types: vec![],
            left_column_sums: vec![true],
            right_column_sums: vec![],
        }
    );
    assert_eq!(
        demand,
        compile_demand(
            &mapping,
            &aggs,
            &schemas[&InputId(1)],
            &schemas[&InputId(2)],
        )
        .unwrap()
    );
}

#[test]
fn unsupported_aggregate_shapes_remain_dense_and_are_not_demand_errors() {
    let schemas = schemas();
    let mut unsupported = plan();
    let Plan::GroupAggregate { aggs, .. } = &mut unsupported else {
        unreachable!();
    };
    aggs.push(AggSpec::avg(3));
    let Recognition::Eligible(mapping) = recognize(&unsupported, &schemas) else {
        panic!("AVG is recognized by the existing normalizer");
    };
    assert_eq!(
        compile_demand(
            &mapping,
            &[
                AggSpec::count(),
                AggSpec::count(),
                AggSpec::sum(3),
                AggSpec::avg(3)
            ],
            &schemas[&InputId(1)],
            &schemas[&InputId(2)]
        ),
        Ok(super::demand::Demand {
            left_join_key: vec![0],
            right_join_key: vec![0],
            group_key: vec![0, 0],
            left_columns: vec![1],
            right_columns: vec![],
            left_column_types: vec![DataType::Int64],
            right_column_types: vec![],
            left_column_sums: vec![true],
            right_column_sums: vec![],
        })
    );
}

#[test]
fn nullable_count_and_sum_share_one_selected_column_and_schema_mismatch_is_rejected() {
    let schemas = schemas();
    let mut plan = plan();
    let Plan::GroupAggregate { aggs, .. } = &mut plan else {
        unreachable!();
    };
    aggs[0] = AggSpec {
        func: hotlap_core::plan::AggFunc::Count,
        input: Some(3),
    };
    let aggs = aggs.clone();
    let Recognition::Eligible(mapping) = recognize(&plan, &schemas) else {
        panic!("expected normalized join aggregate");
    };
    let demand = compile_demand(
        &mapping,
        &aggs,
        &schemas[&InputId(1)],
        &schemas[&InputId(2)],
    )
    .unwrap();
    assert_eq!(demand.left_columns, vec![1]);
    assert_eq!(demand.left_column_sums, vec![true]);

    let wrong_schema = Arc::new(Schema::new(vec![
        Field::new("lk", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
    ]));
    assert!(matches!(
        compile_demand(&mapping, &aggs, &wrong_schema, &schemas[&InputId(2)]),
        Err(hotlap_core::plan_normalize::DenseReason::UnsupportedAggregateType { .. })
    ));
}

#[test]
fn sorts_selected_source_columns_and_checks_schema_bounds() {
    let schemas = schemas();
    let mut plan = plan();
    let Plan::GroupAggregate { aggs, .. } = &mut plan else {
        unreachable!();
    };
    *aggs = vec![AggSpec::sum(0), AggSpec::sum(3), AggSpec::sum(0)];
    let aggs = aggs.clone();
    let Recognition::Eligible(mapping) = recognize(&plan, &schemas) else {
        panic!("expected normalized join aggregate");
    };
    let demand = compile_demand(
        &mapping,
        &aggs,
        &schemas[&InputId(1)],
        &schemas[&InputId(2)],
    )
    .unwrap();
    assert_eq!(demand.left_columns, vec![1]);
    assert_eq!(demand.right_columns, vec![1]);
    assert_eq!(demand.left_column_types, vec![DataType::Int64]);
    assert_eq!(demand.right_column_types, vec![DataType::Int32]);
    assert_eq!(demand.left_column_sums, vec![true]);
    assert_eq!(demand.right_column_sums, vec![true]);

    let too_narrow = Arc::new(Schema::new(vec![Field::new("lk", DataType::Int64, false)]));
    assert!(matches!(
        compile_demand(&mapping, &aggs, &too_narrow, &schemas[&InputId(2)]),
        Err(hotlap_core::plan_normalize::DenseReason::AggregateInputOutOfRange(_))
    ));
}

#[test]
fn join_demand_registration_is_idempotent_and_immutable() {
    let mut join = crate::ops::join::Join::new(&[0], &[0]);
    let first = Demand {
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        left_columns: vec![1],
        right_columns: vec![],
        left_column_types: vec![DataType::Int64],
        right_column_types: vec![],
        left_column_sums: vec![true],
        right_column_sums: vec![],
    };
    join.register_demand(first.clone()).unwrap();
    join.register_demand(first.clone()).unwrap();
    let changed = Demand {
        left_columns: vec![],
        ..first
    };
    assert!(matches!(
        join.register_demand(changed),
        Err(crate::error::EngineError::Unsupported(_))
    ));
}

#[test]
fn join_memory_usage_accounts_owned_typed_demand_buffers() {
    let mut join = crate::ops::join::Join::new(&[0], &[0]);
    let before = join.memory_usage().unwrap();
    join.register_demand(Demand {
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0, 0],
        left_columns: vec![1, 2],
        right_columns: vec![1],
        left_column_types: vec![DataType::Utf8, DataType::Int64],
        right_column_types: vec![DataType::Int32],
        left_column_sums: vec![false, true],
        right_column_sums: vec![true],
    })
    .unwrap();
    let after = join.memory_usage().unwrap();
    assert!(
        after.0 + after.1 + after.2 > before.0 + before.1 + before.2,
        "typed demand must be included in retained join memory"
    );
}

#[test]
fn multikey_projected_grouping_and_unsorted_repeated_demands_resolve_source_columns() {
    let schemas = multikey_schemas();
    let plan = multikey_plan();
    let Plan::GroupAggregate { aggs, .. } = &plan else {
        unreachable!();
    };
    let Recognition::Eligible(mapping) = recognize(&plan, &schemas) else {
        panic!("expected typed multi-key normalized aggregate");
    };
    assert_eq!(mapping.group_key, vec![2, 0, 2]);
    assert_eq!(
        mapping.aggregate_inputs,
        vec![None, Some(4), Some(3), Some(3), Some(8), Some(9), Some(9)]
    );
    let demand =
        compile_demand(&mapping, aggs, &schemas[&InputId(1)], &schemas[&InputId(2)]).unwrap();
    assert_eq!(demand.group_key, vec![2, 0, 2]);
    assert_eq!(demand.left_columns, vec![3, 4]);
    assert_eq!(
        demand.left_column_types,
        vec![DataType::Int64, DataType::Int32]
    );
    assert_eq!(demand.right_columns, vec![3, 4]);
    assert_eq!(
        demand.right_column_types,
        vec![DataType::Null, DataType::Int32]
    );
    assert_eq!(demand.left_column_sums, vec![true, true]);
    assert_eq!(demand.right_column_sums, vec![false, true]);
}

#[test]
fn count_null_type_is_demanded_but_sum_null_type_stays_dense() {
    let schemas = multikey_schemas();
    let mut count_plan = multikey_plan();
    let Plan::GroupAggregate { aggs, .. } = &mut count_plan else {
        unreachable!();
    };
    *aggs = vec![AggSpec {
        func: hotlap_core::plan::AggFunc::Count,
        input: Some(5),
    }];
    let count_aggs = aggs.clone();
    let Recognition::Eligible(mapping) = recognize(&count_plan, &schemas) else {
        panic!("COUNT over Null remains a supported non-null demand");
    };
    let demand = compile_demand(
        &mapping,
        &count_aggs,
        &schemas[&InputId(1)],
        &schemas[&InputId(2)],
    )
    .unwrap();
    assert_eq!(demand.right_columns, vec![3]);
    assert_eq!(demand.right_column_types, vec![DataType::Null]);
    assert_eq!(demand.right_column_sums, vec![false]);

    let mut sum_plan = count_plan.clone();
    let Plan::GroupAggregate { aggs, .. } = &mut sum_plan else {
        unreachable!();
    };
    aggs[0] = AggSpec::sum(5);
    assert!(matches!(
        recognize(&sum_plan, &schemas),
        Recognition::Dense(
            hotlap_core::plan_normalize::DenseReason::UnsupportedAggregateType {
                func: hotlap_core::plan::AggFunc::Sum,
                data_type: DataType::Null,
            }
        )
    ));
}

#[test]
fn changed_side_schema_or_demand_cannot_replace_registered_mapping() {
    let original_schemas = multikey_schemas();
    let plan = multikey_plan();
    let Plan::GroupAggregate { aggs, .. } = &plan else {
        unreachable!();
    };
    let Recognition::Eligible(mapping) = recognize(&plan, &original_schemas) else {
        panic!("expected typed multi-key normalized aggregate");
    };
    let original = compile_demand(
        &mapping,
        aggs,
        &original_schemas[&InputId(1)],
        &original_schemas[&InputId(2)],
    )
    .unwrap();

    let mut join = crate::ops::join::Join::new(&[0, 1, 2], &[0, 1, 2]);
    join.register_demand(original.clone()).unwrap();
    join.register_demand(original.clone()).unwrap();

    let changed_columns = Demand {
        left_columns: vec![4, 3],
        left_column_types: vec![DataType::Int32, DataType::Int64],
        ..original.clone()
    };
    let changed_left_type = changed_schema(&original_schemas, InputId(1), 3, DataType::Int32);
    let changed_right_type = changed_schema(&original_schemas, InputId(2), 4, DataType::Int64);
    let changed_left = compile_for_schemas(&plan, &changed_left_type).unwrap();
    let changed_right = compile_for_schemas(&plan, &changed_right_type).unwrap();
    let changed_right_mapping = Demand {
        right_columns: vec![4, 3],
        right_column_types: vec![DataType::Int32, DataType::Null],
        ..original.clone()
    };

    for changed in [
        changed_columns,
        changed_left,
        changed_right,
        changed_right_mapping,
    ] {
        assert!(matches!(
            join.register_demand(changed),
            Err(crate::error::EngineError::Unsupported(_))
        ));
        // Re-registering the original proves each rejection left the slot intact.
        join.register_demand(original.clone()).unwrap();
    }
}

fn changed_schema(
    schemas: &std::collections::HashMap<InputId, arrow::datatypes::SchemaRef>,
    input: InputId,
    column: usize,
    data_type: DataType,
) -> std::collections::HashMap<InputId, arrow::datatypes::SchemaRef> {
    let mut changed = schemas.clone();
    let fields = schemas[&input]
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if index == column {
                Field::new(field.name(), data_type.clone(), field.is_nullable())
            } else {
                field.as_ref().clone()
            }
        })
        .collect::<Vec<_>>();
    changed.insert(input, Arc::new(Schema::new(fields)));
    changed
}

fn compile_for_schemas(
    plan: &Plan,
    schemas: &std::collections::HashMap<InputId, arrow::datatypes::SchemaRef>,
) -> Result<Demand, hotlap_core::plan_normalize::DenseReason> {
    let Plan::GroupAggregate { aggs, .. } = plan else {
        unreachable!();
    };
    let Recognition::Eligible(mapping) = recognize(plan, schemas) else {
        unreachable!("integer type changes should remain normalizable");
    };
    compile_demand(&mapping, aggs, &schemas[&InputId(1)], &schemas[&InputId(2)])
}
