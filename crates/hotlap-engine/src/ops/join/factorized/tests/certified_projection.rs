use super::certified_consume_support::*;
use super::*;

#[test]
fn normalized_direct_projection_uses_original_source_columns_and_real_projector() {
    let left_schema = rich_left_schema();
    let right_schema = rich_right_schema();
    let left_projection = vec![1, 0, 2, 3, 2];
    let right_projection = vec![1, 0, 2, 3];
    let projection = vec![6, 0, 7, 3, 2, 8, 7];
    let source_schemas = [
        (InputId(0), left_schema.clone()),
        (InputId(1), right_schema.clone()),
    ]
    .into_iter()
    .collect();
    let aggs = vec![
        AggSpec::sum(2),
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(3),
        },
        AggSpec::sum(4),
        AggSpec::count(),
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(5),
        },
    ];
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Join {
                left: Box::new(Plan::Project {
                    input: Box::new(Plan::Source(InputId(0))),
                    cols: left_projection.clone(),
                }),
                right: Box::new(Plan::Project {
                    input: Box::new(Plan::Source(InputId(1))),
                    cols: right_projection.clone(),
                }),
                left_key: vec![0, 1],
                right_key: vec![0, 1],
            }),
            cols: projection.clone(),
        }),
        key: vec![0, 1, 0],
        aggs: aggs.clone(),
    };
    let Recognition::Eligible(mapping) = recognize(&plan, &source_schemas) else {
        panic!("direct join projection should normalize to the certified cut");
    };
    assert_eq!(mapping.group_key, vec![1, 0, 1]);
    assert_eq!(
        mapping.aggregate_inputs,
        vec![Some(7), Some(3), Some(2), None, Some(8)]
    );
    let reducer_aggs = vec![
        AggSpec::sum(2),
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(3),
        },
        AggSpec::sum(4),
        AggSpec::count(),
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(5),
        },
    ];
    let left_source_initial = rich_left(
        left_schema.clone(),
        &[
            (Some(7), Some(1), Some(10), Some("a")),
            (None, Some(2), None, None),
        ],
        &[1, 1],
    );
    let right_source_initial = rich_right(
        right_schema.clone(),
        &[
            (Some(7), Some(1), Some(5), Some(true)),
            (None, Some(2), None, None),
        ],
        &[1, 1],
    );
    let updates = [
        (
            rich_left(
                left_schema.clone(),
                &[(Some(7), Some(1), Some(12), Some("b"))],
                &[1],
            ),
            ZSetBatch::empty(right_schema.clone()),
        ),
        (
            rich_left(
                left_schema.clone(),
                &[
                    (Some(7), Some(1), Some(10), Some("a")),
                    (Some(7), Some(1), Some(14), Some("c")),
                ],
                &[-1, 1],
            ),
            rich_right(
                right_schema.clone(),
                &[
                    (Some(7), Some(1), Some(5), Some(true)),
                    (Some(7), Some(1), Some(6), Some(false)),
                ],
                &[-1, 1],
            ),
        ),
        (
            rich_left(
                left_schema.clone(),
                &[(Some(7), Some(1), Some(15), Some("d"))],
                &[1],
            ),
            rich_right(
                right_schema.clone(),
                &[
                    (Some(7), Some(1), Some(6), Some(false)),
                    (Some(7), Some(1), Some(8), Some(true)),
                ],
                &[-1, 2],
            ),
        ),
    ];

    let left_initial = crate::ops::project(&left_source_initial, &left_projection).unwrap();
    let right_initial = crate::ops::project(&right_source_initial, &right_projection).unwrap();
    let mut candidate_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let full_initial = candidate_join.apply(&left_initial, &right_initial).unwrap();
    candidate_join
        .register_demand(
            compile_demand(
                &mapping,
                &aggs,
                &left_initial.schema(),
                &right_initial.schema(),
            )
            .unwrap(),
        )
        .unwrap();
    let projected_initial = crate::ops::project(&full_initial, &projection).unwrap();
    let mut candidate_group = crate::ops::GroupAggregate::new(&[0, 1, 0], reducer_aggs.clone());
    candidate_group.apply(&projected_initial).unwrap();
    let output_schema = crate::ops::project(&ZSetBatch::empty(full_initial.schema()), &projection)
        .unwrap()
        .schema();

    let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let dense_full = dense_join.apply(&left_initial, &right_initial).unwrap();
    dense_join
        .register_demand(
            compile_demand(
                &mapping,
                &aggs,
                &left_initial.schema(),
                &right_initial.schema(),
            )
            .unwrap(),
        )
        .unwrap();
    let mut dense_group = crate::ops::GroupAggregate::new(&[0, 1, 0], reducer_aggs);
    dense_group
        .apply(&crate::ops::project(&dense_full, &projection).unwrap())
        .unwrap();

    let mut certified_updates = 0;
    for (left_delta, right_delta) in updates {
        let left_delta = crate::ops::project(&left_delta, &left_projection).unwrap();
        let right_delta = crate::ops::project(&right_delta, &right_projection).unwrap();
        let decision =
            crate::ops::join::factorized::certified_consume::prepare_grouped_update_projected(
                &mut candidate_join,
                &mut candidate_group,
                &mapping,
                &left_delta,
                &right_delta,
                output_schema.clone(),
                &projection,
            )
            .unwrap();
        match decision {
            CertifiedDecision::Certified(prepared) => {
                certified_updates += 1;
                let output = prepared.output().clone();
                let dense_joined = dense_join.apply(&left_delta, &right_delta).unwrap();
                let dense_output = dense_group
                    .apply(&crate::ops::project(&dense_joined, &projection).unwrap())
                    .unwrap();
                assert_eq!(output.schema(), dense_output.schema());
                assert_eq!(typed_rows(&output), typed_rows(&dense_output));
                prepared.commit(&mut candidate_join, &mut candidate_group);
            }
            needs_rows @ CertifiedDecision::NeedsRows(_) => {
                let candidate_joined = candidate_join.apply(&left_delta, &right_delta).unwrap();
                let output = candidate_group
                    .apply(&crate::ops::project(&candidate_joined, &projection).unwrap())
                    .unwrap();
                let dense_joined = dense_join.apply(&left_delta, &right_delta).unwrap();
                let dense_output = dense_group
                    .apply(&crate::ops::project(&dense_joined, &projection).unwrap())
                    .unwrap();
                assert_eq!(output.schema(), dense_output.schema());
                assert_eq!(typed_rows(&output), typed_rows(&dense_output));
                needs_rows.commit_dense_metadata(&mut candidate_group);
            }
        }
        assert_eq!(
            groups_vec(candidate_group.retained_groups()),
            groups_vec(dense_group.retained_groups())
        );
        assert_source_arrangements_equal(&candidate_join, &dense_join);
    }
    assert!(certified_updates > 0);
}
