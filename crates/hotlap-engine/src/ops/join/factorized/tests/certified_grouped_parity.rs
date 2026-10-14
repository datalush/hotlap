use super::certified_consume_support::*;
use super::*;

#[test]
fn rich_nullable_group_mappings_and_mixed_updates_match_dense_after_every_step() {
    let left_schema = rich_left_schema();
    let right_schema = rich_right_schema();
    let left_initial = rich_left(
        left_schema.clone(),
        &[
            (Some(7), Some(1), Some(10), Some("a")),
            (None, Some(2), None, None),
        ],
        &[1, 1],
    );
    let right_initial = rich_right(
        right_schema.clone(),
        &[
            (Some(7), Some(1), Some(5), Some(true)),
            (None, Some(2), None, None),
        ],
        &[1, 1],
    );
    let steps = vec![
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
        (
            rich_left(
                left_schema.clone(),
                &[
                    (Some(7), Some(1), Some(12), Some("b")),
                    (Some(7), Some(1), Some(14), Some("c")),
                    (Some(7), Some(1), Some(15), Some("d")),
                ],
                &[-1, -1, -1],
            ),
            rich_right(
                right_schema.clone(),
                &[(Some(7), Some(1), Some(8), Some(true))],
                &[-2],
            ),
        ),
        (
            rich_left(
                left_schema.clone(),
                &[(Some(7), Some(1), Some(3), Some("again"))],
                &[1],
            ),
            rich_right(
                right_schema.clone(),
                &[(Some(7), Some(1), Some(2), Some(false))],
                &[1],
            ),
        ),
    ];

    for group_key in [vec![0, 1], vec![0], vec![1, 0, 1]] {
        let (mapping, aggs) = grouped_mapping(group_key.clone());
        let mut candidate_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
        let initial = candidate_join.apply(&left_initial, &right_initial).unwrap();
        let demand = compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap();
        candidate_join.register_demand(demand.clone()).unwrap();
        let mut candidate_group = crate::ops::GroupAggregate::new(&group_key, aggs.clone());
        candidate_group.apply(&initial).unwrap();

        let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
        let dense_initial = dense_join.apply(&left_initial, &right_initial).unwrap();
        dense_join.register_demand(demand).unwrap();
        let mut dense_group = crate::ops::GroupAggregate::new(&group_key, aggs);
        dense_group.apply(&dense_initial).unwrap();
        let output_schema = candidate_join
            .cut_schema(&left_initial, &right_initial)
            .unwrap();

        for (left_delta, right_delta) in &steps {
            let decision = prepare_grouped_update(
                &mut candidate_join,
                &mut candidate_group,
                &mapping,
                left_delta,
                right_delta,
                output_schema.clone(),
            )
            .unwrap();
            let candidate_output = match decision {
                CertifiedDecision::Certified(prepared) => {
                    let output = prepared.output().clone();
                    let dense_joined = dense_join.apply(left_delta, right_delta).unwrap();
                    let dense_output = dense_group.apply(&dense_joined).unwrap();
                    assert_eq!(output.schema(), dense_output.schema());
                    assert_eq!(typed_rows(&output), typed_rows(&dense_output));
                    prepared.commit(&mut candidate_join, &mut candidate_group);
                    output
                }
                needs_rows @ CertifiedDecision::NeedsRows(_) => {
                    let candidate_joined = candidate_join.apply(left_delta, right_delta).unwrap();
                    let output = candidate_group.apply(&candidate_joined).unwrap();
                    let dense_joined = dense_join.apply(left_delta, right_delta).unwrap();
                    let dense_output = dense_group.apply(&dense_joined).unwrap();
                    assert_eq!(output.schema(), dense_output.schema());
                    assert_eq!(typed_rows(&output), typed_rows(&dense_output));
                    needs_rows.commit_dense_metadata(&mut candidate_group);
                    output
                }
            };
            let dense_entries = groups_vec(dense_group.retained_groups());
            let _ = candidate_output;
            assert_eq!(groups_vec(candidate_group.retained_groups()), dense_entries);
            assert_source_arrangements_equal(&candidate_join, &dense_join);
        }
    }
}

#[test]
fn nullable_sum_moves_between_null_zero_empty_and_recreated_groups() {
    let left_schema = rich_left_schema();
    let right_schema = rich_right_schema();
    let left_initial = rich_left(left_schema.clone(), &[(Some(4), Some(1), None, None)], &[1]);
    let right_initial = rich_right(
        right_schema.clone(),
        &[(Some(4), Some(1), None, None)],
        &[1],
    );
    let (mapping, aggs) = grouped_mapping(vec![0]);
    let mut candidate_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let initial = candidate_join.apply(&left_initial, &right_initial).unwrap();
    candidate_join
        .register_demand(compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap())
        .unwrap();
    let output_schema = candidate_join
        .cut_schema(&left_initial, &right_initial)
        .unwrap();
    let mut candidate_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    candidate_group.apply(&initial).unwrap();
    let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let dense_initial = dense_join.apply(&left_initial, &right_initial).unwrap();
    dense_join
        .register_demand(compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap())
        .unwrap();
    let mut dense_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    dense_group.apply(&dense_initial).unwrap();

    let updates = [
        (
            ZSetBatch::empty(left_schema.clone()),
            rich_right(
                right_schema.clone(),
                &[
                    (Some(4), Some(1), None, None),
                    (Some(4), Some(1), Some(0), Some(false)),
                ],
                &[-1, 1],
            ),
        ),
        (
            ZSetBatch::empty(left_schema.clone()),
            rich_right(
                right_schema.clone(),
                &[
                    (Some(4), Some(1), Some(0), Some(false)),
                    (Some(4), Some(1), None, None),
                ],
                &[-1, 1],
            ),
        ),
        (
            rich_left(
                left_schema.clone(),
                &[(Some(4), Some(1), None, None)],
                &[-1],
            ),
            rich_right(
                right_schema.clone(),
                &[(Some(4), Some(1), None, None)],
                &[-1],
            ),
        ),
        (
            rich_left(
                left_schema.clone(),
                &[(Some(4), Some(1), Some(2), Some("x"))],
                &[1],
            ),
            rich_right(
                right_schema.clone(),
                &[(Some(4), Some(1), Some(3), Some(true))],
                &[1],
            ),
        ),
    ];
    for (step, (left_delta, right_delta)) in updates.iter().enumerate() {
        let decision = prepare_grouped_update(
            &mut candidate_join,
            &mut candidate_group,
            &mapping,
            left_delta,
            right_delta,
            output_schema.clone(),
        )
        .unwrap();
        let output = match decision {
            CertifiedDecision::Certified(prepared) => {
                let output = prepared.output().clone();
                let dense_rows = dense_join.apply(left_delta, right_delta).unwrap();
                let dense_output = dense_group.apply(&dense_rows).unwrap();
                assert_eq!(typed_rows(&output), typed_rows(&dense_output));
                prepared.commit(&mut candidate_join, &mut candidate_group);
                output
            }
            needs_rows @ CertifiedDecision::NeedsRows(_) => {
                let rows = candidate_join.apply(left_delta, right_delta).unwrap();
                let output = candidate_group.apply(&rows).unwrap();
                let dense_rows = dense_join.apply(left_delta, right_delta).unwrap();
                let dense_output = dense_group.apply(&dense_rows).unwrap();
                assert_eq!(typed_rows(&output), typed_rows(&dense_output));
                needs_rows.commit_dense_metadata(&mut candidate_group);
                output
            }
        };
        let dense_entries = groups_vec(dense_group.retained_groups());
        assert_eq!(groups_vec(candidate_group.retained_groups()), dense_entries);
        assert_source_arrangements_equal(&candidate_join, &dense_join);
        match step {
            0 => assert!(
                typed_rows(&output)
                    .iter()
                    .any(|(cells, _)| cells.contains(&"0".into()))
            ),
            1 => assert!(
                typed_rows(&output)
                    .iter()
                    .any(|(cells, _)| cells.contains(&"NULL".into()))
            ),
            2 => assert!(candidate_group.retained_groups().is_empty()),
            3 => assert_eq!(candidate_group.retained_groups().len(), 1),
            _ => unreachable!(),
        }
    }
}

#[test]
fn key_only_sources_preserve_duplicate_identity_weights_and_count_star_deletion() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let keyed = |rows: &[(i64, i64)], diffs: &[i64]| {
        zset(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
            diffs,
        )
    };
    let left_initial = ZSetBatch::empty(schema.clone());
    let right_initial = keyed(&[(9, 4)], &[1]);
    let mapping = ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![0],
        aggregate_inputs: vec![None],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![None],
    };
    let aggs = vec![AggSpec::count()];
    let mut candidate_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let initial = candidate_join.apply(&left_initial, &right_initial).unwrap();
    candidate_join
        .register_demand(compile_demand(&mapping, &aggs, &schema, &schema).unwrap())
        .unwrap();
    let output_schema = candidate_join
        .cut_schema(&left_initial, &right_initial)
        .unwrap();
    let mut candidate_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    candidate_group.apply(&initial).unwrap();
    let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let dense_initial = dense_join.apply(&left_initial, &right_initial).unwrap();
    dense_join
        .register_demand(compile_demand(&mapping, &aggs, &schema, &schema).unwrap())
        .unwrap();
    let mut dense_group = crate::ops::GroupAggregate::new(&[0], aggs);
    dense_group.apply(&dense_initial).unwrap();

    for diffs in [&[1, 1][..], &[-1, -1][..]] {
        let left_delta = keyed(&[(9, 4), (9, 4)], diffs);
        let right_delta = ZSetBatch::empty(schema.clone());
        let decision = prepare_grouped_update(
            &mut candidate_join,
            &mut candidate_group,
            &mapping,
            &left_delta,
            &right_delta,
            output_schema.clone(),
        )
        .unwrap();
        let (candidate_output, dense_output) = match decision {
            CertifiedDecision::Certified(prepared) => {
                let output = prepared.output().clone();
                let dense_rows = dense_join.apply(&left_delta, &right_delta).unwrap();
                let dense_output = dense_group.apply(&dense_rows).unwrap();
                prepared.commit(&mut candidate_join, &mut candidate_group);
                (output, dense_output)
            }
            needs_rows @ CertifiedDecision::NeedsRows(_) => {
                let rows = candidate_join.apply(&left_delta, &right_delta).unwrap();
                let output = candidate_group.apply(&rows).unwrap();
                let dense_rows = dense_join.apply(&left_delta, &right_delta).unwrap();
                let dense_output = dense_group.apply(&dense_rows).unwrap();
                needs_rows.commit_dense_metadata(&mut candidate_group);
                (output, dense_output)
            }
        };
        assert_eq!(typed_rows(&candidate_output), typed_rows(&dense_output));
        assert_eq!(
            groups_vec(candidate_group.retained_groups()),
            groups_vec(dense_group.retained_groups())
        );
        assert_source_arrangements_equal(&candidate_join, &dense_join);
    }
    assert!(candidate_group.retained_groups().is_empty());
    assert_eq!(candidate_group.maintained_memo().unwrap().len(), 1);
}
