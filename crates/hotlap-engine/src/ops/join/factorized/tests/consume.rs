use super::*;

#[test]
fn staged_group_changes_have_a_production_consumer() {
    let empty = empty_groups();
    let output_schema = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let left = arrangement(&simple(&[], &[]), &[0]);
    let right = arrangement(&simple(&[], &[]), &[0]);
    let inputs = DeltaPair::new(
        1,
        Arc::new(simple(&[10, 20], &[1, 1])),
        Arc::new(simple(&[10, 20], &[1, 1])),
    );
    let aggs = [
        AggSpec::count(),
        AggSpec::count(),
        AggSpec::sum(1),
        AggSpec::avg(1),
    ];
    let recognized = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![None, None, Some(1), Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![None, None, Some(DataType::Int64), Some(DataType::Int64)],
    });
    let CutDecision::Eligible(prepared) =
        run_with_aggs(inputs, &left, &right, &empty, recognized, &aggs)
    else {
        panic!("fixture should prepare");
    };
    let consumed =
        crate::ops::join::factorized::consume::consume(&prepared, output_schema, &[0], &aggs)
            .unwrap();
    drop(prepared); // Output and snapshot own their data after input Arcs are released.
    assert_eq!(consumed.changes.len(), 1);
    assert_eq!(consumed.changes[0].1.as_ref().unwrap().rows, 4);
    // Joining left payloads 10 and 20 with two right rows yields these four
    // rows. The join/group key is 7 (simple() fixes it); 10/20 are aggregate
    // payload values, not output group keys.
    let dense_batch = simple(&[10, 10, 20, 20], &[1, 1, 1, 1]);
    let mut dense = crate::ops::GroupAggregate::new(&[0], aggs.to_vec());
    let expected = dense.apply(&dense_batch).unwrap();
    let mut actual_rows = typed_rows(&consumed.output);
    let mut expected_rows = typed_rows(&expected);
    actual_rows.sort();
    expected_rows.sort();
    assert_eq!(consumed.output.schema(), expected.schema());
    assert_eq!(actual_rows, expected_rows);
}

#[test]
fn nullable_updates_match_dense_after_each_insert_retract_remove_and_recreate() {
    use hotlap_core::snapshot::GroupState;

    let nullable = |values: &[Option<i64>], diffs: &[i64]| {
        let s = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        zset(
            s,
            vec![
                Arc::new(Int64Array::from(vec![Some(7); values.len()])),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let aggs = [
        AggSpec::count(),
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(1),
        },
        AggSpec::sum(1),
        AggSpec::avg(1),
    ];
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![None, Some(1), Some(1), Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![
            None,
            Some(DataType::Int64),
            Some(DataType::Int64),
            Some(DataType::Int64),
        ],
    });
    let mut left = arrangement(&nullable(&[Some(2), None], &[1, 1]), &[0]);
    let right_seed = nullable(&[Some(100), Some(200)], &[1, 1]);
    let right = arrangement(&right_seed, &[0]);
    let mut dense = crate::ops::GroupAggregate::new(&[0], aggs.to_vec());
    dense
        .apply(&nullable(&[Some(2), Some(2), None, None], &[1, 1, 1, 1]))
        .unwrap();

    let transitions: [(&[Option<i64>], &[i64]); 5] = [
        (&[Some(4)], &[1]),
        (&[None], &[-1]),
        (&[Some(2)], &[-1]),
        (&[Some(4)], &[-1]),
        (&[Some(9)], &[1]),
    ];
    let mut prior: GroupState = dense.export_state().unwrap();
    let mut current_groups = group_map(&prior.groups);
    for (epoch, (values, weights)) in transitions.into_iter().enumerate() {
        let delta_left = nullable(values, weights);
        let mut joined_values = Vec::new();
        let mut joined_diffs = Vec::new();
        for (&value, &weight) in values.iter().zip(weights) {
            for _ in 0..2 {
                joined_values.push(value);
                joined_diffs.push(weight);
            }
        }
        let dense_delta = nullable(&joined_values, &joined_diffs);
        let expected = dense.apply(&dense_delta).unwrap();
        let decision = prepare(PrepareInput {
            recognition: &mapping,
            aggregates: &aggs,
            inputs: DeltaPair::new(
                epoch as u64 + 1,
                Arc::new(delta_left.clone()),
                Arc::new(nullable(&[], &[])),
            ),
            state: OldState {
                left: &left,
                right: &right,
                groups: &current_groups,
            },
            consumer: ConsumerCaps::FactorizedAggregates,
        });
        let prepared = match decision {
            CutDecision::Eligible(prepared) => prepared,
            CutDecision::Dense(reason) => panic!("transition {epoch} rejected: {reason:?}"),
        };
        let consumed = crate::ops::join::factorized::consume::consume(
            &prepared,
            nullable(&[], &[]).schema(),
            &[0],
            &aggs,
        )
        .unwrap();
        let mut actual_rows = typed_rows(&consumed.output);
        let mut expected_rows = typed_rows(&expected);
        actual_rows.sort();
        expected_rows.sort();
        assert_eq!(consumed.output.schema(), expected.schema(), "epoch {epoch}");
        assert_eq!(actual_rows, expected_rows, "epoch {epoch}");
        assert_eq!(
            consumed.changes.len(),
            prepared.groups.len(),
            "epoch {epoch}"
        );

        let keys = KeyConverter::new(delta_left.schema().as_ref(), &[0]).unwrap();
        left.apply(&delta_left, &keys).unwrap();
        for (key, value) in consumed.changes {
            if let Some(value) = value {
                current_groups.insert(key, value);
            } else {
                current_groups.remove(&key);
            }
        }
        prior = GroupState {
            schema: None,
            groups: groups_vec(&current_groups),
        };
    }

    let final_state = dense.export_state().unwrap();
    assert_eq!(prior.groups, final_state.groups);
    assert_eq!(prior.groups.len(), 1, "recreated group remains live");
    assert_eq!(prior.groups[0].1.rows, 2);
    let count_expr = match &prior.groups[0].1.values[1] {
        AggValue::Count(count) => *count,
        other => panic!("unexpected COUNT(expr) state: {other:?}"),
    };
    assert_eq!(count_expr, 2);
}

#[test]
fn simultaneous_side_deltas_match_dense_including_cross_term() {
    let old_left = simple(&[2], &[1]);
    let old_right = simple(&[100], &[1]);
    let left = arrangement(&old_left, &[0]);
    let right = arrangement(&old_right, &[0]);
    let aggs = [AggSpec::count(), AggSpec::sum(1), AggSpec::avg(1)];
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![None, Some(1), Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![None, Some(DataType::Int64), Some(DataType::Int64)],
    });
    let mut dense = crate::ops::GroupAggregate::new(&[0], aggs.to_vec());
    dense.apply(&old_left).unwrap();
    let prior = dense.export_state().unwrap();
    let delta_left = simple(&[4], &[1]);
    let delta_right = simple(&[200], &[1]);
    // ΔL×Rold + Lold×ΔR + ΔL×ΔR. Aggregates read the left payload.
    let dense_delta = simple(&[4, 2, 4], &[1, 1, 1]);
    let expected = dense.apply(&dense_delta).unwrap();
    let prepared = match prepare(PrepareInput {
        recognition: &mapping,
        aggregates: &aggs,
        inputs: DeltaPair::new(2, Arc::new(delta_left), Arc::new(delta_right)),
        state: OldState {
            left: &left,
            right: &right,
            groups: &group_map(&prior.groups),
        },
        consumer: ConsumerCaps::FactorizedAggregates,
    }) {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("both-sided delta rejected: {reason:?}"),
    };
    let consumed = crate::ops::join::factorized::consume::consume(
        &prepared,
        simple(&[], &[]).schema(),
        &[0],
        &aggs,
    )
    .unwrap();
    let mut actual_rows = typed_rows(&consumed.output);
    let mut expected_rows = typed_rows(&expected);
    actual_rows.sort();
    expected_rows.sort();
    assert_eq!(consumed.output.schema(), expected.schema());
    assert_eq!(actual_rows, expected_rows);
}

#[test]
fn projected_group_subset_keeps_untouched_group_in_snapshot_and_output_keys() {
    let s = schema(&[
        ("join", DataType::Int64),
        ("group", DataType::Int64),
        ("value", DataType::Int64),
    ]);
    let batch = |rows: &[(i64, i64, i64)], diffs: &[i64]| {
        zset(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
            diffs,
        )
    };
    let old = batch(&[(1, 7, 10), (3, 7, 11), (2, 8, 30)], &[1, 1, 1]);
    let left = arrangement(&old, &[0, 1]);
    let right = arrangement(
        &batch(&[(1, 7, 100), (3, 7, 300), (2, 8, 200)], &[1, 1, 1]),
        &[0, 1],
    );
    let aggs = [AggSpec::count(), AggSpec::sum(2), AggSpec::avg(2)];
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![1],
        aggregate_inputs: vec![None, Some(2), Some(2)],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![None, Some(DataType::Int64), Some(DataType::Int64)],
    });
    let mut dense = crate::ops::GroupAggregate::new(&[1], aggs.to_vec());
    dense.apply(&old).unwrap();
    let prior = dense.export_state().unwrap();
    let delta_left = batch(&[(1, 7, 20)], &[1]);
    let expected = dense.apply(&delta_left).unwrap();
    let prepared = match prepare(PrepareInput {
        recognition: &mapping,
        aggregates: &aggs,
        inputs: DeltaPair::new(2, Arc::new(delta_left), Arc::new(batch(&[], &[]))),
        state: OldState {
            left: &left,
            right: &right,
            groups: &group_map(&prior.groups),
        },
        consumer: ConsumerCaps::FactorizedAggregates,
    }) {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("subset grouping rejected: {reason:?}"),
    };
    let consumed =
        crate::ops::join::factorized::consume::consume(&prepared, s, &[1], &aggs).unwrap();
    let mut actual_rows = typed_rows(&consumed.output);
    let mut expected_rows = typed_rows(&expected);
    actual_rows.sort();
    expected_rows.sort();
    assert_eq!(consumed.output.schema(), expected.schema());
    assert_eq!(actual_rows, expected_rows);
    assert_eq!(
        prepared.prior_groups.len(),
        1,
        "only touched group is retained as prior"
    );
    assert_eq!(prepared.prior_groups[0].0, super::simple_key(7));
    let mut candidate = group_map(&prior.groups);
    for (key, value) in consumed.changes {
        if let Some(value) = value {
            candidate.insert(key, value);
        } else {
            candidate.remove(&key);
        }
    }
    assert_eq!(groups_vec(&candidate), dense.export_state().unwrap().groups);
    assert_eq!(candidate.len(), 2, "untouched group 8 remains in candidate");
    assert!(candidate.contains_key(&super::simple_key(8)));
}
