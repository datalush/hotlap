use super::*;

#[test]
fn null_only_delta_preserves_typed_aggregates_in_each_order_and_sign() {
    use hotlap_core::plan::AggFunc;

    let input_schema = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let nullable_batch = |values: &[Option<i64>], diffs: &[i64]| {
        zset(
            input_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(7_i64); values.len()])),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let old_left_z = nullable_batch(&[Some(2), None], &[1, 1]);
    let old_right_z = simple(&[10], &[1]);
    let left = arrangement(&old_left_z, &[0]);
    let right = arrangement(&old_right_z, &[0]);
    let old_left_entries = left.iter().collect::<Vec<_>>();
    let old_right_entries = right.iter().collect::<Vec<_>>();
    let base_aggs = [
        AggSpec {
            func: AggFunc::Count,
            input: Some(1),
        },
        AggSpec {
            func: AggFunc::Sum,
            input: Some(1),
        },
        AggSpec {
            func: AggFunc::Avg,
            input: Some(1),
        },
        AggSpec::count(),
    ];
    let base_values = [
        AggValue::Count(1),
        AggValue::SumInteger { sum: 2, count: 1 },
        AggValue::Avg {
            sum: 2.0,
            count: 1,
            nan: 0,
            pos_inf: 0,
            neg_inf: 0,
        },
        AggValue::Count(2),
    ];
    let orders = [[0, 1, 2, 3], [1, 2, 0, 3], [2, 1, 0, 3]];
    for order in orders {
        let aggregates = order.map(|index| base_aggs[index]);
        let old_groups = group_map(&[(
            simple_key(7),
            GroupEntry {
                rows: 2,
                values: order.map(|index| base_values[index].clone()).to_vec(),
            },
        )]);
        let groups_before = old_groups.clone();
        let recognition = Recognition::Eligible(ColumnMapping {
            left_source: InputId(0),
            right_source: InputId(1),
            left_join_key: vec![0],
            right_join_key: vec![0],
            group_key: vec![0],
            aggregate_inputs: order.map(|index| base_aggs[index].input).to_vec(),
            join_key_types: vec![DataType::Int64],
            aggregate_input_types: order
                .map(|index| base_aggs[index].input.map(|_| DataType::Int64))
                .to_vec(),
        });
        for (diff, expected_rows) in [(1, 3), (-1, 1)] {
            let delta_left = nullable_batch(&[None], &[diff]);
            let delta_right = simple(&[], &[]);
            let result = run_with_aggs(
                DeltaPair::new(33, Arc::new(delta_left), Arc::new(delta_right)),
                &left,
                &right,
                &old_groups,
                recognition.clone(),
                &aggregates,
            );
            let CutDecision::Eligible(prepared) = result else {
                panic!(
                    "NULL-only delta diff {diff} order {order:?} must prepare: {}",
                    match result {
                        CutDecision::Dense(reason) => format!("{reason:?}"),
                        CutDecision::Eligible(_) => "Eligible".into(),
                    }
                )
            };
            let expected = [
                AggValue::Count(1),
                AggValue::SumInteger { sum: 2, count: 1 },
                AggValue::Avg {
                    sum: 2.0,
                    count: 1,
                    nan: 0,
                    pos_inf: 0,
                    neg_inf: 0,
                },
                AggValue::Count(expected_rows),
            ];
            let expected_entry = GroupEntry {
                rows: expected_rows,
                values: order.map(|index| expected[index].clone()).to_vec(),
            };
            assert_eq!(
                groups_vec(&apply_changes_for_test(&old_groups, &prepared.groups)),
                vec![(simple_key(7), expected_entry.clone())]
            );
            assert_eq!(
                prepared.groups,
                vec![GroupChange::Upsert {
                    key: simple_key(7),
                    entry: expected_entry
                }]
            );
            assert!(
                prepared
                    .groups
                    .iter()
                    .all(|change| !matches!(change, GroupChange::Remove { .. }))
            );
            assert_eq!(old_groups, groups_before);
            assert_eq!(left.iter().collect::<Vec<_>>(), old_left_entries);
            assert_eq!(right.iter().collect::<Vec<_>>(), old_right_entries);
        }
    }

    let all_null = nullable_batch(&[None], &[1]);
    let all_null_left = arrangement(&all_null, &[0]);
    let all_null_groups = group_map(&[(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![
                AggValue::Count(0),
                AggValue::SumInteger { sum: 0, count: 0 },
                AggValue::Avg {
                    sum: 0.0,
                    count: 0,
                    nan: 0,
                    pos_inf: 0,
                    neg_inf: 0,
                },
                AggValue::Count(1),
            ],
        },
    )]);
    let order = [0, 1, 2, 3];
    let aggregates = order.map(|index| base_aggs[index]);
    let recognition = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: order.map(|index| base_aggs[index].input).to_vec(),
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: order
            .map(|index| base_aggs[index].input.map(|_| DataType::Int64))
            .to_vec(),
    });
    let result = run_with_aggs(
        DeltaPair::new(
            34,
            Arc::new(nullable_batch(&[None], &[1])),
            Arc::new(simple(&[], &[])),
        ),
        &all_null_left,
        &right,
        &all_null_groups,
        recognition,
        &aggregates,
    );
    let CutDecision::Eligible(prepared) = result else {
        panic!("all-NULL aggregate group should remain typed and eligible")
    };
    assert_eq!(
        changed_entry(&prepared.groups, &simple_key(7))
            .unwrap()
            .rows,
        2
    );
    assert_eq!(
        changed_entry(&prepared.groups, &simple_key(7))
            .unwrap()
            .values,
        vec![
            AggValue::Count(0),
            AggValue::SumInteger { sum: 0, count: 0 },
            AggValue::Avg {
                sum: 0.0,
                count: 0,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0
            },
            AggValue::Count(2),
        ]
    );
}

#[test]
fn all_null_sum_avg_keep_nonempty_group_with_null_accumulator_counts() {
    use arrow::array::StringArray;
    use hotlap_core::plan::AggFunc;
    let ls = schema(&[("lk", DataType::Int64), ("a", DataType::Int64)]);
    let rs = schema(&[
        ("rk", DataType::Int64),
        ("b", DataType::Int64),
        ("tag", DataType::Utf8),
    ]);
    let left_z = zset(
        ls.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(7_i64)])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
        ],
        &[1],
    );
    let right_z = zset(
        rs.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(7_i64)])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(StringArray::from(vec![Some("n")])),
        ],
        &[1],
    );
    let left = arrangement(&left_z, &[0]);
    let right = arrangement(&right_z, &[0]);
    let left_delta = zset(
        ls,
        vec![
            Arc::new(Int64Array::from(vec![Some(7_i64)])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
        ],
        &[1],
    );
    let right_delta = zset(
        rs,
        vec![
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ],
        &[],
    );
    let recognition = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![None, Some(1), Some(1), Some(3)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![
            None,
            Some(DataType::Int64),
            Some(DataType::Int64),
            Some(DataType::Int64),
        ],
    });
    let aggs = [
        AggSpec::count(),
        AggSpec {
            func: AggFunc::Count,
            input: Some(1),
        },
        AggSpec {
            func: AggFunc::Sum,
            input: Some(1),
        },
        AggSpec {
            func: AggFunc::Avg,
            input: Some(3),
        },
    ];
    let old = group_map(&[(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![
                AggValue::Count(1),
                AggValue::Count(0),
                AggValue::SumInteger { sum: 0, count: 0 },
                AggValue::Avg {
                    sum: 0.0,
                    count: 0,
                    nan: 0,
                    pos_inf: 0,
                    neg_inf: 0,
                },
            ],
        },
    )]);
    let result = run_with_aggs(
        DeltaPair::new(21, Arc::new(left_delta), Arc::new(right_delta)),
        &left,
        &right,
        &old,
        recognition,
        &aggs,
    );
    let CutDecision::Eligible(prepared) = result else {
        panic!("all-null aggregate inputs should be eligible")
    };
    assert_eq!(
        changed_entry(&prepared.groups, &simple_key(7))
            .unwrap()
            .clone(),
        GroupEntry {
            rows: 2,
            values: vec![
                AggValue::Count(2),
                AggValue::Count(0),
                AggValue::SumInteger { sum: 0, count: 0 },
                AggValue::Avg {
                    sum: 0.0,
                    count: 0,
                    nan: 0,
                    pos_inf: 0,
                    neg_inf: 0
                }
            ]
        }
    );
}
