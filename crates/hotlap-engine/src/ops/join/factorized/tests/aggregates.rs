use super::*;

#[test]
fn mixed_count_nullable_sum_avg_uses_typed_keyed_bucket_stats() {
    use arrow::array::StringArray;
    use hotlap_core::plan::AggFunc;

    let left_schema = schema(&[("lk", DataType::Int64), ("a", DataType::Int64)]);
    let right_schema = schema(&[
        ("rk", DataType::Int64),
        ("b", DataType::Int64),
        ("tag", DataType::Utf8),
    ]);
    let left_batch = |values: &[Option<i64>], diffs: &[i64]| {
        zset(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(7); values.len()])),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let right_batch = |values: &[Option<i64>], tags: &[Option<&str>], diffs: &[i64]| {
        zset(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(7); values.len()])),
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(StringArray::from(tags.to_vec())),
            ],
            diffs,
        )
    };

    let old_left_z = left_batch(&[Some(2), None], &[2, 1]);
    let old_right_z = right_batch(&[Some(10), None], &[Some("x"), Some("null")], &[2, 1]);
    let left = arrangement(&old_left_z, &[0]);
    let right = arrangement(&old_right_z, &[0]);
    let delta_left = left_batch(&[Some(4)], &[1]);
    let delta_right = right_batch(&[Some(20)], &[Some("y")], &[1]);
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![Some(1), Some(3), Some(1), None],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![
            Some(DataType::Int64),
            Some(DataType::Int64),
            Some(DataType::Int64),
            None,
        ],
    });
    let aggregates = [
        AggSpec {
            func: AggFunc::Sum,
            input: Some(1),
        },
        AggSpec {
            func: AggFunc::Avg,
            input: Some(3),
        },
        AggSpec {
            func: AggFunc::Count,
            input: Some(1),
        },
        AggSpec::count(),
    ];
    let old_group = GroupEntry {
        rows: 9,
        values: vec![
            AggValue::SumInteger { sum: 12, count: 6 },
            AggValue::Avg {
                sum: 60.0,
                count: 6,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            },
            AggValue::Count(6),
            AggValue::Count(9),
        ],
    };
    let groups = [(simple_key(7), old_group)];
    let result = run_with_aggs(
        DeltaPair::new(20, Arc::new(delta_left), Arc::new(delta_right)),
        &left,
        &right,
        &groups,
        mapping.clone(),
        &aggregates,
    );
    let prepared = match result {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("typed COUNT/SUM/AVG rejected: {reason:?}"),
    };
    assert_eq!(
        prepared.groups,
        vec![(
            simple_key(7),
            GroupEntry {
                rows: 16,
                values: vec![
                    AggValue::SumInteger { sum: 32, count: 12 },
                    AggValue::Avg {
                        sum: 160.0,
                        count: 12,
                        nan: 0,
                        pos_inf: 0,
                        neg_inf: 0
                    },
                    AggValue::Count(12),
                    AggValue::Count(16),
                ],
            }
        )]
    );
}

#[test]
fn joined_row_delta_is_independent_of_aggregate_order() {
    use hotlap_core::plan::AggFunc;

    let old_side = simple(&[1], &[1]);
    let left = arrangement(&old_side, &[0]);
    let right = arrangement(&old_side, &[0]);
    let base_aggs = [
        AggSpec {
            func: AggFunc::Sum,
            input: Some(1),
        },
        AggSpec {
            func: AggFunc::Avg,
            input: Some(1),
        },
        AggSpec {
            func: AggFunc::Count,
            input: Some(1),
        },
        AggSpec::count(),
    ];
    let base_inputs = [Some(1), Some(1), Some(1), None];
    let base_types = [
        Some(DataType::Int64),
        Some(DataType::Int64),
        Some(DataType::Int64),
        None,
    ];
    let base_values = [
        AggValue::SumInteger { sum: 1, count: 1 },
        AggValue::Avg {
            sum: 1.0,
            count: 1,
            nan: 0,
            pos_inf: 0,
            neg_inf: 0,
        },
        AggValue::Count(1),
        AggValue::Count(1),
    ];
    let orderings = [[0, 1, 2, 3], [1, 0, 2, 3], [2, 1, 0, 3]];

    for ordering in orderings {
        let aggregates = ordering.map(|index| base_aggs[index]);
        let old_values = ordering.map(|index| base_values[index].clone());
        let recognition = Recognition::Eligible(ColumnMapping {
            left_source: InputId(0),
            right_source: InputId(1),
            left_join_key: vec![0],
            right_join_key: vec![0],
            group_key: vec![0],
            aggregate_inputs: ordering.map(|index| base_inputs[index]).to_vec(),
            join_key_types: vec![DataType::Int64],
            aggregate_input_types: ordering.map(|index| base_types[index].clone()).to_vec(),
        });
        let old_group = [(
            simple_key(7),
            GroupEntry {
                rows: 1,
                values: old_values.to_vec(),
            },
        )];
        let result = run_with_aggs(
            DeltaPair::new(31, Arc::new(simple(&[2], &[1])), Arc::new(simple(&[], &[]))),
            &left,
            &right,
            &old_group,
            recognition,
            &aggregates,
        );
        let CutDecision::Eligible(prepared) = result else {
            panic!("aggregate ordering {ordering:?} must preserve joined-row cardinality")
        };
        let expected = [
            AggValue::SumInteger { sum: 3, count: 2 },
            AggValue::Avg {
                sum: 3.0,
                count: 2,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            },
            AggValue::Count(2),
            AggValue::Count(2),
        ];
        assert_eq!(prepared.groups[0].1.rows, 2);
        assert_eq!(
            prepared.groups[0].1.values,
            ordering.map(|index| expected[index].clone())
        );
    }
}
