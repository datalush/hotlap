use super::*;

#[test]
fn old_counter_plus_absolute_delta_bound_selects_dense() {
    let lz = simple(&[1], &[2]);
    let rz = simple(&[1], &[i64::MAX / 3 + 1]);
    let left = arrangement(&lz, &[0]);
    let right = arrangement(&rz, &[0]);
    let groups = [(
        simple_key(7),
        GroupEntry {
            rows: 7,
            values: vec![AggValue::Count(7)],
        },
    )];
    let result = run(
        DeltaPair::new(5, Arc::new(simple(&[1], &[1])), Arc::new(simple(&[], &[]))),
        &left,
        &right,
        &groups,
        recognition(vec![None], vec![None]),
    );
    match result {
        CutDecision::Dense(DenseReason::NegativeOrOverflow) => {}
        CutDecision::Dense(reason) => panic!("unexpected Dense reason: {reason:?}"),
        CutDecision::Eligible(_) => panic!("bound overflow unexpectedly Eligible"),
    }
}

#[test]
fn non_count_cached_state_is_dense_before_group_staging() {
    let old = simple(&[1], &[1]);
    let left = arrangement(&old, &[0]);
    let right = arrangement(&old, &[0]);
    let groups = [(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::SumInteger { sum: 3, count: 1 }],
        },
    )];
    let result = run(
        DeltaPair::new(11, Arc::new(simple(&[1], &[1])), Arc::new(simple(&[], &[]))),
        &left,
        &right,
        &groups,
        recognition(vec![None], vec![None]),
    );
    assert!(matches!(
        result,
        CutDecision::Dense(DenseReason::StateShape)
    ));
}

#[test]
fn count_cache_disagreement_with_group_rows_selects_dense() {
    let old = simple(&[1], &[1]);
    let left = arrangement(&old, &[0]);
    let right = arrangement(&old, &[0]);
    let groups = [(
        simple_key(7),
        GroupEntry {
            rows: 2,
            values: vec![AggValue::Count(2)],
        },
    )];
    let result = run(
        DeltaPair::new(12, Arc::new(simple(&[], &[])), Arc::new(simple(&[], &[]))),
        &left,
        &right,
        &groups,
        recognition(vec![None], vec![None]),
    );
    assert!(matches!(result, CutDecision::Dense(_)));
}

#[test]
fn authoritative_old_group_counts_and_sum_must_match_full_keyed_arrangements() {
    use hotlap_core::plan::AggFunc;
    let old_left = simple(&[1], &[1]);
    let old_right = simple(&[9], &[1]);
    let left = arrangement(&old_left, &[0]);
    let right = arrangement(&old_right, &[0]);
    let delta = || DeltaPair::new(29, Arc::new(simple(&[2], &[1])), Arc::new(simple(&[], &[])));

    let count_mapping = recognition(vec![None], vec![None]);
    let wrong_count = [(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::Count(2)],
        },
    )];
    assert!(matches!(
        run(delta(), &left, &right, &wrong_count, count_mapping),
        CutDecision::Dense(_)
    ));

    let nullable_mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![None, Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![None, Some(DataType::Int64)],
    });
    let nullable_aggs = [
        AggSpec::count(),
        AggSpec {
            func: AggFunc::Count,
            input: Some(1),
        },
    ];
    let nullable_left_z = zset(
        schema(&[("k", DataType::Int64), ("v", DataType::Int64)]),
        vec![
            Arc::new(Int64Array::from(vec![7_i64])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
        ],
        &[1],
    );
    let nullable_right_z = simple(&[9], &[1]);
    let nullable_left = arrangement(&nullable_left_z, &[0]);
    let nullable_right = arrangement(&nullable_right_z, &[0]);
    let wrong_nullable = [(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::Count(1), AggValue::Count(1)],
        },
    )];
    assert!(matches!(
        run_with_aggs(
            delta(),
            &nullable_left,
            &nullable_right,
            &wrong_nullable,
            nullable_mapping.clone(),
            &nullable_aggs
        ),
        CutDecision::Dense(_)
    ));

    let sum_mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![None, Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![None, Some(DataType::Int64)],
    });
    let sum_aggs = [
        AggSpec::count(),
        AggSpec {
            func: AggFunc::Sum,
            input: Some(1),
        },
    ];
    let wrong_sum = [(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![
                AggValue::Count(1),
                AggValue::SumInteger { sum: 2, count: 1 },
            ],
        },
    )];
    assert!(matches!(
        run_with_aggs(delta(), &left, &right, &wrong_sum, sum_mapping, &sum_aggs),
        CutDecision::Dense(_)
    ));
}

#[test]
fn input_arcs_and_epoch_are_owned_by_prepared_cut() {
    let old = simple(&[1], &[1]);
    let left = arrangement(&old, &[0]);
    let right = arrangement(&old, &[0]);
    let l = Arc::new(simple(&[2], &[1]));
    let r = Arc::new(simple(&[], &[]));
    let owners = (l.clone(), r.clone());
    let groups = [(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::Count(1)],
        },
    )];
    let result = run(
        DeltaPair::new(6, l, r),
        &left,
        &right,
        &groups,
        recognition(vec![None], vec![None]),
    );
    let CutDecision::Eligible(prepared) = result else {
        panic!("expected prepared cut")
    };
    assert_eq!(prepared.epoch, 6);
    assert!(Arc::ptr_eq(&prepared.inputs.left, &owners.0));
    assert!(Arc::ptr_eq(&prepared.inputs.right, &owners.1));
}

#[test]
fn group_subset_conversion_decodes_full_key_selects_ordered_positions() {
    let schema = schema(&[
        ("k1", DataType::Int64),
        ("k2", DataType::Int64),
        ("v", DataType::Int64),
    ]);
    let make = |a, b| {
        zset(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![a])),
                Arc::new(Int64Array::from(vec![b])),
                Arc::new(Int64Array::from(vec![3_i64])),
            ],
            &[1],
        )
    };
    let old_l = make(4, 7);
    let old_r = make(4, 7);
    let full_key = KeyConverter::new(schema.as_ref(), &[0, 1])
        .unwrap()
        .convert(old_l.batch.columns())
        .unwrap()
        .row(0)
        .as_ref()
        .to_vec();
    assert_eq!(
        super::super::stage::encode_group_key(schema.clone(), &[0, 1], &[1], &full_key).unwrap(),
        simple_key(7)
    );
    let left = arrangement(&old_l, &[0, 1]);
    let right = arrangement(&old_r, &[0, 1]);
    let dl = make(4, 7);
    let dr = zset(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ],
        &[],
    );
    let mut mapping = match recognition(vec![None], vec![None]) {
        Recognition::Eligible(m) => m,
        _ => unreachable!(),
    };
    mapping.left_join_key = vec![0, 1];
    mapping.right_join_key = vec![0, 1];
    mapping.group_key = vec![1];
    mapping.join_key_types = vec![DataType::Int64, DataType::Int64];
    let result = run(
        DeltaPair::new(9, Arc::new(dl), Arc::new(dr)),
        &left,
        &right,
        &[(
            simple_key(7),
            GroupEntry {
                rows: 1,
                values: vec![AggValue::Count(1)],
            },
        )],
        Recognition::Eligible(mapping),
    );
    let CutDecision::Eligible(prepared) = result else {
        panic!("subset-key preparation should be eligible")
    };
    assert_eq!(prepared.groups[0].0, simple_key(7));
}
