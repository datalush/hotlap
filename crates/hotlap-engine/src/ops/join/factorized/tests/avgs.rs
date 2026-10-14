use super::*;

#[test]
fn avg_negative_zero_history_is_rejected_as_non_exact() {
    use hotlap_core::plan::AggFunc;
    let old_left_z = simple(&[0], &[1]);
    let old_right_z = simple(&[1], &[1]);
    let left = arrangement(&old_left_z, &[0]);
    let right = arrangement(&old_right_z, &[0]);
    let groups = group_map(&[(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::Avg {
                sum: -0.0,
                count: 1,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            }],
        },
    )]);
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Int64)],
    });
    let aggs = [AggSpec {
        func: AggFunc::Avg,
        input: Some(1),
    }];
    let before = groups.clone();
    let result = run_with_aggs(
        DeltaPair::new(23, Arc::new(simple(&[0], &[1])), Arc::new(simple(&[], &[]))),
        &left,
        &right,
        &groups,
        mapping,
        &aggs,
    );
    assert!(matches!(
        result,
        CutDecision::Dense(DenseReason::UnsupportedNumeric)
    ));
    assert_eq!(groups, before);
}

#[test]
fn avg_old_history_includes_untouched_full_keys_collapsed_to_subset_group() {
    use hotlap_core::plan::AggFunc;
    let s = schema(&[
        ("k1", DataType::Int64),
        ("k2", DataType::Int64),
        ("v", DataType::Int64),
    ]);
    let make = |keys: &[(i64, i64)], values: &[i64], diffs: &[i64]| {
        zset(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(
                    keys.iter().map(|key| key.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    keys.iter().map(|key| key.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let old_left_z = make(&[(4, 7), (5, 7)], &[2, 3], &[1, 1]);
    let old_right_z = make(&[(4, 7), (5, 7)], &[10, 10], &[1, 1]);
    let left = arrangement(&old_left_z, &[0, 1]);
    let right = arrangement(&old_right_z, &[0, 1]);
    let delta_left = make(&[(4, 7)], &[4], &[1]);
    let delta_right = make(&[], &[], &[]);
    let mut m = match recognition(vec![None], vec![None]) {
        Recognition::Eligible(m) => m,
        _ => unreachable!(),
    };
    m.left_join_key = vec![0, 1];
    m.right_join_key = vec![0, 1];
    m.group_key = vec![1];
    m.join_key_types = vec![DataType::Int64, DataType::Int64];
    m.aggregate_inputs = vec![Some(2)];
    m.aggregate_input_types = vec![Some(DataType::Int64)];
    let recognition = Recognition::Eligible(m);
    let aggs = [AggSpec {
        func: AggFunc::Avg,
        input: Some(2),
    }];
    let groups = group_map(&[(
        simple_key(7),
        GroupEntry {
            rows: 2,
            values: vec![AggValue::Avg {
                sum: 5.0,
                count: 2,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            }],
        },
    )]);
    let result = run_with_aggs(
        DeltaPair::new(24, Arc::new(delta_left), Arc::new(delta_right)),
        &left,
        &right,
        &groups,
        recognition,
        &aggs,
    );
    let CutDecision::Eligible(prepared) = result else {
        panic!("old AVG derivation must include untouched same-subset key")
    };
    assert_eq!(
        changed_entry(&prepared.groups, &simple_key(7))
            .unwrap()
            .clone(),
        GroupEntry {
            rows: 3,
            values: vec![AggValue::Avg {
                sum: 9.0,
                count: 3,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0
            }]
        }
    );
    crate::ops::join::factorized::OLD_HISTORY_KEYS_CANDIDATE
        .with(|count| assert_eq!(count.get(), 4));
}

#[test]
fn full_group_key_coverage_detects_permuted_duplicate_mapping() {
    let mut mapping = match recognition(vec![None], vec![None]) {
        Recognition::Eligible(mapping) => mapping,
        _ => unreachable!(),
    };
    mapping.group_key = vec![0, 1, 0];
    mapping.join_key_types = vec![DataType::Int64, DataType::Int64];
    assert!(group_key_covers_join_key(&mapping));
    mapping.group_key = vec![0, 0];
    assert!(!group_key_covers_join_key(&mapping));
}

#[test]
fn full_group_key_coverage_avoids_arrangement_metadata_scan() {
    let s = schema(&[
        ("k1", DataType::Int64),
        ("k2", DataType::Int64),
        ("v", DataType::Int64),
    ]);
    let make = |keys: &[(i64, i64)], values: &[i64], diffs: &[i64]| {
        zset(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(
                    keys.iter().map(|key| key.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    keys.iter().map(|key| key.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let old_l = make(&[(1, 1), (2, 2), (3, 3)], &[10, 20, 30], &[1; 3]);
    let old_r = make(&[(1, 1), (2, 2), (3, 3)], &[2, 3, 4], &[1; 3]);
    let left = arrangement(&old_l, &[0, 1]);
    let right = arrangement(&old_r, &[0, 1]);
    let delta_l = make(&[(1, 1)], &[11], &[1]);
    let delta_r = make(&[], &[], &[]);
    let mut mapping = match recognition(vec![None], vec![None]) {
        Recognition::Eligible(mapping) => mapping,
        _ => unreachable!(),
    };
    mapping.left_join_key = vec![0, 1];
    mapping.right_join_key = vec![0, 1];
    mapping.join_key_types = vec![DataType::Int64, DataType::Int64];
    mapping.group_key = vec![0, 1, 0];
    mapping.aggregate_inputs = vec![None];
    mapping.aggregate_input_types = vec![None];
    let join_conv = arrow::row::RowConverter::new(vec![
        arrow::row::SortField::new(DataType::Int64),
        arrow::row::SortField::new(DataType::Int64),
    ])
    .unwrap();
    let group_conv = arrow::row::RowConverter::new(vec![
        arrow::row::SortField::new(DataType::Int64),
        arrow::row::SortField::new(DataType::Int64),
        arrow::row::SortField::new(DataType::Int64),
    ])
    .unwrap();
    let groups = group_map(
        &[(1, 1), (2, 2), (3, 3)]
            .iter()
            .map(|&(a, b)| {
                let encoded = join_conv
                    .convert_columns(&[
                        Arc::new(Int64Array::from(vec![a])),
                        Arc::new(Int64Array::from(vec![b])),
                    ])
                    .unwrap();
                let key = super::super::prepare::group_key_for(
                    &join_conv,
                    &group_conv,
                    encoded.row(0).as_ref(),
                    &[0, 1, 0],
                )
                .unwrap();
                (
                    key,
                    GroupEntry {
                        rows: 1,
                        values: vec![AggValue::Count(1)],
                    },
                )
            })
            .collect::<Vec<_>>(),
    );
    let touched_encoded = join_conv
        .convert_columns(&[
            Arc::new(Int64Array::from(vec![1_i64])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ])
        .unwrap();
    let touched_group_key = super::super::prepare::group_key_for(
        &join_conv,
        &group_conv,
        touched_encoded.row(0).as_ref(),
        &[0, 1, 0],
    )
    .unwrap();
    let result = run_with_aggs(
        DeltaPair::new(1, Arc::new(delta_l), Arc::new(delta_r)),
        &left,
        &right,
        &groups,
        Recognition::Eligible(mapping),
        &[AggSpec::count()],
    );
    let CutDecision::Eligible(prepared) = result else {
        panic!("valid full-key state must remain eligible")
    };
    assert_eq!(prepared.groups.len(), 1);
    crate::ops::join::factorized::OLD_HISTORY_KEYS_CANDIDATE
        .with(|count| assert_eq!(count.get(), 0));
    crate::ops::join::factorized::OLD_HISTORY_DIRECT_LOOKUPS
        .with(|count| assert_eq!(count.get(), 2));
    crate::ops::join::factorized::OLD_BUCKET_DECODES.with(|count| assert_eq!(count.get(), 2));
    let mut corrupt = groups.clone();
    corrupt.remove(&touched_group_key);
    assert!(matches!(
        run_with_aggs(
            DeltaPair::new(
                2,
                Arc::new(make(&[(1, 1)], &[11], &[1])),
                Arc::new(make(&[], &[], &[]))
            ),
            &left,
            &right,
            &corrupt,
            Recognition::Eligible({
                let mut mapping = match recognition(vec![None], vec![None]) {
                    Recognition::Eligible(mapping) => mapping,
                    _ => unreachable!(),
                };
                mapping.left_join_key = vec![0, 1];
                mapping.right_join_key = vec![0, 1];
                mapping.join_key_types = vec![DataType::Int64, DataType::Int64];
                mapping.group_key = vec![0, 1, 0];
                mapping.aggregate_inputs = vec![None];
                mapping.aggregate_input_types = vec![None];
                mapping
            }),
            &[AggSpec::count()],
        ),
        CutDecision::Dense(DenseReason::StateShape)
    ));
    let mut wrong_count = groups.clone();
    wrong_count.get_mut(&touched_group_key).unwrap().rows = 2;
    wrong_count.get_mut(&touched_group_key).unwrap().values = vec![AggValue::Count(2)];
    assert!(matches!(
        run_with_aggs(
            DeltaPair::new(
                3,
                Arc::new(make(&[(1, 1)], &[11], &[1])),
                Arc::new(make(&[], &[], &[]))
            ),
            &left,
            &right,
            &wrong_count,
            Recognition::Eligible({
                let mut mapping = match recognition(vec![None], vec![None]) {
                    Recognition::Eligible(mapping) => mapping,
                    _ => unreachable!(),
                };
                mapping.left_join_key = vec![0, 1];
                mapping.right_join_key = vec![0, 1];
                mapping.join_key_types = vec![DataType::Int64, DataType::Int64];
                mapping.group_key = vec![0, 1, 0];
                mapping.aggregate_inputs = vec![None];
                mapping.aggregate_input_types = vec![None];
                mapping
            }),
            &[AggSpec::count()],
        ),
        CutDecision::Dense(DenseReason::StateShape)
    ));
}

#[test]
fn avg_proposed_absolute_state_includes_old_cancelled_full_keys() {
    use hotlap_core::plan::AggFunc;

    let left_schema = schema(&[
        ("k1", DataType::Int64),
        ("k2", DataType::Int64),
        ("v", DataType::Int64),
    ]);
    let right_schema = schema(&[
        ("k1", DataType::Int64),
        ("k2", DataType::Int64),
        ("r", DataType::Int64),
    ]);
    let left_batch = |keys: &[(i64, i64)], values: &[i64], diffs: &[i64]| {
        zset(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    keys.iter().map(|key| key.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    keys.iter().map(|key| key.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let right_batch = |keys: &[(i64, i64)], diffs: &[i64]| {
        zset(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    keys.iter().map(|key| key.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    keys.iter().map(|key| key.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vec![1_i64; keys.len()])),
            ],
            diffs,
        )
    };
    let old_left = left_batch(&[(4, 7), (5, 7)], &[(1_i64 << 52), -(1_i64 << 52)], &[1, 1]);
    let old_right = right_batch(&[(4, 7), (5, 7)], &[1, 1]);
    let left = arrangement(&old_left, &[0, 1]);
    let right = arrangement(&old_right, &[0, 1]);
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![1],
        aggregate_inputs: vec![Some(2)],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Int64)],
    });
    let aggs = [AggSpec {
        func: AggFunc::Avg,
        input: Some(2),
    }];
    let groups = group_map(&[(
        simple_key(7),
        GroupEntry {
            rows: 2,
            values: vec![AggValue::Avg {
                sum: 0.0,
                count: 2,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            }],
        },
    )]);
    let delta_left = left_batch(&[(4, 7)], &[1], &[1]);
    let delta_right = right_batch(&[], &[]);
    let result = run_with_aggs(
        DeltaPair::new(32, Arc::new(delta_left), Arc::new(delta_right)),
        &left,
        &right,
        &groups,
        mapping.clone(),
        &aggs,
    );
    assert!(matches!(
        result,
        CutDecision::Dense(DenseReason::UnsupportedNumeric)
    ));

    let retract_left = left_batch(&[(4, 7)], &[(1_i64 << 52)], &[-1]);
    let retract_right = right_batch(&[], &[]);
    let result = run_with_aggs(
        DeltaPair::new(35, Arc::new(retract_left), Arc::new(retract_right)),
        &left,
        &right,
        &groups,
        mapping,
        &aggs,
    );
    let CutDecision::Eligible(prepared) = result else {
        panic!("retraction reducing proposed absolute AVG history should stay eligible")
    };
    assert_eq!(
        changed_entry(&prepared.groups, &simple_key(7))
            .unwrap()
            .clone(),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::Avg {
                sum: -((1_i64 << 52) as f64),
                count: 1,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            }],
        }
    );
}

#[test]
fn avg_intermediate_absolute_sum_bound_rejects_rounding_path() {
    use arrow::array::Float64Array;
    use hotlap_core::plan::AggFunc;

    let left_schema = schema(&[
        ("k", DataType::Int64),
        ("tag", DataType::Int64),
        ("value", DataType::Int64),
    ]);
    let make_left = |tags: &[i64], values: &[i64], diffs: &[i64]| {
        zset(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7_i64; tags.len()])),
                Arc::new(Int64Array::from(tags.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let old_left = make_left(&[1, 2], &[(1_i64 << 53) - 2, 1], &[1, 1]);
    let delta_left = make_left(&[0, 2], &[2, 1], &[1, -1]);
    let left = arrangement(&old_left, &[0]);
    let right = arrangement(&simple(&[9], &[1]), &[0]);
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![Some(2)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Int64)],
    });
    let aggs = [AggSpec {
        func: AggFunc::Avg,
        input: Some(2),
    }];
    let old_groups = group_map(&[(
        simple_key(7),
        GroupEntry {
            rows: 2,
            values: vec![AggValue::Avg {
                sum: ((1_i64 << 53) - 1) as f64,
                count: 2,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            }],
        },
    )]);

    let mut dense = crate::ops::group_aggregate::GroupAggregate::new(&[0], aggs.to_vec());
    let dense_old_changes = dense
        .apply(&crate::zset::sort_rows(&old_left).unwrap())
        .unwrap();
    let dense_old_diffs = dense_old_changes
        .diff()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let dense_old_values = dense_old_changes
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let dense_old_insert = (0..dense_old_changes.len())
        .find(|&row| dense_old_diffs.value(row) == 1)
        .expect("dense old state should emit its aggregate row");
    let dense_avg = dense_old_values.value(dense_old_insert);
    assert_eq!(dense_avg.to_bits(), ((1_i64 << 52) as f64 - 0.5).to_bits());

    let dense_changes = dense
        .apply(&crate::zset::sort_rows(&delta_left).unwrap())
        .unwrap();
    assert!(
        dense_changes.is_empty(),
        "ordered Dense fold should preserve this AVG exactly"
    );

    let result = run_with_aggs(
        DeltaPair::new(36, Arc::new(delta_left), Arc::new(simple(&[], &[]))),
        &left,
        &right,
        &old_groups,
        mapping,
        &aggs,
    );
    assert!(matches!(
        result,
        CutDecision::Dense(DenseReason::UnsupportedNumeric)
    ));
}

#[test]
fn avg_integer_history_over_exact_f64_domain_selects_dense() {
    use hotlap_core::plan::AggFunc;
    let value = (1_i64 << 53) + 1;
    let old = simple(&[value], &[1]);
    let right_old = simple(&[1], &[1]);
    let left = arrangement(&old, &[0]);
    let right = arrangement(&right_old, &[0]);
    let groups = group_map(&[(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::Avg {
                sum: value as f64,
                count: 1,
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
            }],
        },
    )]);
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Int64)],
    });
    let aggs = [AggSpec {
        func: AggFunc::Avg,
        input: Some(1),
    }];
    let result = run_with_aggs(
        DeltaPair::new(25, Arc::new(simple(&[1], &[1])), Arc::new(simple(&[], &[]))),
        &left,
        &right,
        &groups,
        mapping,
        &aggs,
    );
    assert!(matches!(
        result,
        CutDecision::Dense(DenseReason::UnsupportedNumeric)
    ));
}

#[test]
fn new_avg_rejects_unsafe_cancellation_that_dense_fold_rounds() {
    use arrow::array::Float64Array;
    use hotlap_core::plan::AggFunc;
    let input = simple(&[(1_i64 << 53) + 1, -(1_i64 << 53)], &[1, 1]);
    let empty = simple(&[], &[]);
    let left = arrangement(&empty, &[0]);
    let right_old = simple(&[9], &[1]);
    let right = arrangement(&right_old, &[0]);
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Int64)],
    });
    let aggs = [AggSpec {
        func: AggFunc::Avg,
        input: Some(1),
    }];
    let mut dense = crate::ops::group_aggregate::GroupAggregate::new(&[0], aggs.to_vec());
    let dense_out = dense.apply(&input).unwrap();
    assert_eq!(
        dense_out
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.0
    );
    let result = run_with_aggs(
        DeltaPair::new(27, Arc::new(input), Arc::new(simple(&[], &[]))),
        &left,
        &right,
        &empty_groups(),
        mapping,
        &aggs,
    );
    assert!(matches!(
        result,
        CutDecision::Dense(DenseReason::UnsupportedNumeric)
    ));
}

#[test]
fn new_avg_exact_small_cancellation_remains_eligible() {
    use hotlap_core::plan::AggFunc;
    let input = simple(&[5, -5], &[1, 1]);
    let empty = simple(&[], &[]);
    let left = arrangement(&empty, &[0]);
    let right = arrangement(&simple(&[9], &[1]), &[0]);
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Int64)],
    });
    let aggs = [AggSpec {
        func: AggFunc::Avg,
        input: Some(1),
    }];
    let result = run_with_aggs(
        DeltaPair::new(28, Arc::new(input), Arc::new(simple(&[], &[]))),
        &left,
        &right,
        &empty_groups(),
        mapping,
        &aggs,
    );
    let CutDecision::Eligible(prepared) = result else {
        panic!("exact cancellation within the bound should be eligible")
    };
    assert_eq!(
        changed_entry(&prepared.groups, &simple_key(7))
            .unwrap()
            .values,
        vec![AggValue::Avg {
            sum: 0.0,
            count: 2,
            nan: 0,
            pos_inf: 0,
            neg_inf: 0
        }]
    );
}
