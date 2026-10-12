use super::*;

#[test]
fn different_valid_side_schemas_are_accepted_by_per_side_mapping() {
    let mk = |name: &str| {
        let schema = schema(&[(name, DataType::Int64)]);
        zset(schema, vec![Arc::new(Int64Array::from(vec![7_i64]))], &[1])
    };
    let left_delta = mk("left_key");
    let right_delta = mk("right_key");
    let left_old = arrangement(&left_delta, &[0]);
    let right_old = arrangement(&right_delta, &[0]);
    let result = run(
        DeltaPair::new(1, Arc::new(left_delta), Arc::new(right_delta)),
        &left_old,
        &right_old,
        &[],
        recognition(vec![None], vec![None]),
    );
    assert!(!matches!(
        result,
        CutDecision::Dense(DenseReason::InputMapping)
    ));
}

#[test]
fn flat_rows_consumer_cannot_receive_aggregate_only_prepared_cut() {
    let empty = simple(&[], &[]);
    let left = arrangement(&empty, &[0]);
    let right = arrangement(&empty, &[0]);
    let recognition = recognition(vec![None], vec![None]);
    let aggregates = [AggSpec::count()];
    let result = prepare(PrepareInput {
        recognition: &recognition,
        aggregates: &aggregates,
        inputs: DeltaPair::new(30, Arc::new(simple(&[1], &[1])), Arc::new(simple(&[], &[]))),
        state: OldState {
            left: &left,
            right: &right,
            groups: &[],
        },
        consumer: ConsumerCaps::flat_rows(ConsumerCaps::DEFAULT_FLAT_CHUNK_ROWS),
    });
    assert!(matches!(
        result,
        CutDecision::Dense(DenseReason::UnsupportedConsumer)
    ));
}

#[test]
fn strict_group_attribution_uses_full_join_keys_then_merges_declared_subset() {
    let schema = schema(&[
        ("k1", DataType::Int64),
        ("k2", DataType::Int64),
        ("v", DataType::Int64),
    ]);
    let make = |keys: &[(i64, i64)], values: &[i64], diffs: &[i64]| {
        zset(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    keys.iter().map(|k| k.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    keys.iter().map(|k| k.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let left_old_z = make(
        &[(4, 7), (5, 7), (6, 8), (9, 8)],
        &[10, 11, 12, 14],
        &[2, 1, 5, 2],
    );
    let right_old_z = make(&[(4, 7), (5, 7), (9, 8)], &[20, 21, 24], &[3, 2, 2]);
    let left_old = arrangement(&left_old_z, &[0, 1]);
    let right_old = arrangement(&right_old_z, &[0, 1]);
    let delta_left = make(&[(4, 7), (6, 8)], &[10, 13], &[1, 2]);
    let delta_right = make(&[(5, 7)], &[21], &[1]);
    let mut mapping = match recognition(vec![None], vec![None]) {
        Recognition::Eligible(m) => m,
        _ => unreachable!(),
    };
    mapping.left_join_key = vec![0, 1];
    mapping.right_join_key = vec![0, 1];
    mapping.group_key = vec![1];
    mapping.join_key_types = vec![DataType::Int64, DataType::Int64];
    let recognition = Recognition::Eligible(mapping);
    let groups = [
        (
            simple_key(7),
            GroupEntry {
                rows: 8,
                values: vec![AggValue::Count(8)],
            },
        ),
        (
            simple_key(8),
            GroupEntry {
                rows: 4,
                values: vec![AggValue::Count(4)],
            },
        ),
    ];
    let prior = groups.clone();
    let result = run(
        DeltaPair::new(2, Arc::new(delta_left), Arc::new(delta_right)),
        &left_old,
        &right_old,
        &groups,
        recognition,
    );
    let prepared = match result {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => {
            panic!("valid same-subset keys unexpectedly Dense: {reason:?}")
        }
    };
    assert_eq!(
        prepared.groups,
        vec![
            (
                simple_key(7),
                GroupEntry {
                    rows: 12,
                    values: vec![AggValue::Count(12)]
                }
            ),
            (
                simple_key(8),
                GroupEntry {
                    rows: 4,
                    values: vec![AggValue::Count(4)]
                }
            ),
        ]
    );
    assert_eq!(groups, prior);
    assert_eq!(
        prepared
            .left
            .iter()
            .map(|r| (r.old, r.new))
            .collect::<Vec<_>>(),
        vec![(2, 3), (0, 2)]
    );
    assert_eq!(
        prepared
            .right
            .iter()
            .map(|r| (r.old, r.new))
            .collect::<Vec<_>>(),
        vec![(2, 3)]
    );
}

#[test]
fn absent_identity_retraction_is_dense_even_with_other_positive_delta() {
    let old = simple(&[1], &[1]);
    let left = arrangement(&old, &[0]);
    let right = arrangement(&old, &[0]);
    let groups = [(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::Count(1)],
        },
    )];
    let result = run(
        DeltaPair::new(
            3,
            Arc::new(simple(&[9, 2], &[-1, 1])),
            Arc::new(simple(&[], &[])),
        ),
        &left,
        &right,
        &groups,
        recognition(vec![None], vec![None]),
    );
    assert!(
        matches!(result, CutDecision::Dense(DenseReason::NegativeOrOverflow)),
        "unexpected decision: {}",
        match result {
            CutDecision::Dense(ref reason) => format!("{reason:?}"),
            CutDecision::Eligible(_) => "Eligible".into(),
        }
    );
}

#[test]
fn zero_aggregate_shape_returns_dense_without_indexing() {
    let empty = simple(&[], &[]);
    let left = arrangement(&empty, &[0]);
    let right = arrangement(&empty, &[0]);
    let result = run(
        DeltaPair::new(4, Arc::new(empty.clone()), Arc::new(empty)),
        &left,
        &right,
        &[],
        recognition(vec![], vec![]),
    );
    assert!(matches!(
        result,
        CutDecision::Dense(DenseReason::StateShape)
    ));
}

#[test]
fn same_join_key_retraction_and_insert_preserves_old_group_rows() {
    let old = simple(&[1], &[1]);
    let left = arrangement(&old, &[0]);
    let right = arrangement(&old, &[0]);
    let groups = [(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::Count(1)],
        },
    )];
    let result = run(
        DeltaPair::new(
            40,
            Arc::new(simple(&[2, 1], &[1, -1])),
            Arc::new(simple(&[], &[])),
        ),
        &left,
        &right,
        &groups,
        recognition(vec![None], vec![None]),
    );
    let CutDecision::Eligible(prepared) = result else {
        panic!("safe replacement should prepare")
    };
    assert_eq!(prepared.groups[0].1.rows, 1);
}

#[test]
fn retracting_last_join_row_removes_group_from_prepared_final_state() {
    let old = simple(&[1], &[1]);
    let left = arrangement(&old, &[0]);
    let right = arrangement(&old, &[0]);
    let groups = [(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::Count(1)],
        },
    )];
    let before = groups.clone();
    let result = run(
        DeltaPair::new(
            41,
            Arc::new(simple(&[1], &[-1])),
            Arc::new(simple(&[], &[])),
        ),
        &left,
        &right,
        &groups,
        recognition(vec![None], vec![None]),
    );
    let CutDecision::Eligible(prepared) = result else {
        panic!("valid final retraction should prepare")
    };
    assert!(
        prepared.groups.is_empty(),
        "zero-row group must be absent from final prepared state"
    );
    assert_eq!(
        prepared.group_changes,
        vec![GroupChange::Remove { key: simple_key(7) }]
    );
    assert_eq!(groups, before);
}
