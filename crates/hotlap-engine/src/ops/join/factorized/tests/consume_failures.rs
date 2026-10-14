use super::*;

#[test]
fn output_render_error_returns_no_candidate_or_partial_changelog() {
    let empty = simple(&[], &[]);
    let left = arrangement(&empty, &[0]);
    let right = arrangement(&empty, &[0]);
    let old_groups = empty_groups();
    let aggs = [AggSpec::count(), AggSpec::sum(1)];
    let mapping = recognition(vec![None, Some(1)], vec![None, Some(DataType::Int64)]);
    let mut prepared = match prepare(PrepareInput {
        recognition: &mapping,
        aggregates: &aggs,
        inputs: DeltaPair::new(
            1,
            Arc::new(simple(&[10], &[1])),
            Arc::new(simple(&[20], &[1])),
        ),
        state: OldState {
            left: &left,
            right: &right,
            groups: &old_groups,
        },
        consumer: ConsumerCaps::FactorizedAggregates,
    }) {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("fixture rejected: {reason:?}"),
    };
    for change in &mut prepared.groups {
        if let GroupChange::Upsert { entry, .. } = change {
            entry.values[1] = AggValue::SumInteger {
                sum: i128::from(i64::MAX) + 1,
                count: 1,
            };
        }
    }
    let error =
        crate::ops::join::factorized::consume::consume(&prepared, empty.schema(), &[0], &aggs);
    assert!(
        error.is_err(),
        "out-of-range SUM must reject whole candidate"
    );
    assert!(
        old_groups.is_empty(),
        "authoritative old snapshot remains untouched"
    );
    assert!(prepared.prior_groups.is_empty());
}

#[test]
fn later_output_error_is_exact_and_discards_prior_rendered_change() {
    let s = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let batch = |keys: &[i64], values: &[i64], diffs: &[i64]| {
        zset(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let old_left = batch(&[7, 8], &[10, 20], &[1, 1]);
    let old_right = batch(&[7, 8], &[100, 200], &[1, 1]);
    let left = arrangement(&old_left, &[0]);
    let right = arrangement(&old_right, &[0]);
    let old_groups = group_map(&[
        (
            simple_key(7),
            GroupEntry {
                rows: 1,
                values: vec![
                    AggValue::Count(1),
                    AggValue::SumInteger { sum: 10, count: 1 },
                ],
            },
        ),
        (
            simple_key(8),
            GroupEntry {
                rows: 1,
                values: vec![
                    AggValue::Count(1),
                    AggValue::SumInteger { sum: 20, count: 1 },
                ],
            },
        ),
    ]);
    let old_groups_before = old_groups.clone();
    let aggs = [AggSpec::count(), AggSpec::sum(1)];
    let mapping = recognition(vec![None, Some(1)], vec![None, Some(DataType::Int64)]);
    let mut prepared = match prepare(PrepareInput {
        recognition: &mapping,
        aggregates: &aggs,
        inputs: DeltaPair::new(
            1,
            Arc::new(batch(&[7, 8], &[11, 21], &[1, 1])),
            Arc::new(batch(&[], &[], &[])),
        ),
        state: OldState {
            left: &left,
            right: &right,
            groups: &old_groups,
        },
        consumer: ConsumerCaps::FactorizedAggregates,
    }) {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("ordered fixture rejected: {reason:?}"),
    };
    prepared
        .groups
        .sort_by(|a, b| change_key(a).cmp(change_key(b)));
    assert_eq!(change_key(&prepared.groups[0]), &simple_key(7));
    assert_eq!(change_key(&prepared.groups[1]), &simple_key(8));
    assert!(matches!(
        &prepared.groups[0],
        GroupChange::Upsert { entry, .. }
            if entry.values[1] == AggValue::SumInteger { sum: 21, count: 2 }
    ));
    let staged_before_consume = prepared.groups.clone();
    let prior_before_consume = prepared.prior_groups.clone();
    for change in &mut prepared.groups {
        if let GroupChange::Upsert { key, entry } = change
            && *key == simple_key(8)
        {
            entry.values[1] = AggValue::SumInteger {
                sum: i128::from(i64::MAX) + 1,
                count: 1,
            };
        }
    }
    let error = crate::ops::join::factorized::consume::consume(&prepared, s, &[0], &aggs)
        .err()
        .expect("second output must abort whole consume");
    match error {
        crate::error::EngineError::Infrastructure(message) => {
            assert_eq!(message, "group sum out of range for Int64")
        }
        other => panic!("unexpected renderer error variant: {other}"),
    }
    assert_eq!(
        old_groups, old_groups_before,
        "authoritative old groups unchanged"
    );
    assert_eq!(prepared.prior_groups, prior_before_consume);
    assert_ne!(
        prepared.groups, staged_before_consume,
        "test intentionally corrupts the final candidate before rendering"
    );
}

fn change_key(change: &GroupChange) -> &[u8] {
    match change {
        GroupChange::Upsert { key, .. } | GroupChange::Remove { key } => key,
    }
}

#[test]
fn dense_cap_rejection_leaves_old_arrangements_and_group_state_untouched() {
    let old = simple(&[3], &[1]);
    let left = arrangement(&old, &[0]);
    let right = arrangement(&old, &[0]);
    let old_groups = group_map(&[(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![AggValue::Count(1)],
        },
    )]);
    let before_left = left.to_zset_for_key(&simple_key(7)).unwrap();
    let before_right = right.to_zset_for_key(&simple_key(7)).unwrap();
    let aggs = [AggSpec::count()];
    let mapping = recognition(vec![None], vec![None]);
    let decision = prepare(PrepareInput {
        recognition: &mapping,
        aggregates: &aggs,
        inputs: DeltaPair::new(1, Arc::new(simple(&[4], &[1])), Arc::new(simple(&[], &[]))),
        state: OldState {
            left: &left,
            right: &right,
            groups: &old_groups,
        },
        consumer: ConsumerCaps::flat_rows(ConsumerCaps::DEFAULT_FLAT_CHUNK_ROWS),
    });
    assert!(matches!(
        decision,
        CutDecision::Dense(DenseReason::UnsupportedConsumer)
    ));
    assert_eq!(old_groups[&simple_key(7)].rows, 1);
    assert_eq!(
        left.to_zset_for_key(&simple_key(7)).unwrap().len(),
        before_left.len()
    );
    assert_eq!(
        right.to_zset_for_key(&simple_key(7)).unwrap().len(),
        before_right.len()
    );
}
