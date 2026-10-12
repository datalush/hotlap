use super::*;

#[test]
fn integer_sum_render_overflow_selects_dense_without_mutating_old_state() {
    use hotlap_core::plan::AggFunc;
    let oldz = simple(&[i64::MAX], &[1]);
    let left = arrangement(&oldz, &[0]);
    let right_oldz = simple(&[1], &[1]);
    let right = arrangement(&right_oldz, &[0]);
    let before_left = left.iter().collect::<Vec<_>>();
    let before_right = right.iter().collect::<Vec<_>>();
    let groups = [(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![
                AggValue::Count(1),
                AggValue::SumInteger {
                    sum: i64::MAX as i128,
                    count: 1,
                },
            ],
        },
    )];
    let before_groups = groups.clone();
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![None, Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![None, Some(DataType::Int64)],
    });
    let aggs = [
        AggSpec::count(),
        AggSpec {
            func: AggFunc::Sum,
            input: Some(1),
        },
    ];
    let result = run_with_aggs(
        DeltaPair::new(22, Arc::new(simple(&[1], &[1])), Arc::new(simple(&[], &[]))),
        &left,
        &right,
        &groups,
        mapping,
        &aggs,
    );
    assert!(matches!(result, CutDecision::Dense(_)));
    assert_eq!(groups, before_groups);
    assert_eq!(left.iter().collect::<Vec<_>>(), before_left);
    assert_eq!(right.iter().collect::<Vec<_>>(), before_right);
}

#[test]
fn authoritative_integer_sum_near_i128_limit_rejects_delta_bound() {
    use hotlap_core::plan::AggFunc;
    let oldz = simple(&[1], &[1]);
    let rightz = simple(&[1], &[1]);
    let left = arrangement(&oldz, &[0]);
    let right = arrangement(&rightz, &[0]);
    let groups = [(
        simple_key(7),
        GroupEntry {
            rows: 1,
            values: vec![
                AggValue::Count(1),
                AggValue::SumInteger {
                    sum: i128::MAX,
                    count: 1,
                },
            ],
        },
    )];
    let before_groups = groups.clone();
    let mapping = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![None, Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![None, Some(DataType::Int64)],
    });
    let aggs = [
        AggSpec::count(),
        AggSpec {
            func: AggFunc::Sum,
            input: Some(1),
        },
    ];
    let result = run_with_aggs(
        DeltaPair::new(26, Arc::new(simple(&[2], &[1])), Arc::new(simple(&[], &[]))),
        &left,
        &right,
        &groups,
        mapping,
        &aggs,
    );
    assert!(matches!(result, CutDecision::Dense(_)));
    assert_eq!(groups, before_groups);
}

#[test]
fn absolute_i128_bound_guard_rejects_first_nonrepresentable_bound() {
    // Valid arrangement diffs are i64 and the end-to-end preparation bound
    // can reject earlier; exercise this production guard at its exact boundary.
    assert_eq!(
        super::super::stats::validate_i128_abs_bound(i128::MAX as u128),
        Ok(())
    );
    assert_eq!(
        super::super::stats::validate_i128_abs_bound(i128::MAX as u128 + 1),
        Err(DenseReason::NegativeOrOverflow)
    );
}
