use super::*;

#[test]
fn retracting_last_nonnull_value_emits_null_row_while_group_survives() {
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
    let left_seed = nullable(&[Some(10), None], &[1, 1]);
    let right_seed = nullable(&[Some(100), Some(200)], &[1, 1]);
    let left = arrangement(&left_seed, &[0]);
    let right = arrangement(&right_seed, &[0]);
    let mut dense = crate::ops::GroupAggregate::new(&[0], aggs.to_vec());
    dense
        .apply(&nullable(&[Some(10), Some(10), None, None], &[1, 1, 1, 1]))
        .unwrap();
    let prior = dense.export_state().unwrap();
    let expected = dense
        .apply(&nullable(&[Some(10), Some(10)], &[-1, -1]))
        .unwrap();
    let prepared = match prepare(PrepareInput {
        recognition: &mapping,
        aggregates: &aggs,
        inputs: DeltaPair::new(
            2,
            Arc::new(nullable(&[Some(10)], &[-1])),
            Arc::new(nullable(&[], &[])),
        ),
        state: OldState {
            left: &left,
            right: &right,
            groups: &group_map(&prior.groups),
        },
        consumer: ConsumerCaps::FactorizedAggregates,
    }) {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("last nonnull retraction rejected: {reason:?}"),
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
    assert_eq!(consumed.output.schema(), expected.schema());
    assert_eq!(actual_rows, expected_rows);
    assert_eq!(
        actual_rows.len(),
        2,
        "retraction and NULL-valued replacement"
    );
    let entry = consumed.changes[0].1.as_ref().unwrap();
    assert_eq!(entry.rows, 2);
    assert_eq!(entry.values[1], AggValue::Count(0));
    assert_eq!(entry.values[2], AggValue::SumInteger { sum: 0, count: 0 });
}
