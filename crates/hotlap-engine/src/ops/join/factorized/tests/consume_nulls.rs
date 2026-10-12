use super::*;

#[test]
fn first_all_null_group_emits_count_star_and_null_sum_avg() {
    let s = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let make = |values: &[Option<i64>], diffs: &[i64]| {
        zset(
            s.clone(),
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
    let left = arrangement(&make(&[], &[]), &[0]);
    let right = arrangement(&make(&[], &[]), &[0]);
    let delta_left = make(&[None], &[1]);
    let delta_right = make(&[Some(99)], &[1]);
    let mut dense = crate::ops::GroupAggregate::new(&[0], aggs.to_vec());
    let expected = dense.apply(&make(&[None], &[1])).unwrap();
    let prepared = match prepare(PrepareInput {
        recognition: &mapping,
        aggregates: &aggs,
        inputs: DeltaPair::new(1, Arc::new(delta_left), Arc::new(delta_right)),
        state: OldState {
            left: &left,
            right: &right,
            groups: &[],
        },
        consumer: ConsumerCaps::FactorizedAggregates,
    }) {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("all-null insertion rejected: {reason:?}"),
    };
    let consumed =
        crate::ops::join::factorized::consume::consume(&prepared, s, &[0], &aggs).unwrap();
    assert_eq!(consumed.output.schema(), expected.schema());
    assert_eq!(typed_rows(&consumed.output), typed_rows(&expected));
    assert_eq!(
        typed_rows(&consumed.output),
        vec![(
            vec![
                "7".into(),
                "1".into(),
                "0".into(),
                "NULL".into(),
                "NULL".into()
            ],
            1
        )]
    );
}
