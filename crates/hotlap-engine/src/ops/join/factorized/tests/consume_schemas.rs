use super::*;

#[test]
fn consumer_matches_dense_with_unequal_side_schemas() {
    use arrow::array::StringArray;

    let left_schema = schema(&[("lk", DataType::Int64), ("v", DataType::Int64)]);
    let right_schema = schema(&[("rk", DataType::Int64), ("label", DataType::Utf8)]);
    let joined_schema = schema(&[
        ("lk", DataType::Int64),
        ("v", DataType::Int64),
        ("rk", DataType::Int64),
        ("label", DataType::Utf8),
    ]);
    let left_batch = |keys: &[i64], values: &[i64], diffs: &[i64]| {
        zset(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let right_batch = |keys: &[i64], labels: &[&str], diffs: &[i64]| {
        zset(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(StringArray::from(labels.to_vec())),
            ],
            diffs,
        )
    };
    let joined_batch = |lk: &[i64], values: &[i64], rk: &[i64], labels: &[&str], diffs: &[i64]| {
        zset(
            joined_schema.clone(),
            vec![
                Arc::new(Int64Array::from(lk.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(Int64Array::from(rk.to_vec())),
                Arc::new(StringArray::from(labels.to_vec())),
            ],
            diffs,
        )
    };

    let old_left = left_batch(&[1, 2], &[10, 20], &[1, 1]);
    let old_right = right_batch(&[1, 2], &["a", "b"], &[1, 1]);
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
    dense
        .apply(&joined_batch(
            &[1, 2],
            &[10, 20],
            &[1, 2],
            &["a", "b"],
            &[1, 1],
        ))
        .unwrap();
    let prior = dense.export_state().unwrap();
    let delta_left = left_batch(&[1], &[11], &[1]);
    let expected = dense
        .apply(&joined_batch(&[1], &[11], &[1], &["a"], &[1]))
        .unwrap();
    let prepared = match prepare(PrepareInput {
        recognition: &mapping,
        aggregates: &aggs,
        inputs: DeltaPair::new(
            3,
            Arc::new(delta_left),
            Arc::new(right_batch(&[], &[], &[])),
        ),
        state: OldState {
            left: &left,
            right: &right,
            groups: &group_map(&prior.groups),
        },
        consumer: ConsumerCaps::FactorizedAggregates,
    }) {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("unequal valid side schemas rejected: {reason:?}"),
    };
    let consumed =
        crate::ops::join::factorized::consume::consume(&prepared, joined_schema, &[0], &aggs)
            .unwrap();
    let mut actual_rows = typed_rows(&consumed.output);
    let mut expected_rows = typed_rows(&expected);
    actual_rows.sort();
    expected_rows.sort();
    assert_eq!(consumed.output.schema(), expected.schema());
    assert_eq!(actual_rows, expected_rows);
}
