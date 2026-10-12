use super::*;

#[test]
fn staged_group_changes_have_a_production_consumer() {
    let empty = Vec::<(Vec<u8>, GroupEntry)>::new();
    let output_schema = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let left = arrangement(&simple(&[], &[]), &[0]);
    let right = arrangement(&simple(&[], &[]), &[0]);
    let inputs = DeltaPair::new(
        1,
        Arc::new(simple(&[10, 20], &[1, 1])),
        Arc::new(simple(&[10, 20], &[1, 1])),
    );
    let aggs = [AggSpec::count(), AggSpec::sum(1), AggSpec::avg(1)];
    let recognized = Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: vec![None, Some(1), Some(1)],
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: vec![None, Some(DataType::Int64), Some(DataType::Int64)],
    });
    let CutDecision::Eligible(prepared) =
        run_with_aggs(inputs, &left, &right, &empty, recognized, &aggs)
    else {
        panic!("fixture should prepare");
    };
    let consumed =
        crate::ops::join::factorized::consume::consume(&prepared, output_schema, &[0], &aggs)
            .unwrap();
    assert_eq!(consumed.changes.len(), 1);
    assert_eq!(consumed.changes[0].1.rows, 4);
    // Joining left payloads 10 and 20 with two right rows yields these four
    // rows. The join/group key is 7 (simple() fixes it); 10/20 are aggregate
    // payload values, not output group keys.
    let dense_batch = simple(&[10, 10, 20, 20], &[1, 1, 1, 1]);
    let mut dense = crate::ops::GroupAggregate::new(&[0], aggs.to_vec());
    let expected = dense.apply(&dense_batch).unwrap();
    let mut actual_rows = typed_rows(&consumed.output);
    let mut expected_rows = typed_rows(&expected);
    actual_rows.sort();
    expected_rows.sort();
    assert_eq!(consumed.output.schema(), expected.schema());
    assert_eq!(actual_rows, expected_rows);
}

fn typed_rows(z: &ZSetBatch) -> Vec<(Vec<String>, i64)> {
    use arrow::array::Array;
    let diff = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len())
        .map(|i| {
            (
                z.batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(i) {
                            "NULL".to_string()
                        } else if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
                            values.value(i).to_string()
                        } else if let Some(values) =
                            column.as_any().downcast_ref::<arrow::array::Float64Array>()
                        {
                            format!("{:016x}", values.value(i).to_bits())
                        } else {
                            panic!("unexpected output type {:?}", column.data_type())
                        }
                    })
                    .collect(),
                diff.value(i),
            )
        })
        .collect()
}

#[test]
fn nullable_updates_match_dense_after_each_insert_retract_remove_and_recreate() {
    use hotlap_core::snapshot::GroupState;

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
    let mut left = arrangement(&nullable(&[Some(2), None], &[1, 1]), &[0]);
    let right_seed = nullable(&[Some(100), Some(200)], &[1, 1]);
    let right = arrangement(&right_seed, &[0]);
    let mut dense = crate::ops::GroupAggregate::new(&[0], aggs.to_vec());
    dense
        .apply(&nullable(&[Some(2), Some(2), None, None], &[1, 1, 1, 1]))
        .unwrap();

    let transitions: [(&[Option<i64>], &[i64]); 5] = [
        (&[Some(4)], &[1]),
        (&[None], &[-1]),
        (&[Some(2)], &[-1]),
        (&[Some(4)], &[-1]),
        (&[Some(9)], &[1]),
    ];
    let mut prior: GroupState = dense.export_state().unwrap();
    for (epoch, (values, weights)) in transitions.into_iter().enumerate() {
        let delta_left = nullable(values, weights);
        let mut joined_values = Vec::new();
        let mut joined_diffs = Vec::new();
        for (&value, &weight) in values.iter().zip(weights) {
            for _ in 0..2 {
                joined_values.push(value);
                joined_diffs.push(weight);
            }
        }
        let dense_delta = nullable(&joined_values, &joined_diffs);
        let expected = dense.apply(&dense_delta).unwrap();
        let decision = prepare(PrepareInput {
            recognition: &mapping,
            aggregates: &aggs,
            inputs: DeltaPair::new(
                epoch as u64 + 1,
                Arc::new(delta_left.clone()),
                Arc::new(nullable(&[], &[])),
            ),
            state: OldState {
                left: &left,
                right: &right,
                groups: &prior.groups,
            },
            consumer: ConsumerCaps::FactorizedAggregates,
        });
        let prepared = match decision {
            CutDecision::Eligible(prepared) => prepared,
            CutDecision::Dense(reason) => panic!("transition {epoch} rejected: {reason:?}"),
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
        assert_eq!(consumed.output.schema(), expected.schema(), "epoch {epoch}");
        assert_eq!(actual_rows, expected_rows, "epoch {epoch}");
        assert_eq!(consumed.changes, prepared.groups, "epoch {epoch}");

        let keys = KeyConverter::new(delta_left.schema().as_ref(), &[0]).unwrap();
        left.apply(&delta_left, &keys).unwrap();
        prior = GroupState {
            schema: None,
            groups: consumed.changes,
        };
    }

    let final_state = dense.export_state().unwrap();
    assert_eq!(prior.groups, final_state.groups);
    assert_eq!(prior.groups.len(), 1, "recreated group remains live");
    assert_eq!(prior.groups[0].1.rows, 2);
    let count_expr = match &prior.groups[0].1.values[1] {
        AggValue::Count(count) => *count,
        other => panic!("unexpected COUNT(expr) state: {other:?}"),
    };
    assert_eq!(count_expr, 2);
}
