use super::*;

#[test]
fn integer_avg_null_and_count_slots_match_dense_and_restore_continuation() {
    let mut maintained = core();
    let mut dense = core();
    maintained
        .push(InputId(0), &pairs(&[(1, 0, 1), (1, 2, 1)]))
        .unwrap();
    dense
        .push(InputId(0), &pairs(&[(1, 0, 1), (1, 2, 1)]))
        .unwrap();
    let right = pairs(&[(1, 8, 1)]);
    maintained.push(InputId(1), &right).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(1), &right).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    same_batches_bitwise(
        &maintained.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same_avg_history(&maintained, &dense);

    // Input `v` is non-null in this fixture, so add a separate plan with a
    // nullable integer source to exercise COUNT(expr), COUNT(*), and AVG NN.
    let mut nullable = EngineCore::new();
    nullable.register_input(InputId(0)).unwrap();
    nullable.register_input(InputId(1)).unwrap();
    let nullable_plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![
            AggSpec::avg(1),
            AggSpec::count(),
            AggSpec {
                func: hotlap_core::plan::AggFunc::Count,
                input: Some(1),
            },
        ],
    };
    nullable.build_view(ViewId(0), &nullable_plan).unwrap();
    nullable.tap_view(ViewId(0)).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let nullable_batch = |values: Vec<Option<i64>>, diffs: Vec<i64>| {
        ZSetBatch::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1; values.len()])),
                    Arc::new(Int64Array::from(values)),
                ],
            )
            .unwrap(),
            Arc::new(Int64Array::from(diffs)),
        )
        .unwrap()
    };
    nullable
        .push(InputId(0), &nullable_batch(vec![None, Some(4)], vec![1, 1]))
        .unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    nullable.push(InputId(1), &pairs(&[(1, 9, 1)])).unwrap();
    let visible = nullable.snapshot(ViewId(0)).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));
    let avg = visible
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(avg.values(), &[4.0]);
    assert_eq!(avg.null_count(), 0);
    let counts = visible
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.values(), &[2]);
    let nonnull = visible
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(nonnull.values(), &[1]);

    let checkpoint = nullable.checkpoint().unwrap();
    let mut dense_nullable = EngineCore::new();
    dense_nullable.register_input(InputId(0)).unwrap();
    dense_nullable.register_input(InputId(1)).unwrap();
    dense_nullable
        .build_view(ViewId(0), &nullable_plan)
        .unwrap();
    dense_nullable.tap_view(ViewId(0)).unwrap();
    dense_nullable.restore(&checkpoint).unwrap();
    let remove_last_value = nullable_batch(vec![Some(4)], vec![-1]);
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    nullable.push(InputId(0), &remove_last_value).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense_nullable.push(InputId(0), &remove_last_value).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));
    same_batches_bitwise(
        &nullable.take_changes(ViewId(0)).unwrap(),
        &dense_nullable.take_changes(ViewId(0)).unwrap(),
    );
    same_avg_history(&nullable, &dense_nullable);
    let all_null_result = nullable.snapshot(ViewId(0)).unwrap();
    assert_eq!(all_null_result.batch.column(1).null_count(), 1);
    assert_eq!(
        all_null_result
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1]
    );
    assert_eq!(
        all_null_result
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0]
    );

    let mut all_null = EngineCore::new();
    all_null.register_input(InputId(0)).unwrap();
    all_null.register_input(InputId(1)).unwrap();
    all_null
        .build_view(
            ViewId(0),
            &Plan::GroupAggregate {
                input: Box::new(Plan::Join {
                    left: Box::new(Plan::Source(InputId(0))),
                    right: Box::new(Plan::Source(InputId(1))),
                    left_key: vec![0],
                    right_key: vec![0],
                }),
                key: vec![0],
                aggs: vec![
                    AggSpec::avg(1),
                    AggSpec::count(),
                    AggSpec {
                        func: hotlap_core::plan::AggFunc::Count,
                        input: Some(1),
                    },
                ],
            },
        )
        .unwrap();
    all_null.tap_view(ViewId(0)).unwrap();
    let mut all_null_dense = EngineCore::new();
    all_null_dense.register_input(InputId(0)).unwrap();
    all_null_dense.register_input(InputId(1)).unwrap();
    all_null_dense
        .build_view(ViewId(0), &nullable_plan)
        .unwrap();
    all_null_dense.tap_view(ViewId(0)).unwrap();
    all_null
        .push(InputId(0), &nullable_batch(vec![None], vec![1]))
        .unwrap();
    all_null_dense
        .push(InputId(0), &nullable_batch(vec![None], vec![1]))
        .unwrap();
    all_null.push(InputId(1), &pairs(&[(1, 9, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    all_null_dense
        .push(InputId(1), &pairs(&[(1, 9, 1)]))
        .unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    same_batches_bitwise(
        &all_null.snapshot(ViewId(0)).unwrap(),
        &all_null_dense.snapshot(ViewId(0)).unwrap(),
    );
    same_avg_history(&all_null, &all_null_dense);
    let empty_avg = all_null.snapshot(ViewId(0)).unwrap();
    assert_eq!(empty_avg.batch.column(1).null_count(), 1);
    assert_eq!(
        empty_avg
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1]
    );
    assert_eq!(
        empty_avg
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0]
    );
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    let retract_null = nullable_batch(vec![None], vec![-1]);
    all_null.push(InputId(0), &retract_null).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    all_null_dense.push(InputId(0), &retract_null).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 0));
    same_batches_bitwise(
        &all_null.take_changes(ViewId(0)).unwrap(),
        &all_null_dense.take_changes(ViewId(0)).unwrap(),
    );
    assert!(all_null.snapshot(ViewId(0)).unwrap().is_empty());
    same_avg_history(&all_null, &all_null_dense);
}
