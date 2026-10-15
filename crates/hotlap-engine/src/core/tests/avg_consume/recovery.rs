use super::*;

fn signed_nullable_pairs(rows: &[(i64, Option<i64>, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    ZSetBatch::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.2).collect::<Vec<_>>(),
        )),
    )
    .unwrap()
}

fn nullable_value_core(aggregates: Vec<AggSpec>) -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupAggregate {
            input: Box::new(Plan::Join {
                left: Box::new(Plan::Source(InputId(0))),
                right: Box::new(Plan::Source(InputId(1))),
                left_key: vec![0],
                right_key: vec![0],
            }),
            key: vec![0],
            aggs: aggregates,
        },
    )
    .unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core
}

fn avg_counts_and_sum_bits(core: &EngineCore) -> Vec<(i64, u64)> {
    use hotlap_core::snapshot::{AggValue, OperatorState};
    core.checkpoint().unwrap().views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|operator| match operator {
            OperatorState::Group(state) => Some(
                state
                    .groups
                    .iter()
                    .flat_map(|(_, entry)| entry.values.iter())
                    .filter_map(|value| match value {
                        AggValue::Avg { sum, count, .. } => Some((*count, sum.to_bits())),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        })
        .expect("view has GroupAggregate")
}

#[test]
fn v1_restore_preserves_inexact_integer_avg_history_and_falls_back() {
    let mut dense = core();
    dense.push(InputId(1), &pairs(&[(1, 0, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense
        .push(InputId(0), &pairs(&[(1, 1_i64 << 53, 1)]))
        .unwrap();
    dense.push(InputId(0), &pairs(&[(1, 1, 1)])).unwrap();
    dense
        .push(InputId(0), &pairs(&[(1, -(1_i64 << 53), 1)]))
        .unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    let bits = avg_sum_bits(&dense);
    assert_eq!(bits.len(), 1);
    let exact_sum_as_f64 = 1.0_f64;
    assert_ne!(
        bits[0],
        exact_sum_as_f64.to_bits(),
        "fixture must produce inexact Dense AVG history"
    );
    let checkpoint = dense.checkpoint().unwrap();

    let mut restored = core();
    restored.restore(&checkpoint).unwrap();
    assert_eq!(
        avg_sum_bits(&restored),
        bits,
        "restore must preserve authoritative AVG.sum bits"
    );
    assert_eq!(restored.checkpoint().unwrap(), checkpoint);

    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    let delta = pairs(&[(1, 1, -1), (1, 3, 1)]);
    restored.push(InputId(0), &delta).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(0), &delta).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 0));
    same_batches_bitwise(
        &restored.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    assert_eq!(avg_sum_bits(&restored), avg_sum_bits(&dense));
}

#[test]
fn v1_restore_leaves_float_avg_history_to_dense_semantics() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::avg(1)],
    };
    let mut engine = EngineCore::new();
    engine.register_input(InputId(0)).unwrap();
    engine.register_input(InputId(1)).unwrap();
    engine.build_view(ViewId(0), &plan).unwrap();
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Float64, true),
    ]));
    let right_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    engine
        .push(
            InputId(0),
            &ZSetBatch::new(
                RecordBatch::try_new(
                    left_schema,
                    vec![
                        Arc::new(Int64Array::from(vec![1])),
                        Arc::new(arrow::array::Float64Array::from(vec![Some(1.5)])),
                    ],
                )
                .unwrap(),
                Arc::new(Int64Array::from(vec![1])),
            )
            .unwrap(),
        )
        .unwrap();
    engine
        .push(
            InputId(1),
            &ZSetBatch::new(
                RecordBatch::try_new(right_schema, vec![Arc::new(Int64Array::from(vec![1]))])
                    .unwrap(),
                Arc::new(Int64Array::from(vec![1])),
            )
            .unwrap(),
        )
        .unwrap();
    let checkpoint = engine.checkpoint().unwrap();
    let mut restored = EngineCore::new();
    restored.register_input(InputId(0)).unwrap();
    restored.register_input(InputId(1)).unwrap();
    restored.build_view(ViewId(0), &plan).unwrap();
    restored.restore(&checkpoint).unwrap();
    assert_eq!(restored.checkpoint().unwrap(), checkpoint);
}

#[test]
fn v1_restore_accepts_live_avg_history_after_signed_zero_row_group_recreation() {
    let right = pairs(&[(1, 0, 1)]);
    let mut live = nullable_value_core(vec![AggSpec::avg(1)]);
    live.push(InputId(1), &right).unwrap();
    // Input order matters: the negative NULL removes the group's last physical
    // joined row after the positive non-NULL row has been folded.
    live.push(
        InputId(0),
        &signed_nullable_pairs(&[(1, Some(7), 1), (1, None, -1)]),
    )
    .unwrap();
    live.push(InputId(0), &signed_nullable_pairs(&[(1, None, 1)]))
        .unwrap();
    let authoritative_history = avg_counts_and_sum_bits(&live);
    assert_eq!(authoritative_history.len(), 1);
    assert_eq!(authoritative_history[0].0, 0);

    let checkpoint = live.checkpoint().unwrap();

    let mut restored = nullable_value_core(vec![AggSpec::avg(1)]);
    restored
        .restore(&checkpoint)
        .expect("live v1 state after a signed zero-row group removal is valid");
    assert_eq!(avg_counts_and_sum_bits(&restored), authoritative_history);
    assert_eq!(
        avg_counts_and_sum_bits(&restored),
        avg_counts_and_sum_bits(&live),
        "restored history is checked against the unmodified live oracle"
    );
    assert_eq!(
        live.checkpoint().unwrap(),
        checkpoint,
        "oracle is not mutated to manufacture the restore case"
    );
    assert_eq!(restored.checkpoint().unwrap(), checkpoint);

    let mut dense_reference = nullable_value_core(vec![AggSpec::avg(1)]);
    dense_reference.restore(&checkpoint).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense_reference
        .push(InputId(0), &signed_nullable_pairs(&[(1, Some(2), 1)]))
        .unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));

    let continuation = signed_nullable_pairs(&[(1, Some(2), 1)]);
    live.push(InputId(0), &continuation).unwrap();
    restored.push(InputId(0), &continuation).unwrap();
    same_batches_bitwise(
        &live.take_changes(ViewId(0)).unwrap(),
        &restored.take_changes(ViewId(0)).unwrap(),
    );
    same_batches_bitwise(
        &live.snapshot(ViewId(0)).unwrap(),
        &dense_reference.snapshot(ViewId(0)).unwrap(),
    );
    assert_eq!(
        avg_counts_and_sum_bits(&live),
        avg_counts_and_sum_bits(&dense_reference)
    );
    assert_eq!(
        avg_counts_and_sum_bits(&restored),
        avg_counts_and_sum_bits(&live)
    );

    let mut zero_value_live = nullable_value_core(vec![AggSpec::avg(1)]);
    zero_value_live.push(InputId(1), &right).unwrap();
    zero_value_live
        .push(
            InputId(0),
            &signed_nullable_pairs(&[(1, Some(0), 1), (1, None, -1)]),
        )
        .unwrap();
    zero_value_live
        .push(InputId(0), &signed_nullable_pairs(&[(1, None, 1)]))
        .unwrap();
    let zero_history = avg_counts_and_sum_bits(&zero_value_live);
    assert_eq!(zero_history, vec![(0, 0.0_f64.to_bits())]);
    let zero_checkpoint = zero_value_live.checkpoint().unwrap();
    let mut zero_restored = nullable_value_core(vec![AggSpec::avg(1)]);
    zero_restored.restore(&zero_checkpoint).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| count.set(0));
    let zero_update = signed_nullable_pairs(&[(1, Some(0), 1)]);
    let mut zero_dense = nullable_value_core(vec![AggSpec::avg(1)]);
    zero_dense.restore(&zero_checkpoint).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    zero_dense.push(InputId(0), &zero_update).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    zero_restored.push(InputId(0), &zero_update).unwrap();
    assert_eq!(
        avg_counts_and_sum_bits(&zero_restored),
        vec![(1, 0.0_f64.to_bits())]
    );
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| assert_eq!(count.get(), 1));
    assert_eq!(
        avg_counts_and_sum_bits(&zero_restored),
        avg_counts_and_sum_bits(&zero_dense)
    );
    let mut zero_original = nullable_value_core(vec![AggSpec::avg(1)]);
    zero_original.restore(&zero_checkpoint).unwrap();
    zero_original.push(InputId(0), &zero_update).unwrap();
    assert_eq!(
        avg_counts_and_sum_bits(&zero_original),
        avg_counts_and_sum_bits(&zero_dense)
    );
}

#[test]
fn v1_restore_accepts_live_integer_history_after_signed_zero_row_group_recreation() {
    use hotlap_core::snapshot::{AggValue, OperatorState};

    let right = pairs(&[(1, 0, 1)]);
    let aggregates = vec![
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(1),
        },
        AggSpec::sum(1),
    ];
    let mut live = nullable_value_core(aggregates.clone());
    live.push(InputId(1), &right).unwrap();
    live.push(
        InputId(0),
        &signed_nullable_pairs(&[(1, Some(7), 1), (1, None, -1)]),
    )
    .unwrap();
    live.push(InputId(0), &signed_nullable_pairs(&[(1, None, 1)]))
        .unwrap();
    let checkpoint = live.checkpoint().unwrap();
    let history = checkpoint.views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|operator| match operator {
            OperatorState::Group(state) => state.groups.first().map(|(_, entry)| entry.clone()),
            _ => None,
        })
        .expect("live group entry");
    assert_eq!(history.values[0], AggValue::Count(0));
    assert_eq!(history.values[1], AggValue::SumInteger { sum: 0, count: 0 });

    let mut restored = nullable_value_core(aggregates);
    restored
        .restore(&checkpoint)
        .expect("valid live v1 checkpoint");
    assert_eq!(restored.checkpoint().unwrap(), checkpoint);

    let continuation = signed_nullable_pairs(&[(1, Some(2), 1)]);
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| count.set(0));
    live.push(InputId(0), &continuation).unwrap();
    restored.push(InputId(0), &continuation).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| assert_eq!(count.get(), 2));
    same_batches_bitwise(
        &live.take_changes(ViewId(0)).unwrap(),
        &restored.take_changes(ViewId(0)).unwrap(),
    );
    same_batches_bitwise(
        &restored.snapshot(ViewId(0)).unwrap(),
        &live.snapshot(ViewId(0)).unwrap(),
    );
    assert_eq!(restored.checkpoint().unwrap(), live.checkpoint().unwrap());

    let mut resync_live = nullable_value_core(vec![
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(1),
        },
        AggSpec::sum(1),
    ]);
    resync_live.restore(&checkpoint).unwrap();
    let mut resync_restored = nullable_value_core(vec![
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(1),
        },
        AggSpec::sum(1),
    ]);
    resync_restored.restore(&checkpoint).unwrap();
    let delete_old_value = signed_nullable_pairs(&[(1, Some(7), -1)]);
    resync_live.push(InputId(0), &delete_old_value).unwrap();
    resync_restored.push(InputId(0), &delete_old_value).unwrap();
    let insert_resynchronized_value = signed_nullable_pairs(&[(1, Some(3), 1)]);
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| count.set(0));
    resync_live
        .push(InputId(0), &insert_resynchronized_value)
        .unwrap();
    resync_restored
        .push(InputId(0), &insert_resynchronized_value)
        .unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| assert_eq!(count.get(), 0));
    same_batches_bitwise(
        &resync_restored.take_changes(ViewId(0)).unwrap(),
        &resync_live.take_changes(ViewId(0)).unwrap(),
    );
    assert_eq!(
        resync_restored.checkpoint().unwrap(),
        resync_live.checkpoint().unwrap()
    );
}

#[test]
fn mixed_avg_first_live_signed_history_restores_and_continues() {
    let aggs = vec![
        AggSpec::avg(1),
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(1),
        },
        AggSpec::sum(1),
    ];
    let mut live = nullable_value_core(aggs.clone());
    live.push(InputId(1), &pairs(&[(1, 0, 1)])).unwrap();
    live.push(
        InputId(0),
        &signed_nullable_pairs(&[(1, Some(7), 1), (1, None, -1)]),
    )
    .unwrap();
    live.push(InputId(0), &signed_nullable_pairs(&[(1, None, 1)]))
        .unwrap();
    let checkpoint = live.checkpoint().unwrap();
    let pre_restore_history = avg_counts_and_sum_bits(&live);
    let mut restored = nullable_value_core(aggs);
    restored.restore(&checkpoint).unwrap();
    assert_eq!(avg_counts_and_sum_bits(&restored), pre_restore_history);
    let continuation = signed_nullable_pairs(&[(1, Some(2), 1)]);
    live.push(InputId(0), &continuation).unwrap();
    restored.push(InputId(0), &continuation).unwrap();
    same_batches_bitwise(
        &live.take_changes(ViewId(0)).unwrap(),
        &restored.take_changes(ViewId(0)).unwrap(),
    );
    same_batches_bitwise(
        &live.snapshot(ViewId(0)).unwrap(),
        &restored.snapshot(ViewId(0)).unwrap(),
    );
    assert_eq!(restored.checkpoint().unwrap(), live.checkpoint().unwrap());
}

#[test]
fn integer_sum_endpoint_history_mismatch_is_preserved_and_continues() {
    let mut live = nullable_value_core(vec![AggSpec::sum(1)]);
    live.push(InputId(1), &pairs(&[(1, 0, 1)])).unwrap();
    live.push(
        InputId(0),
        &signed_nullable_pairs(&[(1, Some(7), 1), (1, None, -1)]),
    )
    .unwrap();
    live.push(InputId(0), &signed_nullable_pairs(&[(1, None, 1)]))
        .unwrap();
    let checkpoint = live.checkpoint().unwrap();
    let mut restored = nullable_value_core(vec![AggSpec::sum(1)]);
    restored.restore(&checkpoint).unwrap();
    assert_eq!(restored.checkpoint().unwrap(), checkpoint);
    let continuation = signed_nullable_pairs(&[(1, Some(2), 1)]);
    live.push(InputId(0), &continuation).unwrap();
    restored.push(InputId(0), &continuation).unwrap();
    same_batches_bitwise(
        &live.take_changes(ViewId(0)).unwrap(),
        &restored.take_changes(ViewId(0)).unwrap(),
    );
    assert_eq!(restored.checkpoint().unwrap(), live.checkpoint().unwrap());
}

#[test]
fn avg_only_recreated_group_restores_and_continues_after_dense_zero_row_removal() {
    let mut live = nullable_value_core(vec![AggSpec::avg(1)]);
    live.push(InputId(1), &pairs(&[(1, 0, 1)])).unwrap();
    live.push(
        InputId(0),
        &signed_nullable_pairs(&[(1, Some(7), 1), (1, None, -1)]),
    )
    .unwrap();
    live.push(InputId(0), &signed_nullable_pairs(&[(1, None, 1)]))
        .unwrap();
    let checkpoint = live.checkpoint().unwrap();
    let mut restored = nullable_value_core(vec![AggSpec::avg(1)]);
    restored.restore(&checkpoint).unwrap();
    assert_eq!(
        avg_counts_and_sum_bits(&restored),
        avg_counts_and_sum_bits(&live)
    );
    let continuation = signed_nullable_pairs(&[(1, Some(2), 1)]);
    live.push(InputId(0), &continuation).unwrap();
    restored.push(InputId(0), &continuation).unwrap();
    same_batches_bitwise(
        &live.take_changes(ViewId(0)).unwrap(),
        &restored.take_changes(ViewId(0)).unwrap(),
    );
    assert_eq!(
        avg_counts_and_sum_bits(&live),
        avg_counts_and_sum_bits(&restored)
    );
}

#[test]
fn avg_certificate_rejects_old_nn_mismatch_even_when_zero_sum_bits_match() {
    let right = pairs(&[(1, 0, 1)]);
    let mut live = nullable_value_core(vec![AggSpec::avg(1)]);
    live.push(InputId(1), &right).unwrap();
    live.push(
        InputId(0),
        &signed_nullable_pairs(&[(1, Some(7), 1), (1, None, -1)]),
    )
    .unwrap();
    live.push(InputId(0), &signed_nullable_pairs(&[(1, None, 1)]))
        .unwrap();
    let history = avg_counts_and_sum_bits(&live);
    assert_eq!(history, vec![(0, 0.0_f64.to_bits())]);
    let checkpoint = live.checkpoint().unwrap();

    let mut restored = nullable_value_core(vec![AggSpec::avg(1)]);
    restored.restore(&checkpoint).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| count.set(0));
    let update = signed_nullable_pairs(&[(1, Some(0), 1)]);
    restored.push(InputId(0), &update).unwrap();
    assert_eq!(
        avg_counts_and_sum_bits(&restored),
        vec![(1, 0.0_f64.to_bits())],
        "Dense row history has one non-null value; certification must not derive count from endpoint NN"
    );
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 0));
    assert_eq!(
        crate::core::graph::FLAT_ROWS_CUTS.with(|count| count.get()),
        1,
        "stale old NN must use the ordinary row consumer"
    );

    let mut dense = nullable_value_core(vec![AggSpec::avg(1)]);
    dense.restore(&checkpoint).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(0), &update).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    assert_eq!(
        avg_counts_and_sum_bits(&restored),
        avg_counts_and_sum_bits(&dense)
    );
}

#[test]
fn v1_restore_preserves_signed_zero_avg_history_and_keeps_it_on_dense_fallback() {
    use arrow::array::Float64Array;
    use hotlap_core::snapshot::{AggValue, OperatorState};

    let mut engine = core();
    engine.push(InputId(1), &pairs(&[(1, 0, 1)])).unwrap();
    engine.push(InputId(0), &pairs(&[(1, 7, 1)])).unwrap();
    let mut snapshot = engine.checkpoint().unwrap();
    let group = snapshot.views[0]
        .operators
        .iter_mut()
        .flatten()
        .find_map(|operator| match operator {
            OperatorState::Group(state) => Some(state),
            _ => None,
        })
        .unwrap();
    let sum = group.groups[0]
        .1
        .values
        .iter_mut()
        .find_map(|value| match value {
            AggValue::Avg { sum, .. } => Some(sum),
            _ => None,
        })
        .unwrap();
    *sum = -0.0;
    let output =
        crate::core::ipc::decode_zset(snapshot.views[0].output.as_ref().expect("tapped output"))
            .unwrap();
    let mut columns = (0..output.batch.num_columns())
        .map(|index| output.batch.column(index).clone())
        .collect::<Vec<_>>();
    *columns.last_mut().unwrap() = Arc::new(Float64Array::from(vec![-0.0]));
    let coherent_output = ZSetBatch::new(
        RecordBatch::try_new(output.schema(), columns).unwrap(),
        output.diff().clone(),
    )
    .unwrap();
    snapshot.views[0].output = Some(crate::core::ipc::encode_zset(&coherent_output).unwrap());
    let before = snapshot.clone();
    let mut maintained = core();
    maintained.restore(&snapshot).unwrap();
    let mut dense = core();
    dense.restore(&snapshot).unwrap();
    assert_eq!(avg_sum_bits(&maintained), vec![(-0.0_f64).to_bits()]);
    assert_eq!(maintained.checkpoint().unwrap(), before);

    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    let delta = pairs(&[(1, 8, 1)]);
    maintained.push(InputId(0), &delta).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(0), &delta).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 0));
    same_batches_bitwise(
        &maintained.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    assert_eq!(avg_sum_bits(&maintained), avg_sum_bits(&dense));
}
