use super::*;

fn nullable_batch(rows: &[(i64, Option<i64>, i64)]) -> ZSetBatch {
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

fn nullable_group(aggregates: Vec<AggSpec>) -> EngineCore {
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

#[test]
fn count_star_remains_provable_when_nullable_count_and_sum_history_drift() {
    use hotlap_core::snapshot::{AggValue, OperatorState};

    let aggregates = vec![
        AggSpec::count(),
        AggSpec {
            func: hotlap_core::AggFunc::Count,
            input: Some(1),
        },
        AggSpec::sum(1),
    ];
    let mut live = nullable_group(aggregates.clone());
    live.push(InputId(1), &aggregate_zset(&[(1, 0, 1)]))
        .unwrap();
    live.push(
        InputId(0),
        &nullable_batch(&[(1, Some(7), 1), (1, None, -1)]),
    )
    .unwrap();
    live.push(InputId(0), &nullable_batch(&[(1, None, 1)]))
        .unwrap();

    let checkpoint = live.checkpoint().unwrap();
    let group_entry = checkpoint.views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|state| match state {
            OperatorState::Group(group) => group.groups.first().map(|(_, entry)| entry),
            _ => None,
        })
        .unwrap();
    assert_eq!(group_entry.rows, 1);
    assert_eq!(group_entry.values[0], AggValue::Count(1));
    assert_eq!(group_entry.values[1], AggValue::Count(0));
    assert_eq!(
        group_entry.values[2],
        AggValue::SumInteger { sum: 0, count: 0 }
    );

    let mut restored = nullable_group(aggregates);
    restored.restore(&checkpoint).unwrap();
    assert_eq!(restored.checkpoint().unwrap(), checkpoint);

    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| count.set(0));
    let next = nullable_batch(&[(1, Some(3), 1)]);
    live.push(InputId(0), &next).unwrap();
    restored.push(InputId(0), &next).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| assert_eq!(count.get(), 2));
    assert_eq!(live.checkpoint().unwrap(), restored.checkpoint().unwrap());
}

#[test]
fn hidden_sum_count_mismatch_with_matching_root_output_falls_back_without_repair() {
    use hotlap_core::snapshot::OperatorState;

    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::sum(3)],
    };
    let mut live = EngineCore::new();
    live.register_input(InputId(0)).unwrap();
    live.register_input(InputId(1)).unwrap();
    live.build_view(ViewId(0), &plan).unwrap();
    live.tap_view(ViewId(0)).unwrap();
    live.push(InputId(0), &aggregate_zset(&[(7, 1, 1)]))
        .unwrap();
    live.push(InputId(1), &aggregate_zset(&[(7, 20, 1)]))
        .unwrap();
    live.take_changes(ViewId(0)).unwrap();
    let mut checkpoint = live.checkpoint().unwrap();
    let group = checkpoint.views[0]
        .operators
        .iter_mut()
        .flatten()
        .find_map(|state| match state {
            OperatorState::Group(group) => Some(group),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        group.groups[0].1.values[0],
        hotlap_core::snapshot::AggValue::SumInteger { sum: 20, count: 1 }
    );
    group.groups[0].1.values[0] = hotlap_core::snapshot::AggValue::SumInteger { sum: 20, count: 2 };

    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    let restored_checkpoint = restored.checkpoint().unwrap();
    let restored_entry = restored_checkpoint.views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|state| match state {
            OperatorState::Group(group) => Some(&group.groups[0].1.values[0]),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        restored_entry,
        &hotlap_core::snapshot::AggValue::SumInteger { sum: 20, count: 2 },
        "restore preserves hidden historical count bits"
    );

    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| count.set(0));
    let continuation = aggregate_zset(&[(7, 2, 1)]);
    live.push(InputId(0), &continuation).unwrap();
    restored.push(InputId(0), &continuation).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 1));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| assert_eq!(count.get(), 1));
    let original_changes = live.take_changes(ViewId(0)).unwrap();
    let restored_changes = restored.take_changes(ViewId(0)).unwrap();
    assert_eq!(original_changes.len(), 2);
    assert_eq!(restored_changes.len(), 2);
    assert_eq!(
        original_changes.diff().to_data(),
        restored_changes.diff().to_data()
    );
    assert_eq!(
        original_changes.batch.column(0).to_data(),
        restored_changes.batch.column(0).to_data()
    );
    assert_eq!(
        original_changes.batch.column(1).to_data(),
        restored_changes.batch.column(1).to_data()
    );
    let live_output = live.snapshot(ViewId(0)).unwrap();
    let restored_output = restored.snapshot(ViewId(0)).unwrap();
    assert_eq!(live_output.len(), 1);
    assert_eq!(restored_output.len(), 1);
    assert_eq!(
        live_output
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[40]
    );
    assert_eq!(
        live_output.batch.column(1).to_data(),
        restored_output.batch.column(1).to_data()
    );
}
