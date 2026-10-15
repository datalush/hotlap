use super::*;

#[path = "avg_bench/measurement.rs"]
mod measurement;
#[path = "avg_bench/measurement/memory.rs"]
mod memory;
#[path = "avg_bench/measurement/recovery.rs"]
mod recovery;

type AvgHistory = Vec<(Vec<u8>, Vec<(u64, i64, i64, i64, i64)>)>;

fn nullable_rows(keys: &[i64], values: &[Option<i64>], diffs: &[i64]) -> ZSetBatch {
    assert_eq!(keys.len(), values.len());
    assert_eq!(keys.len(), diffs.len());
    let key_hi = keys.iter().map(|key| key.div_euclid(2)).collect::<Vec<_>>();
    let key_lo = keys.iter().map(|key| key.rem_euclid(2)).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("join_key_hi", DataType::Int64, false),
        Field::new("join_key_lo", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(key_hi)) as ArrayRef,
            Arc::new(Int64Array::from(key_lo)) as ArrayRef,
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

#[derive(Clone, Copy)]
struct AvgCase {
    name: &'static str,
    fanout: usize,
    project_subset: bool,
    avg_left: bool,
    repeated: bool,
    with_mixed_flat_view: bool,
    update: Update,
    all_null: bool,
}

fn avg_plan(case: AvgCase) -> Plan {
    let join = Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![0, 1],
        right_key: vec![0, 1],
    };
    let (input, group, avg_input) = if case.project_subset {
        // Reordered payload columns and duplicate group-key alias exercise the
        // actual normalized projected join schema, not a fixture label.
        (
            Plan::Project {
                input: Box::new(join),
                cols: vec![3, 0, 5, 2],
            },
            vec![1, 0, 1],
            if case.avg_left { 3 } else { 2 },
        )
    } else {
        (join, vec![0], if case.avg_left { 2 } else { 5 })
    };
    let mut aggs = vec![AggSpec::avg(avg_input)];
    if case.repeated {
        aggs.push(AggSpec::avg(avg_input));
    }
    Plan::GroupAggregate {
        input: Box::new(input),
        key: group,
        aggs,
    }
}

fn avg_core(case: AvgCase) -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(ViewId(0), &avg_plan(case)).unwrap();
    if case.with_mixed_flat_view {
        core.build_view(
            ViewId(1),
            &Plan::Join {
                left: Box::new(Plan::Source(InputId(0))),
                right: Box::new(Plan::Source(InputId(1))),
                left_key: vec![0, 1],
                right_key: vec![0, 1],
            },
        )
        .unwrap();
    }
    core.tap_view(ViewId(0)).unwrap();
    if case.with_mixed_flat_view {
        core.tap_view(ViewId(1)).unwrap();
    }
    core
}

fn history_signature(core: &EngineCore) -> AvgHistory {
    use hotlap_core::snapshot::{AggValue, OperatorState};
    let checkpoint = core.checkpoint().unwrap();
    let state = checkpoint.views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|operator| match operator {
            OperatorState::Group(state) => Some(state),
            _ => None,
        })
        .unwrap();
    state
        .groups
        .iter()
        .map(|(key, entry)| {
            let values = entry
                .values
                .iter()
                .filter_map(|value| match value {
                    AggValue::Avg {
                        sum,
                        count,
                        nan,
                        pos_inf,
                        neg_inf,
                    } => Some((sum.to_bits(), *count, *nan, *pos_inf, *neg_inf)),
                    _ => None,
                })
                .collect();
            (key.clone(), values)
        })
        .collect()
}

fn assert_batches_same(left: &[ZSetBatch], right: &[ZSetBatch]) {
    use arrow::array::Float64Array;

    assert_eq!(left.len(), right.len());
    for (left, right) in left.iter().zip(right) {
        assert_eq!(left.schema(), right.schema());
        assert_eq!(left.len(), right.len(), "typed output row count differs");
        assert_eq!(
            left.diff()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            right
                .diff()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
        for (left_column, right_column) in left.batch.columns().iter().zip(right.batch.columns()) {
            match (
                left_column.as_any().downcast_ref::<Float64Array>(),
                right_column.as_any().downcast_ref::<Float64Array>(),
            ) {
                (Some(left), Some(right)) => {
                    assert_eq!(left.len(), right.len());
                    assert_eq!(left.null_count(), right.null_count());
                    for row in 0..left.len() {
                        assert_eq!(left.is_null(row), right.is_null(row));
                        if !left.is_null(row) {
                            assert_eq!(left.value(row).to_bits(), right.value(row).to_bits());
                        }
                    }
                }
                (None, None) => assert_eq!(left_column.to_data(), right_column.to_data()),
                _ => panic!("output column types differ"),
            }
        }
    }
}

fn push_and_compare(
    auto: &mut EngineCore,
    dense: &mut EngineCore,
    case: AvgCase,
    input: InputId,
    delta: &ZSetBatch,
) -> bool {
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| count.set(0));
    auto.push(input, delta).unwrap();
    let factorized = crate::core::graph::FACTORIZED_CUTS.with(Cell::get);
    let row_cuts = crate::core::graph::FLAT_ROWS_CUTS.with(Cell::get);
    let mode = ModeGuard::new(true);
    dense.push(input, delta).unwrap();
    drop(mode);
    let mut auto_changes = vec![auto.take_changes(ViewId(0)).unwrap()];
    let mut dense_changes = vec![dense.take_changes(ViewId(0)).unwrap()];
    if case.with_mixed_flat_view {
        auto_changes.push(auto.take_changes(ViewId(1)).unwrap());
        dense_changes.push(dense.take_changes(ViewId(1)).unwrap());
    }
    assert_batches_same(&auto_changes, &dense_changes);
    let mut auto_snapshots = vec![auto.snapshot(ViewId(0)).unwrap()];
    let mut dense_snapshots = vec![dense.snapshot(ViewId(0)).unwrap()];
    if case.with_mixed_flat_view {
        auto_snapshots.push(auto.snapshot(ViewId(1)).unwrap());
        dense_snapshots.push(dense.snapshot(ViewId(1)).unwrap());
    }
    assert_batches_same(&auto_snapshots, &dense_snapshots);
    assert_eq!(history_signature(auto), history_signature(dense));
    assert!(
        factorized > 0,
        "{} must prove an eligible AVG cut",
        case.name
    );
    assert_eq!(row_cuts, u64::from(case.with_mixed_flat_view));
    true
}

#[test]
fn eligible_avg_fixture_contracts_prove_runtime_path_and_dense_parity() {
    let cases = [
        AvgCase {
            name: "avg-left-small-replace",
            fanout: 1,
            project_subset: false,
            avg_left: true,
            repeated: false,
            with_mixed_flat_view: false,
            update: Update::ReplaceLeft,
            all_null: false,
        },
        AvgCase {
            name: "avg-right-high-fanout-append",
            fanout: 16,
            project_subset: false,
            avg_left: false,
            repeated: false,
            with_mixed_flat_view: false,
            update: Update::AppendRight,
            all_null: false,
        },
        AvgCase {
            name: "avg-left-256-fanout-replace",
            fanout: 256,
            project_subset: false,
            avg_left: true,
            repeated: false,
            with_mixed_flat_view: false,
            update: Update::ReplaceLeft,
            all_null: false,
        },
        AvgCase {
            name: "avg-right-projected-repeated-mixed-flat",
            fanout: 4,
            project_subset: true,
            avg_left: false,
            repeated: true,
            with_mixed_flat_view: true,
            update: Update::ReplaceRight,
            all_null: false,
        },
        AvgCase {
            name: "avg-all-null-right-to-value",
            fanout: 1,
            project_subset: false,
            avg_left: false,
            repeated: false,
            with_mixed_flat_view: false,
            update: Update::ReplaceRight,
            all_null: true,
        },
    ];
    for case in cases {
        assert!(!case.name.is_empty());
        let mut auto = avg_core(case);
        let mut dense = avg_core(case);
        // Every joined key is composite. Small bounded integers stay inside the
        // certified f64 exact-integer domain while values vary by key and row.
        let mut keys = Vec::new();
        let mut left_values = Vec::new();
        let mut right_values = Vec::new();
        for key in 0..4_i64 {
            for row in 0..case.fanout {
                keys.push(key);
                left_values.push(Some((row % 9 + 2) as i64));
                right_values.push(if case.all_null || row == 1 || row != 0 && row % 7 == 0 {
                    None
                } else {
                    Some((row % 11 + 1) as i64)
                });
            }
        }
        for key in 0..4 {
            let start = key * case.fanout;
            let end = start + case.fanout;
            let left = nullable_rows(
                &keys[start..end],
                &left_values[start..end],
                &vec![1; case.fanout],
            );
            let right = nullable_rows(
                &keys[start..end],
                &right_values[start..end],
                &vec![1; case.fanout],
            );
            auto.push(InputId(0), &left).unwrap();
            dense.push(InputId(0), &left).unwrap();
            assert_eq!(history_signature(&auto), history_signature(&dense));
            auto.push(InputId(1), &right).unwrap();
            let auto_group_changes = auto.take_changes(ViewId(0)).unwrap();
            let _ = dense.take_changes(ViewId(0)).unwrap();
            let auto_flat_changes = case
                .with_mixed_flat_view
                .then(|| auto.take_changes(ViewId(1)).unwrap());
            if case.with_mixed_flat_view {
                let _ = dense.take_changes(ViewId(1)).unwrap();
            }
            {
                let mode = ModeGuard::new(true);
                dense.push(InputId(1), &right).unwrap();
                drop(mode);
            }
            assert_batches_same(
                &[auto_group_changes],
                &[dense.take_changes(ViewId(0)).unwrap()],
            );
            if case.with_mixed_flat_view {
                assert_batches_same(
                    &[auto_flat_changes.unwrap()],
                    &[dense.take_changes(ViewId(1)).unwrap()],
                );
            }
            assert_batches_same(
                &[auto.snapshot(ViewId(0)).unwrap()],
                &[dense.snapshot(ViewId(0)).unwrap()],
            );
            assert_eq!(history_signature(&auto), history_signature(&dense));
        }
        let _ = auto.take_changes(ViewId(0)).unwrap();
        let _ = dense.take_changes(ViewId(0)).unwrap();
        if case.with_mixed_flat_view {
            let _ = auto.take_changes(ViewId(1)).unwrap();
            let _ = dense.take_changes(ViewId(1)).unwrap();
        }
        assert_batches_same(
            &[auto.snapshot(ViewId(0)).unwrap()],
            &[dense.snapshot(ViewId(0)).unwrap()],
        );
        assert_eq!(history_signature(&auto), history_signature(&dense));

        let (input, update) = match case.update {
            Update::ReplaceLeft => (
                InputId(0),
                nullable_rows(&[0, 0], &[left_values[0], Some(12)], &[-1, 1]),
            ),
            Update::ReplaceRight => (
                InputId(1),
                nullable_rows(&[0, 0], &[right_values[0], Some(10)], &[-1, 1]),
            ),
            Update::AppendRight => (InputId(1), nullable_rows(&[0], &[Some(10)], &[1])),
            _ => panic!("AVG fixtures use append-right or replacement updates"),
        };
        let eligible = push_and_compare(&mut auto, &mut dense, case, input, &update);
        assert!(eligible, "{} must prove an eligible AVG cut", case.name);

        let checkpoint = auto.checkpoint().unwrap();
        let mut restored = avg_core(case);
        restored.restore(&checkpoint).unwrap();
        assert_eq!(history_signature(&auto), history_signature(&restored));
        assert_batches_same(
            &[auto.snapshot(ViewId(0)).unwrap()],
            &[restored.snapshot(ViewId(0)).unwrap()],
        );
        let continuation = nullable_rows(&[0], &[Some(9)], &[1]);
        let continuation_input = InputId(1);
        crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
        auto.push(continuation_input, &continuation).unwrap();
        let continued_cuts = crate::core::graph::FACTORIZED_CUTS.with(Cell::get);
        restored.push(continuation_input, &continuation).unwrap();
        let mode = ModeGuard::new(true);
        dense.push(continuation_input, &continuation).unwrap();
        drop(mode);
        assert!(continued_cuts > 0, "restored fixture remains eligible");
        let mut auto_changes = vec![auto.take_changes(ViewId(0)).unwrap()];
        let mut restored_changes = vec![restored.take_changes(ViewId(0)).unwrap()];
        let mut dense_changes = vec![dense.take_changes(ViewId(0)).unwrap()];
        if case.with_mixed_flat_view {
            auto_changes.push(auto.take_changes(ViewId(1)).unwrap());
            restored_changes.push(restored.take_changes(ViewId(1)).unwrap());
            dense_changes.push(dense.take_changes(ViewId(1)).unwrap());
        }
        assert_batches_same(&auto_changes, &dense_changes);
        assert_batches_same(&restored_changes, &dense_changes);
        let mut auto_snapshots = vec![auto.snapshot(ViewId(0)).unwrap()];
        let mut dense_snapshots = vec![dense.snapshot(ViewId(0)).unwrap()];
        let mut restored_snapshots = vec![restored.snapshot(ViewId(0)).unwrap()];
        if case.with_mixed_flat_view {
            auto_snapshots.push(auto.snapshot(ViewId(1)).unwrap());
            restored_snapshots.push(restored.snapshot(ViewId(1)).unwrap());
            dense_snapshots.push(dense.snapshot(ViewId(1)).unwrap());
        }
        assert_batches_same(&auto_snapshots, &dense_snapshots);
        assert_batches_same(&restored_snapshots, &dense_snapshots);
        assert_eq!(history_signature(&auto), history_signature(&dense));
        assert_eq!(history_signature(&restored), history_signature(&dense));
    }
}

#[test]
fn all_null_avg_to_nonnull_contracts_fall_back_or_certify_only_after_proof() {
    let case = AvgCase {
        name: "avg-null-to-value",
        fanout: 1,
        project_subset: false,
        avg_left: false,
        repeated: false,
        with_mixed_flat_view: false,
        update: Update::AppendRight,
        all_null: false,
    };
    let mut auto = avg_core(case);
    let mut dense = avg_core(case);
    auto.push(InputId(0), &nullable_rows(&[0], &[Some(2)], &[1]))
        .unwrap();
    auto.push(InputId(1), &nullable_rows(&[0], &[None], &[1]))
        .unwrap();
    {
        let mode = ModeGuard::new(true);
        dense
            .push(InputId(0), &nullable_rows(&[0], &[Some(2)], &[1]))
            .unwrap();
        dense
            .push(InputId(1), &nullable_rows(&[0], &[None], &[1]))
            .unwrap();
        drop(mode);
    }
    let _ = auto.take_changes(ViewId(0)).unwrap();
    let _ = dense.take_changes(ViewId(0)).unwrap();
    let null_replacement = nullable_rows(&[0, 0], &[None, Some(4)], &[-1, 1]);
    let certified = push_and_compare(&mut auto, &mut dense, case, InputId(1), &null_replacement);
    assert!(
        certified,
        "valid small integer AVG should certify after NULL replacement"
    );
}

#[test]
fn inexact_integer_avg_fixture_remains_a_dense_fallback_control() {
    let case = AvgCase {
        name: "unsafe-inexact-integer-avg-control",
        fanout: 1,
        project_subset: false,
        avg_left: true,
        repeated: false,
        with_mixed_flat_view: false,
        update: Update::AppendRight,
        all_null: false,
    };
    let mut auto = avg_core(case);
    let mut dense = avg_core(case);
    let initial_left = nullable_rows(&[0], &[Some((1_i64 << 53) + 1)], &[1]);
    let initial_right = nullable_rows(&[0], &[Some(3)], &[1]);
    auto.push(InputId(0), &initial_left).unwrap();
    auto.push(InputId(1), &initial_right).unwrap();
    {
        let mode = ModeGuard::new(true);
        dense.push(InputId(0), &initial_left).unwrap();
        dense.push(InputId(1), &initial_right).unwrap();
        drop(mode);
    }
    let _ = auto.take_changes(ViewId(0)).unwrap();
    let _ = dense.take_changes(ViewId(0)).unwrap();
    let update = nullable_rows(&[0], &[Some(5)], &[1]);
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| count.set(0));
    auto.push(InputId(1), &update).unwrap();
    let factorized = crate::core::graph::FACTORIZED_CUTS.with(Cell::get);
    let row_cuts = crate::core::graph::FLAT_ROWS_CUTS.with(Cell::get);
    let mode = ModeGuard::new(true);
    dense.push(InputId(1), &update).unwrap();
    drop(mode);
    assert_eq!(factorized, 0, "inexact AVG history is not certifiable");
    assert_eq!(
        row_cuts, 1,
        "inexact AVG history must retain Dense row semantics"
    );
    assert_batches_same(
        &[auto.take_changes(ViewId(0)).unwrap()],
        &[dense.take_changes(ViewId(0)).unwrap()],
    );
    assert_batches_same(
        &[auto.snapshot(ViewId(0)).unwrap()],
        &[dense.snapshot(ViewId(0)).unwrap()],
    );
    assert_eq!(history_signature(&auto), history_signature(&dense));
}
