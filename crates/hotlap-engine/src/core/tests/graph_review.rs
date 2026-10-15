use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, ZSetBatch};

use crate::core::EngineCore;

fn pairs(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(
        batch,
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
    )
    .unwrap()
}

fn grouped_plan() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count(), AggSpec::sum(1), AggSpec::avg(1)],
    }
}

fn keyed_rows(rows: &[(i64, i64, i64, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group_key", DataType::Int64, false),
        Field::new("join_key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(
        batch,
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        )),
    )
    .unwrap()
}

fn core_with_views(mixed: bool) -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(ViewId(0), &grouped_plan()).unwrap();
    if mixed {
        core.build_view(
            ViewId(1),
            &Plan::Join {
                left: Box::new(Plan::Source(InputId(0))),
                right: Box::new(Plan::Source(InputId(1))),
                left_key: vec![0],
                right_key: vec![0],
            },
        )
        .unwrap();
    }
    core.tap_view(ViewId(0)).unwrap();
    if mixed {
        core.tap_view(ViewId(1)).unwrap();
    }
    core
}

fn take_all(core: &mut EngineCore, mixed: bool) -> Vec<ZSetBatch> {
    (0..if mixed { 2 } else { 1 })
        .map(|i| core.take_changes(ViewId(i)).unwrap())
        .collect()
}

fn snapshots(core: &mut EngineCore, mixed: bool) -> Vec<ZSetBatch> {
    (0..if mixed { 2 } else { 1 })
        .map(|i| core.snapshot(ViewId(i)).unwrap())
        .collect()
}

fn compare_views(a: &[ZSetBatch], b: &[ZSetBatch]) {
    assert_eq!(a.len(), b.len());
    for (index, (a, b)) in a.iter().zip(b).enumerate() {
        assert_eq!(a.len(), b.len(), "view {index} row count differs");
        if a.is_empty() || b.is_empty() {
            assert!(
                a.is_empty() && b.is_empty(),
                "view {index} has only one empty batch"
            );
            continue;
        }
        assert_same_batch(a, b);
    }
}

fn assert_same_batch(a: &ZSetBatch, b: &ZSetBatch) {
    assert_eq!(a.schema(), b.schema());
    assert_eq!(a.len(), b.len());
    assert_eq!(a.diff().to_data(), b.diff().to_data());
    for (left, right) in a.batch.columns().iter().zip(b.batch.columns()) {
        assert_eq!(left.to_data(), right.to_data());
    }
}

#[test]
fn undrained_changes_survive_checkpoint_and_emit_once_after_restore() {
    let mut original = core_with_views(false);
    original.push(InputId(0), &pairs(&[(1, 10, 1)])).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    original.push(InputId(1), &pairs(&[(1, 20, 1)])).unwrap();
    let pending = original.checkpoint().unwrap();
    assert!(!pending.views[0].pending.as_ref().unwrap().diff.is_empty());
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));

    let mut restored = EngineCore::new();
    restored.register_input(InputId(0)).unwrap();
    restored.register_input(InputId(1)).unwrap();
    restored.build_view(ViewId(0), &grouped_plan()).unwrap();
    restored.tap_view(ViewId(0)).unwrap();
    restored.restore(&pending).unwrap();

    compare_views(
        &take_all(&mut original, false),
        &take_all(&mut restored, false),
    );
    assert!(take_all(&mut original, false)[0].is_empty());
    assert!(take_all(&mut restored, false)[0].is_empty());
    original
        .push(InputId(0), &pairs(&[(1, 10, -1), (1, 12, 1)]))
        .unwrap();
    restored
        .push(InputId(0), &pairs(&[(1, 10, -1), (1, 12, 1)]))
        .unwrap();
    compare_views(
        &take_all(&mut original, false),
        &take_all(&mut restored, false),
    );
    compare_views(
        &snapshots(&mut original, false),
        &snapshots(&mut restored, false),
    );
}

#[test]
fn restored_group_cut_rebuilds_source_stats_and_memo_before_next_delta() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count(), AggSpec::sum(1)],
    };
    let mut factor = EngineCore::new();
    factor.register_input(InputId(0)).unwrap();
    factor.register_input(InputId(1)).unwrap();
    factor.build_view(ViewId(0), &plan).unwrap();
    factor.tap_view(ViewId(0)).unwrap();
    factor.push(InputId(0), &pairs(&[(1, 10, 1)])).unwrap();
    factor.push(InputId(1), &pairs(&[(1, 20, 1)])).unwrap();
    crate::ops::join::factorized::FLAT_PAIR_PRODUCTS.with(|count| count.set(0));
    factor.push(InputId(0), &pairs(&[(1, 11, 1)])).unwrap();
    crate::ops::join::factorized::FLAT_PAIR_PRODUCTS.with(|count| {
        assert_eq!(
            count.get(),
            0,
            "aggregate-only graph cut evaluates no pairs"
        )
    });
    let checkpoint = factor.checkpoint().unwrap();

    let mut restored = EngineCore::new();
    restored.register_input(InputId(0)).unwrap();
    restored.register_input(InputId(1)).unwrap();
    restored.build_view(ViewId(0), &plan).unwrap();
    restored.tap_view(ViewId(0)).unwrap();
    crate::arrange::KeyedArrangement::reset_source_stats_rebuild_rows_visited_for_test();
    restored.restore(&checkpoint).unwrap();
    assert_eq!(
        crate::arrange::KeyedArrangement::source_stats_rebuild_rows_visited_for_test(),
        3,
        "restore visits only the canonical input rows"
    );
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    restored.push(InputId(0), &pairs(&[(1, 12, 1)])).unwrap();
    crate::core::graph::FACTORIZED_CUTS
        .with(|count| assert_eq!(count.get(), 1, "restored cut should execute certified path"));
    let changes = take_all(&mut restored, false);
    let count = changes[0]
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.values(), &[3]);
}

#[test]
fn restore_rejects_count_nonnull_history_that_disagrees_with_cached_root_output() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec {
            func: hotlap_core::AggFunc::Count,
            input: Some(2),
        }],
    };
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source.push(InputId(0), &pairs(&[(7, 10, 1)])).unwrap();
    source.push(InputId(1), &pairs(&[(7, 20, 1)])).unwrap();
    let mut snapshot = source.checkpoint().unwrap();
    let group = snapshot.views[0]
        .operators
        .iter_mut()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Group(group) => Some(group),
            _ => None,
        })
        .unwrap();
    group.groups[0].1.values[0] = hotlap_core::snapshot::AggValue::Count(0);

    let mut target = EngineCore::new();
    target.register_input(InputId(9)).unwrap();
    target
        .build_view(ViewId(9), &Plan::Source(InputId(9)))
        .unwrap();
    target.tap_view(ViewId(9)).unwrap();
    target.push(InputId(9), &pairs(&[(99, 1, 1)])).unwrap();
    let before = target.checkpoint().unwrap();
    assert!(target.validate_snapshot(&snapshot).is_err());
    assert!(target.restore(&snapshot).is_err());
    assert_eq!(target.checkpoint().unwrap(), before);
}

#[test]
fn restore_rejects_sum_history_that_disagrees_with_cached_root_output() {
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
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source.push(InputId(0), &pairs(&[(7, 10, 1)])).unwrap();
    source.push(InputId(1), &pairs(&[(7, 20, 1)])).unwrap();
    let mut snapshot = source.checkpoint().unwrap();
    let group = snapshot.views[0]
        .operators
        .iter_mut()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Group(group) => Some(group),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        group.groups[0].1.values[0],
        hotlap_core::snapshot::AggValue::SumInteger { sum: 20, count: 1 }
    );
    group.groups[0].1.values[0] = hotlap_core::snapshot::AggValue::SumInteger { sum: 21, count: 1 };

    let mut target = EngineCore::new();
    target.register_input(InputId(9)).unwrap();
    target
        .build_view(ViewId(9), &Plan::Source(InputId(9)))
        .unwrap();
    target.tap_view(ViewId(9)).unwrap();
    target.push(InputId(9), &pairs(&[(99, 1, 1)])).unwrap();
    let before = target.checkpoint().unwrap();
    assert!(target.validate_snapshot(&snapshot).is_err());
    assert!(target.restore(&snapshot).is_err());
    assert_eq!(target.checkpoint().unwrap(), before);
}

#[test]
fn restore_rejects_malformed_scalar_group_key_without_decoding_it() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source.push(InputId(0), &pairs(&[(7, 10, 1)])).unwrap();
    source.push(InputId(1), &pairs(&[(7, 20, 1)])).unwrap();
    let mut snapshot = source.checkpoint().unwrap();
    let group = snapshot.views[0]
        .operators
        .iter_mut()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Group(group) => Some(group),
            _ => None,
        })
        .unwrap();
    group.groups[0].0 = vec![1];

    let mut restored = EngineCore::new();
    assert!(matches!(
        restored.restore(&snapshot),
        Err(crate::error::EngineError::Infrastructure(_))
    ));
}

#[test]
fn restore_rejects_malformed_later_group_key_field_after_valid_prefix() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("major", DataType::Int64, false),
        Field::new("minor", DataType::Int32, true),
    ]));
    let make_batch = |rows: &[(i64, Option<i32>, i64)]| {
        ZSetBatch::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(arrow::array::Int32Array::from(
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
    };
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0, 1],
            right_key: vec![0, 1],
        }),
        key: vec![0, 1],
        aggs: vec![AggSpec::count()],
    };
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source
        .push(InputId(0), &make_batch(&[(7, Some(3), 1)]))
        .unwrap();
    source
        .push(InputId(1), &make_batch(&[(7, Some(3), 1)]))
        .unwrap();
    let mut snapshot = source.checkpoint().unwrap();
    let group = snapshot.views[0]
        .operators
        .iter_mut()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Group(group) => Some(group),
            _ => None,
        })
        .unwrap();
    group.groups[0].0 = [group.groups[0].0[..9].to_vec(), vec![1]].concat();

    let mut restored = EngineCore::new();
    assert!(matches!(
        restored.restore(&snapshot),
        Err(crate::error::EngineError::Infrastructure(_))
    ));
}

#[test]
fn restore_root_project_validates_consolidated_colliding_group_rows() {
    let group = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    let plan = Plan::Project {
        input: Box::new(group),
        cols: vec![1],
    };
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source
        .push(InputId(0), &pairs(&[(1, 10, 1), (2, 20, 1)]))
        .unwrap();
    source
        .push(InputId(1), &pairs(&[(1, 11, 1), (2, 21, 1)]))
        .unwrap();
    let expected = source.snapshot(ViewId(0)).unwrap();
    assert_eq!(expected.len(), 1);
    assert_eq!(
        expected
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[2]
    );

    let checkpoint = source.checkpoint().unwrap();
    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    assert_eq!(restored.checkpoint().unwrap(), checkpoint);

    let mut corrupted = checkpoint.clone();
    let group = corrupted.views[0]
        .operators
        .iter_mut()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Group(group) => Some(group),
            _ => None,
        })
        .unwrap();
    group.groups[0].1.values[0] = hotlap_core::snapshot::AggValue::Count(2);
    assert!(EngineCore::new().validate_snapshot(&corrupted).is_err());
}

#[test]
fn restore_checks_row_cardinality_without_visible_count_star() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec {
            func: hotlap_core::AggFunc::Count,
            input: Some(3),
        }],
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let batch = |value: Option<i64>| {
        ZSetBatch::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![7])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![value])) as ArrayRef,
                ],
            )
            .unwrap(),
            Arc::new(Int64Array::from(vec![1])),
        )
        .unwrap()
    };
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source.push(InputId(0), &batch(Some(10))).unwrap();
    source.push(InputId(1), &batch(Some(20))).unwrap();
    let mut snapshot = source.checkpoint().unwrap();
    let group = snapshot.views[0]
        .operators
        .iter_mut()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Group(group) => Some(group),
            _ => None,
        })
        .unwrap();
    assert_eq!(group.groups[0].1.rows, 1);
    assert_eq!(
        group.groups[0].1.values[0],
        hotlap_core::snapshot::AggValue::Count(1)
    );
    group.groups[0].1.rows = 2;

    let mut restored = EngineCore::new();
    assert!(matches!(
        restored.restore(&snapshot),
        Err(crate::error::EngineError::Infrastructure(_))
    ));
}

#[test]
fn all_null_sum_keeps_row_cardinality_independent_of_nonnull_count() {
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
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let batch = |value: Option<i64>, diff: i64| {
        ZSetBatch::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![7])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![value])) as ArrayRef,
                ],
            )
            .unwrap(),
            Arc::new(Int64Array::from(vec![diff])),
        )
        .unwrap()
    };
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source.push(InputId(0), &batch(Some(10), 1)).unwrap();
    source.push(InputId(1), &batch(None, 1)).unwrap();
    let checkpoint = source.checkpoint().unwrap();
    let entry = checkpoint.views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Group(group) => Some(&group.groups[0].1),
            _ => None,
        })
        .unwrap();
    assert_eq!(entry.rows, 1);
    assert_eq!(
        entry.values[0],
        hotlap_core::snapshot::AggValue::SumInteger { sum: 0, count: 0 }
    );

    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    let restored_rows = restored.snapshot(ViewId(0)).unwrap();
    assert_eq!(restored_rows.len(), 1);
    assert_eq!(
        restored_rows.schema().fields().len(),
        2,
        "hidden row metric is not an output slot"
    );
    restored.push(InputId(1), &batch(None, -1)).unwrap();
    assert!(restored.snapshot(ViewId(0)).unwrap().is_empty());
}

#[test]
fn restore_rejects_visible_aggregate_output_and_provable_row_corruption() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![
            AggSpec::sum(3),
            AggSpec::count(),
            AggSpec {
                func: hotlap_core::AggFunc::Count,
                input: Some(3),
            },
        ],
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let batch = |key, value: Option<i64>| {
        ZSetBatch::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![key])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![value])) as ArrayRef,
                ],
            )
            .unwrap(),
            Arc::new(Int64Array::from(vec![1])),
        )
        .unwrap()
    };
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source.tap_view(ViewId(0)).unwrap();
    source.push(InputId(0), &batch(7, Some(10))).unwrap();
    source.push(InputId(1), &batch(7, None)).unwrap();
    let good = source.checkpoint().unwrap();
    let mut accepted = EngineCore::new();
    accepted.restore(&good).unwrap();

    for mutate in 0..5 {
        let mut invalid = good.clone();
        let group = invalid.views[0]
            .operators
            .iter_mut()
            .flatten()
            .find_map(|state| match state {
                hotlap_core::snapshot::OperatorState::Group(group) => Some(group),
                _ => None,
            })
            .unwrap();
        match mutate {
            0 => group.groups[0].1.values[2] = hotlap_core::snapshot::AggValue::Count(1),
            1 => {
                group.groups[0].1.values[0] =
                    hotlap_core::snapshot::AggValue::SumInteger { sum: 10, count: 1 }
            }
            2 => group.groups.clear(),
            3 => group.groups.push((vec![0xff], group.groups[0].1.clone())),
            4 => group.groups[0].1.values[1] = hotlap_core::snapshot::AggValue::Count(2),
            _ => group.groups[0].1.rows = 2,
        }
        let mut restored = EngineCore::new();
        assert!(
            matches!(
                restored.restore(&invalid),
                Err(crate::error::EngineError::Infrastructure(_))
            ),
            "inconsistent cached output or provable corruption case {mutate} should be rejected"
        );
    }
}

#[test]
fn later_invalid_view_is_rejected_before_any_derived_restore_rebuild() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source.build_view(ViewId(1), &plan).unwrap();
    source.tap_view(ViewId(0)).unwrap();
    source.tap_view(ViewId(1)).unwrap();
    source.push(InputId(0), &pairs(&[(7, 10, 1)])).unwrap();
    source.push(InputId(1), &pairs(&[(7, 20, 1)])).unwrap();
    let snapshot = source.checkpoint().unwrap();
    for invalid_part in 0..4 {
        let mut invalid = snapshot.clone();
        match invalid_part {
            0 => {
                invalid.views[1].pending = Some(hotlap_core::snapshot::SnapshotTable {
                    ipc: vec![0xff],
                    diff: vec![],
                })
            }
            1 => {
                invalid.views[1].output = Some(hotlap_core::snapshot::SnapshotTable {
                    ipc: vec![0xff],
                    diff: vec![],
                })
            }
            2 => {
                let group = invalid.views[1]
                    .operators
                    .iter_mut()
                    .flatten()
                    .find_map(|state| match state {
                        hotlap_core::snapshot::OperatorState::Group(group) => Some(group),
                        _ => None,
                    })
                    .unwrap();
                group.schema = Some(vec![0xff]);
            }
            _ => {
                let group = invalid.views[1]
                    .operators
                    .iter_mut()
                    .flatten()
                    .find_map(|state| match state {
                        hotlap_core::snapshot::OperatorState::Group(group) => Some(group),
                        _ => None,
                    })
                    .unwrap();
                group.groups[0].1.values[0] =
                    hotlap_core::snapshot::AggValue::SumInteger { sum: 1, count: 1 };
            }
        }
        crate::arrange::KeyedArrangement::reset_source_stats_rebuild_rows_visited_for_test();
        let mut restored = EngineCore::new();
        assert!(restored.restore(&invalid).is_err());
        assert_eq!(
            crate::arrange::KeyedArrangement::source_stats_rebuild_rows_visited_for_test(),
            0,
            "invalid later view part {invalid_part} must fail before derived rebuild"
        );
        assert!(restored.views.is_empty());
    }
}

#[test]
fn forced_dense_uses_the_same_maintained_source_and_group_metadata() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count(), AggSpec::sum(1)],
    };
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(ViewId(0), &plan).unwrap();
    core.push(InputId(0), &pairs(&[(7, 10, 1)])).unwrap();
    crate::arrange::KeyedArrangement::reset_source_stats_rebuild_rows_visited_for_test();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    core.push(InputId(1), &pairs(&[(7, 20, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    assert!(
        crate::arrange::KeyedArrangement::source_stats_ordinary_values_read_for_test() > 0,
        "forced Dense must maintain registered source summaries during ordinary apply"
    );
    crate::ops::join::factorized::FLAT_PAIR_PRODUCTS.with(|count| count.set(0));
    core.push(InputId(0), &pairs(&[(7, 11, 1)])).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| {
        assert_eq!(
            count.get(),
            1,
            "the Dense-maintained memo is ready for sparse certify"
        )
    });
    crate::ops::join::factorized::FLAT_PAIR_PRODUCTS.with(|count| assert_eq!(count.get(), 0));
}

#[test]
fn restore_preserves_valid_inexact_avg_history_bits() {
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
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source
        .push(InputId(0), &pairs(&[(7, (1_i64 << 53) + 1, 1)]))
        .unwrap();
    source.push(InputId(1), &pairs(&[(7, 0, 1)])).unwrap();
    let checkpoint = source.checkpoint().unwrap();
    let stored_avg = checkpoint.views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Group(group) => {
                Some(&group.groups[0].1.values[0])
            }
            _ => None,
        })
        .unwrap();
    let hotlap_core::snapshot::AggValue::Avg {
        sum: stored_sum,
        count,
        nan,
        pos_inf,
        neg_inf,
    } = stored_avg
    else {
        panic!("expected authoritative AVG history");
    };
    let expected = (*stored_sum).to_bits();
    let counts = (*count, *nan, *pos_inf, *neg_inf);

    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    let restored_checkpoint = restored.checkpoint().unwrap();
    let restored_avg = restored_checkpoint.views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Group(group) => {
                Some(&group.groups[0].1.values[0])
            }
            _ => None,
        })
        .unwrap();
    let hotlap_core::snapshot::AggValue::Avg {
        sum,
        count,
        nan,
        pos_inf,
        neg_inf,
    } = restored_avg
    else {
        panic!("expected restored authoritative AVG history");
    };
    assert_eq!(sum.to_bits(), expected);
    assert_eq!((*count, *nan, *pos_inf, *neg_inf), counts);
}

#[test]
fn restore_does_not_reject_valid_mixed_signed_source_history() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    let mut source = EngineCore::new();
    source.register_input(InputId(0)).unwrap();
    source.register_input(InputId(1)).unwrap();
    source.build_view(ViewId(0), &plan).unwrap();
    source
        .push(InputId(0), &pairs(&[(7, 10, 2), (7, 11, -1)]))
        .unwrap();
    source.push(InputId(1), &pairs(&[(7, 20, 1)])).unwrap();
    let checkpoint = source.checkpoint().unwrap();
    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    assert_eq!(
        restored.snapshot(ViewId(0)).unwrap().len(),
        source.snapshot(ViewId(0)).unwrap().len()
    );
}

#[test]
fn pending_right_delta_resolves_after_restore_and_matches_dense() {
    let mut factor = core_with_views(false);
    let mut dense = core_with_views(false);
    factor.push(InputId(1), &pairs(&[(2, 30, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.set(true));
    dense.push(InputId(1), &pairs(&[(2, 30, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.set(false));
    compare_views(&take_all(&mut factor, false), &take_all(&mut dense, false));
    let checkpoint = factor.checkpoint().unwrap();
    let join = checkpoint.views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Join(join) => Some(join),
            _ => None,
        })
        .unwrap();
    assert!(join.right_pending.is_some());

    let mut restored = EngineCore::new();
    restored.register_input(InputId(0)).unwrap();
    restored.register_input(InputId(1)).unwrap();
    restored.build_view(ViewId(0), &grouped_plan()).unwrap();
    restored.tap_view(ViewId(0)).unwrap();
    restored.restore(&checkpoint).unwrap();

    let left = pairs(&[(2, 8, 1)]);
    factor.push(InputId(0), &left).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.set(true));
    dense.push(InputId(0), &left).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.set(false));
    restored.push(InputId(0), &left).unwrap();
    let factor_changes = take_all(&mut factor, false);
    let dense_changes = take_all(&mut dense, false);
    let restored_changes = take_all(&mut restored, false);
    compare_views(&factor_changes, &dense_changes);
    compare_views(&factor_changes, &restored_changes);
    compare_views(
        &snapshots(&mut factor, false),
        &snapshots(&mut restored, false),
    );
}

#[test]
fn mixed_factorized_and_flat_views_match_dense_after_each_push_and_restore() {
    let mut factor = core_with_views(true);
    let mut dense = core_with_views(true);
    for (step, (input, batch)) in [
        (InputId(0), pairs(&[(1, 4, 1), (1, 7, 1)])),
        (InputId(1), pairs(&[(1, 9, 1), (1, 12, 1)])),
        (InputId(0), pairs(&[(1, 4, -1), (1, 3, 1)])),
    ]
    .into_iter()
    .enumerate()
    {
        if step == 1 {
            crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
            crate::ops::join::factorized::FLAT_PAIR_PRODUCTS.with(|count| count.set(0));
        }
        factor.push(input, &batch).unwrap();
        if step == 1 {
            crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));
            crate::ops::join::factorized::FLAT_PAIR_PRODUCTS
                .with(|count| assert_eq!(count.get(), 4));
        }
        crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.set(true));
        dense.push(input, &batch).unwrap();
        crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.set(false));
        let factor_changes = take_all(&mut factor, true);
        let dense_changes = take_all(&mut dense, true);
        assert_eq!(
            factor_changes
                .iter()
                .map(ZSetBatch::len)
                .collect::<Vec<_>>(),
            dense_changes.iter().map(ZSetBatch::len).collect::<Vec<_>>(),
            "step {step}"
        );
        compare_views(&factor_changes, &dense_changes);
        compare_views(&snapshots(&mut factor, true), &snapshots(&mut dense, true));
    }
    let checkpoint = factor.checkpoint().unwrap();
    let mut restored = core_with_views(true);
    restored.restore(&checkpoint).unwrap();
    let next = pairs(&[(1, 6, 1)]);
    factor.push(InputId(1), &next).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.set(true));
    dense.push(InputId(1), &next).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.set(false));
    restored.push(InputId(1), &next).unwrap();
    let factor_changes = take_all(&mut factor, true);
    let dense_changes = take_all(&mut dense, true);
    let restored_changes = take_all(&mut restored, true);
    compare_views(&factor_changes, &dense_changes);
    compare_views(&factor_changes, &restored_changes);
    compare_views(
        &snapshots(&mut factor, true),
        &snapshots(&mut restored, true),
    );
}

#[test]
fn typed_join_overflow_after_factorized_history_poison_rejects_checkpoint() {
    let mut core = core_with_views(false);
    crate::ops::join::factorized::FLAT_PAIR_PRODUCTS.with(|count| count.set(0));
    core.push(InputId(0), &pairs(&[(1, 1, 1)])).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    core.push(InputId(1), &pairs(&[(1, 1, 2)])).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));
    crate::ops::join::factorized::FLAT_PAIR_PRODUCTS.with(|count| {
        assert_eq!(
            count.get(),
            0,
            "certified AVG path avoids join pair products"
        )
    });
    core.push(InputId(0), &pairs(&[(1, 1, -1)])).unwrap();
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());
    let good = core.checkpoint().unwrap();
    let error = core
        .push(InputId(0), &pairs(&[(1, 1, i64::MAX)]))
        .unwrap_err();
    assert!(
        matches!(error, hotlap_core::CoreError::NumericOverflow(_)),
        "unexpected overflow error: {error:?}"
    );
    assert!(core.checkpoint().is_err());

    let mut restored = EngineCore::new();
    restored.register_input(InputId(0)).unwrap();
    restored.register_input(InputId(1)).unwrap();
    restored.build_view(ViewId(0), &grouped_plan()).unwrap();
    restored.restore(&good).unwrap();
    restored.push(InputId(0), &pairs(&[(1, 1, 1)])).unwrap();
    let result = restored.snapshot(ViewId(0)).unwrap();
    let count = result
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.values(), &[2]);
}

#[test]
fn core_v1_restore_rejects_incompatible_grouped_plan_before_publish() {
    let mut core = core_with_views(false);
    core.push(InputId(0), &pairs(&[(1, 3, 1)])).unwrap();
    core.push(InputId(1), &pairs(&[(1, 4, 1)])).unwrap();
    let mut snapshot = core.checkpoint().unwrap();
    let Plan::GroupAggregate { key, .. } = &mut snapshot.views[0].plan else {
        unreachable!()
    };
    *key = vec![8];
    let mut incompatible = EngineCore::new();
    assert!(incompatible.restore(&snapshot).is_err());
    assert!(incompatible.views.is_empty());

    let mut corrupt = core.checkpoint().unwrap();
    let group = corrupt.views[0]
        .operators
        .iter_mut()
        .flatten()
        .find_map(|state| match state {
            hotlap_core::snapshot::OperatorState::Group(group) => Some(group),
            _ => None,
        })
        .unwrap();
    group.schema = Some(b"not-arrow-schema".to_vec());
    assert!(incompatible.restore(&corrupt).is_err());
    assert!(incompatible.views.is_empty());
}

#[test]
fn old_history_materializes_only_join_buckets_in_touched_group() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0, 1],
            right_key: vec![0, 1],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count(), AggSpec::sum(2)],
    };
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(ViewId(0), &plan).unwrap();
    core.push(
        InputId(0),
        &keyed_rows(&[(1, 10, 2, 1), (1, 11, 3, 1), (2, 20, 5, 1)]),
    )
    .unwrap();
    core.push(
        InputId(1),
        &keyed_rows(&[(1, 10, 4, 1), (1, 11, 6, 1), (2, 20, 7, 1)]),
    )
    .unwrap();

    crate::ops::join::factorized::OLD_HISTORY_ROWS_SCANNED.with(|count| count.set(0));
    core.push(InputId(0), &keyed_rows(&[(1, 10, 8, 1)]))
        .unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));
    crate::ops::join::factorized::OLD_HISTORY_ROWS_SCANNED.with(|count| {
        assert_eq!(
            count.get(),
            0,
            "maintained memo avoids history reconstruction"
        );
    });
}
