use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, ZSetBatch};

use crate::core::EngineCore;

struct ForceDenseFilterProjectGuard(bool);

impl ForceDenseFilterProjectGuard {
    fn set(force_dense: bool) -> Self {
        let previous =
            crate::core::graph::FORCE_DENSE_FILTER_PROJECT.with(|force| force.replace(force_dense));
        Self(previous)
    }
}

impl Drop for ForceDenseFilterProjectGuard {
    fn drop(&mut self) {
        crate::core::graph::FORCE_DENSE_FILTER_PROJECT.with(|force| force.set(self.0));
    }
}

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

fn assert_same_zset(actual: &ZSetBatch, expected: &ZSetBatch) {
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.batch.columns().iter().zip(expected.batch.columns()) {
        assert_eq!(actual.to_data(), expected.to_data());
    }
    assert_eq!(actual.diff.to_data(), expected.diff.to_data());
}

fn tap_and_push(
    engine: &mut EngineCore,
    view: ViewId,
    source_id: InputId,
    source: &ZSetBatch,
) -> ZSetBatch {
    engine.push(source_id, source).unwrap();
    engine.take_changes(view).unwrap()
}

#[test]
fn source_filter_project_dense_reference_preserves_projection_and_predicate_rows() {
    let plan = Plan::Project {
        input: Box::new(Plan::Filter {
            input: Box::new(Plan::Source(InputId(0))),
            pred: hotlap_core::Predicate::Cmp {
                op: hotlap_core::CmpOp::Gt,
                col: 0,
                scalar: hotlap_core::Scalar::I64(1),
            },
        }),
        cols: vec![1, 0, 1],
    };
    let source = pairs(&[(1, 100, 2), (2, 200, -3), (3, 300, 0)]);
    let mask = hotlap_core::Predicate::Cmp {
        op: hotlap_core::CmpOp::Gt,
        col: 0,
        scalar: hotlap_core::Scalar::I64(1),
    }
    .eval(&source.batch)
    .unwrap();
    let filtered = crate::ops::filter_project::filter(&source, &mask).unwrap();
    let dense = crate::zset::consolidate(
        &crate::ops::filter_project::project(&filtered, &[1, 0, 1]).unwrap(),
    )
    .unwrap();

    let run = |force_dense| {
        let _guard = ForceDenseFilterProjectGuard::set(force_dense);
        crate::core::graph::SELECTED_FILTER_PROJECTS.with(|count| count.set(0));
        let mut engine = EngineCore::new();
        engine.register_input(InputId(0)).unwrap();
        engine.build_view(ViewId(0), &plan).unwrap();
        engine.tap_view(ViewId(0)).unwrap();
        engine.push(InputId(0), &source).unwrap();
        crate::zset::consolidate(&engine.snapshot(ViewId(0)).unwrap()).unwrap()
    };
    crate::core::graph::SELECTED_FILTER_PROJECTS.with(|count| count.set(0));
    crate::ops::filter_project::reset_fused_filter_project_kernel_count();
    let optimized = run(false);
    assert_eq!(crate::core::graph::selected_filter_project_count(), 1);
    assert_eq!(
        crate::ops::filter_project::fused_filter_project_kernel_count(),
        1
    );
    let dense_path = run(true);
    assert_eq!(
        crate::ops::filter_project::fused_filter_project_kernel_count(),
        1
    );
    assert_same_zset(&optimized, &dense);
    assert_same_zset(&optimized, &dense_path);
    assert!(mask.value(1));
}

#[test]
fn source_filter_project_live_and_checkpoint_continuation_match_dense_path() {
    let plan = Plan::Project {
        input: Box::new(Plan::Filter {
            input: Box::new(Plan::Source(InputId(0))),
            pred: hotlap_core::Predicate::Cmp {
                op: hotlap_core::CmpOp::Gt,
                col: 0,
                scalar: hotlap_core::Scalar::I64(1),
            },
        }),
        cols: vec![1, 0],
    };
    let first = pairs(&[(1, 10, 1), (2, 20, -2)]);
    let second = pairs(&[(3, 30, 0), (4, 40, 3)]);
    let build = |force_dense| {
        let _guard = ForceDenseFilterProjectGuard::set(force_dense);
        crate::core::graph::SELECTED_FILTER_PROJECTS.with(|count| count.set(0));
        let mut engine = EngineCore::new();
        engine.register_input(InputId(0)).unwrap();
        engine.build_view(ViewId(0), &plan).unwrap();
        engine.tap_view(ViewId(0)).unwrap();
        let tapped = tap_and_push(&mut engine, ViewId(0), InputId(0), &first);
        let before = engine.snapshot(ViewId(0)).unwrap();
        let changes_before = tapped;
        let selected_count = crate::core::graph::selected_filter_project_count();
        (engine, before, changes_before, selected_count)
    };
    crate::core::graph::SELECTED_FILTER_PROJECTS.with(|count| count.set(0));
    crate::ops::filter_project::reset_fused_filter_project_kernel_count();
    let (mut optimized, optimized_before, optimized_changes, optimized_count) = build(false);
    assert_eq!(optimized_count, 1);
    assert_eq!(
        crate::ops::filter_project::fused_filter_project_kernel_count(),
        1
    );
    crate::core::graph::SELECTED_FILTER_PROJECTS.with(|count| count.set(0));
    let (mut dense, dense_before, dense_changes, dense_count) = build(true);
    assert_eq!(dense_count, 0);
    assert_eq!(
        crate::ops::filter_project::fused_filter_project_kernel_count(),
        1
    );
    assert_same_zset(&optimized_before, &dense_before);
    assert_same_zset(&optimized_changes, &dense_changes);
    assert!(!optimized_changes.is_empty());

    let checkpoint = optimized.checkpoint().unwrap();
    let dense_checkpoint = dense.checkpoint().unwrap();
    let mut dense_restored = EngineCore::new();
    dense_restored.restore(&dense_checkpoint).unwrap();
    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    let optimized_second = tap_and_push(&mut optimized, ViewId(0), InputId(0), &second);
    assert_eq!(
        crate::ops::filter_project::fused_filter_project_kernel_count(),
        2
    );
    {
        let _guard = ForceDenseFilterProjectGuard::set(true);
        let dense_second = tap_and_push(&mut dense, ViewId(0), InputId(0), &second);
        let dense_restored_second =
            tap_and_push(&mut dense_restored, ViewId(0), InputId(0), &second);
        let restored_second = tap_and_push(&mut restored, ViewId(0), InputId(0), &second);
        assert_eq!(
            crate::ops::filter_project::fused_filter_project_kernel_count(),
            2
        );
        assert_same_zset(&dense_second, &dense_restored_second);
        assert_same_zset(&dense_second, &restored_second);
        assert_same_zset(&optimized_second, &dense_second);
    }
    let view = ViewId(0);
    let expected = crate::zset::consolidate(&dense.snapshot(view).unwrap()).unwrap();
    assert_eq!(
        crate::zset::consolidate(&optimized.snapshot(view).unwrap())
            .unwrap()
            .batch,
        expected.batch
    );
    assert_eq!(
        crate::zset::consolidate(&restored.snapshot(view).unwrap())
            .unwrap()
            .batch,
        expected.batch
    );
    let optimized_delta = optimized.take_changes(view).unwrap();
    let dense_delta = dense.take_changes(view).unwrap();
    let dense_restored_delta = dense_restored.take_changes(view).unwrap();
    let restored_delta = restored.take_changes(view).unwrap();
    assert_same_zset(&optimized_delta, &dense_delta);
    assert_same_zset(&restored_delta, &dense_delta);
    assert_same_zset(&dense_restored_delta, &dense_delta);
    assert_eq!(
        optimized.checkpoint().unwrap(),
        dense_restored.checkpoint().unwrap()
    );
    let continued_checkpoint = optimized.checkpoint().unwrap();
    assert_eq!(
        continued_checkpoint.format_version,
        checkpoint.format_version
    );
}

fn float_group_history(core: &EngineCore) -> (u64, u64) {
    use hotlap_core::snapshot::{AggValue, OperatorState};

    let snapshot = core.checkpoint().unwrap();
    let group = snapshot.views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|state| match state {
            OperatorState::Group(group) => Some(group),
            _ => None,
        })
        .unwrap();
    let values = &group.groups[0].values;
    let sum = match values[0] {
        AggValue::SumFloat { sum, .. } => sum.to_bits(),
        ref other => panic!("expected float SUM, got {other:?}"),
    };
    let avg = match values[1] {
        AggValue::Avg { sum, .. } => sum.to_bits(),
        ref other => panic!("expected AVG, got {other:?}"),
    };
    (sum, avg)
}

fn float_value(value: f64) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    ZSetBatch::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(arrow::array::Float64Array::from(vec![value])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![1])),
    )
    .unwrap()
}

fn flagged_row(key: i64, value: i64, include: i64, diff: i64) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("include", DataType::Int64, false),
    ]));
    ZSetBatch::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![key])),
                Arc::new(Int64Array::from(vec![value])),
                Arc::new(Int64Array::from(vec![include])),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![diff])),
    )
    .unwrap()
}

fn group_overflow_plan(agg: AggSpec) -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::Source(InputId(0))),
                pred: hotlap_core::Predicate::Cmp {
                    op: hotlap_core::CmpOp::Eq,
                    col: 2,
                    scalar: hotlap_core::Scalar::I64(1),
                },
            }),
            cols: vec![0, 1],
        }),
        key: vec![0],
        aggs: vec![agg],
    }
}

fn overflow_result(
    force_dense: bool,
    plan: &Plan,
    rows: &[ZSetBatch],
) -> (usize, String, bool, bool) {
    let _guard = ForceDenseFilterProjectGuard::set(force_dense);
    let mut engine = EngineCore::new();
    engine.register_input(InputId(0)).unwrap();
    engine.build_view(ViewId(0), plan).unwrap();
    engine.tap_view(ViewId(0)).unwrap();
    for (index, row) in rows.iter().enumerate() {
        if let Err(error) = engine.push(InputId(0), row) {
            let poisoned = engine.snapshot(ViewId(0)).is_err();
            let checkpoint_blocked = engine.checkpoint().is_err();
            return (index, error.to_string(), poisoned, checkpoint_blocked);
        }
    }
    panic!("expected a group aggregate overflow")
}

#[test]
fn selected_group_count_overflow_matches_dense_error_and_poisons_engine() {
    let plan = group_overflow_plan(AggSpec::count());
    let rows = [
        flagged_row(7, 10, 1, i64::MAX),
        flagged_row(7, 11, 0, -i64::MAX),
        flagged_row(7, 12, 1, 1),
    ];
    let selected = overflow_result(false, &plan, &rows);
    let dense = overflow_result(true, &plan, &rows);
    assert_eq!(selected, dense);
    assert_eq!(selected.0, 2, "COUNT should overflow on selected row three");
    assert!(selected.2, "failed group push must poison EngineCore");
    assert!(selected.3, "poisoned EngineCore must reject checkpoint");
}

#[test]
fn selected_group_sum_prefix_overflow_matches_dense_error_and_poisons_engine() {
    let plan = group_overflow_plan(AggSpec::sum(1));
    let rows = [
        flagged_row(7, i64::MAX, 1, i64::MAX),
        flagged_row(7, i64::MIN, 1, -i64::MAX),
        flagged_row(7, i64::MAX, 1, i64::MAX),
    ];
    let selected = overflow_result(false, &plan, &rows);
    let dense = overflow_result(true, &plan, &rows);
    assert_eq!(selected, dense);
    assert_eq!(
        selected.0, 0,
        "SUM output range fails on the first weighted row"
    );
    assert!(selected.2, "failed group push must poison EngineCore");
    assert!(selected.3, "poisoned EngineCore must reject checkpoint");
}

#[test]
fn grouped_float_history_order_and_checkpoint_continuation_match_dense_path() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::Source(InputId(0))),
                pred: hotlap_core::Predicate::Cmp {
                    op: hotlap_core::CmpOp::Gt,
                    col: 0,
                    scalar: hotlap_core::Scalar::I64(0),
                },
            }),
            cols: vec![0, 1],
        }),
        key: vec![0],
        aggs: vec![AggSpec::sum(1), AggSpec::avg(1)],
    };
    let values = [((1_i64 << 53) as f64), 1.0, -((1_i64 << 53) as f64)];
    let build = |force_dense| {
        let _guard = ForceDenseFilterProjectGuard::set(force_dense);
        crate::core::graph::SELECTED_FILTER_PROJECTS.with(|count| count.set(0));
        let mut engine = EngineCore::new();
        engine.register_input(InputId(0)).unwrap();
        engine.build_view(ViewId(0), &plan).unwrap();
        engine.tap_view(ViewId(0)).unwrap();
        engine
    };
    crate::ops::filter_project::reset_fused_filter_project_kernel_count();
    let mut optimized = build(false);
    let mut dense = build(true);
    assert_eq!(crate::core::graph::selected_filter_project_count(), 0);
    for (index, value) in values.into_iter().enumerate() {
        crate::core::graph::SELECTED_FILTER_PROJECTS.with(|count| count.set(0));
        let delta = float_value(value);
        optimized.push(InputId(0), &delta).unwrap();
        assert_eq!(
            crate::ops::filter_project::fused_filter_project_kernel_count(),
            index as u64 + 1
        );
        assert_eq!(crate::core::graph::selected_filter_project_count(), 1);
        let expected_bits = match index {
            0 => ((1_i64 << 53) as f64).to_bits(),
            1 => ((1_i64 << 53) as f64).to_bits(),
            _ => 0.0_f64.to_bits(),
        };
        assert_eq!(
            float_group_history(&optimized),
            (expected_bits, expected_bits)
        );
        {
            let _guard = ForceDenseFilterProjectGuard::set(true);
            dense.push(InputId(0), &delta).unwrap();
        }
        assert_eq!(
            crate::ops::filter_project::fused_filter_project_kernel_count(),
            index as u64 + 1
        );
        assert_eq!(crate::core::graph::selected_filter_project_count(), 1);
        assert_eq!(float_group_history(&optimized), float_group_history(&dense));
        let optimized_changes = optimized.take_changes(ViewId(0)).unwrap();
        let dense_changes = dense.take_changes(ViewId(0)).unwrap();
        assert!(!optimized_changes.is_empty());
        assert_same_zset(&optimized_changes, &dense_changes);
    }
    assert_eq!(
        float_group_history(&optimized),
        (0.0_f64.to_bits(), 0.0_f64.to_bits())
    );

    let checkpoint = optimized.checkpoint().unwrap();
    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    crate::core::graph::SELECTED_FILTER_PROJECTS.with(|count| count.set(0));
    let continuation = float_value(2.0);
    optimized.push(InputId(0), &continuation).unwrap();
    assert_eq!(
        crate::ops::filter_project::fused_filter_project_kernel_count(),
        4
    );
    assert_eq!(crate::core::graph::selected_filter_project_count(), 1);
    {
        let _guard = ForceDenseFilterProjectGuard::set(true);
        dense.push(InputId(0), &continuation).unwrap();
        restored.push(InputId(0), &continuation).unwrap();
    }
    assert_eq!(
        crate::ops::filter_project::fused_filter_project_kernel_count(),
        4
    );
    let optimized_continuation = optimized.take_changes(ViewId(0)).unwrap();
    let dense_continuation = dense.take_changes(ViewId(0)).unwrap();
    let restored_continuation = restored.take_changes(ViewId(0)).unwrap();
    assert!(!optimized_continuation.is_empty());
    assert_same_zset(&optimized_continuation, &dense_continuation);
    assert_same_zset(&restored_continuation, &dense_continuation);
    assert_eq!(crate::core::graph::selected_filter_project_count(), 1);
    assert_eq!(float_group_history(&optimized), float_group_history(&dense));
    assert_eq!(float_group_history(&restored), float_group_history(&dense));
    assert_eq!(optimized.checkpoint().unwrap(), dense.checkpoint().unwrap());
}
