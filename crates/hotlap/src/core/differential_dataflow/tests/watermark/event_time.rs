//! Event-time clock, late-drop and drain-visibility tests.

use super::super::{snapshot_pairs, source};
use crate::core::differential_dataflow::DifferentialCore;
use crate::core::{CoreError, IncrementalCore, InputId, ViewId, WatermarkSpec};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row, Scalar};

#[test]
fn event_time_drops_late_and_advances_watermark() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(InputId(0), WatermarkSpec { time_col: 0, lag: 0 })
        .unwrap();
    core.build_view(ViewId(0), &Plan::GroupCount {
        input: Box::new(source()),
        key: vec![1],
    })
    .unwrap();

    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(100), Scalar::I64(1)]), 1);
    core.push(InputId(0), &first).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);

    let mut late = ChangeBatch::default();
    late.push(Row(vec![Scalar::I64(90), Scalar::I64(1)]), 1);
    core.push(InputId(0), &late).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 1);
    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(1, 1)]);
}

#[test]
fn event_time_boundary_is_not_late() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(InputId(0), WatermarkSpec { time_col: 0, lag: 0 })
        .unwrap();
    core.build_view(ViewId(0), &Plan::GroupCount {
        input: Box::new(source()),
        key: vec![1],
    })
    .unwrap();
    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(100), Scalar::I64(1)]), 1);
    core.push(InputId(0), &first).unwrap();
    // ts == wm (100) no es tardío.
    let mut edge = ChangeBatch::default();
    edge.push(Row(vec![Scalar::I64(100), Scalar::I64(1)]), 1);
    core.push(InputId(0), &edge).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);
}

#[test]
fn event_time_empty_batch_does_not_move_watermark() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(InputId(0), WatermarkSpec { time_col: 0, lag: 0 })
        .unwrap();
    core.build_view(ViewId(0), &Plan::GroupCount {
        input: Box::new(source()),
        key: vec![1],
    })
    .unwrap();
    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(50), Scalar::I64(1)]), 1);
    core.push(InputId(0), &first).unwrap();
    core.push(InputId(0), &ChangeBatch::default()).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);
    let mut late = ChangeBatch::default();
    late.push(Row(vec![Scalar::I64(40), Scalar::I64(1)]), 1);
    core.push(InputId(0), &late).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 1);
}

#[test]
fn event_time_same_ts_second_push_is_visible() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(InputId(0), WatermarkSpec { time_col: 0, lag: 0 })
        .unwrap();
    core.build_view(ViewId(0), &Plan::GroupCount {
        input: Box::new(source()),
        key: vec![1],
    })
    .unwrap();

    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(100), Scalar::I64(1)]), 1);
    core.push(InputId(0), &first).unwrap();
    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(1, 1)]);

    // Same event-time as the current watermark: the watermark must still advance
    // so this row (inserted at the current logical time) is visible this push.
    let mut second = ChangeBatch::default();
    second.push(Row(vec![Scalar::I64(100), Scalar::I64(1)]), 1);
    core.push(InputId(0), &second).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);
    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(1, 2)]);
}

#[test]
fn rejected_push_does_not_advance_clock_or_count_late() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(InputId(0), WatermarkSpec { time_col: 0, lag: 0 })
        .unwrap();
    core.build_view(ViewId(0), &Plan::GroupCount {
        input: Box::new(source()),
        key: vec![1],
    })
    .unwrap();

    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(100), Scalar::I64(1)]), 1);
    core.push(InputId(0), &first).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);

    // Arity 3 vs the learned 2: rejected before the clock or late metric is touched.
    let mut bad = ChangeBatch::default();
    bad.push(Row(vec![Scalar::I64(1000), Scalar::I64(1), Scalar::I64(0)]), 1);
    assert!(matches!(
        core.push(InputId(0), &bad),
        Err(CoreError::Unsupported(_))
    ));
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);

    // Had the rejected push advanced the watermark to ~1001, ts=500 would be late.
    let mut mid = ChangeBatch::default();
    mid.push(Row(vec![Scalar::I64(500), Scalar::I64(1)]), 1);
    core.push(InputId(0), &mid).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);
    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(1, 2)]);
}
