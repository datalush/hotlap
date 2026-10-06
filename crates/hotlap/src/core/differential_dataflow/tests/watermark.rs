//! Watermark-declaration core tests.

use super::super::DifferentialCore;
use super::{group_by, snapshot_pairs, source};
use crate::core::{CoreError, IncrementalCore, InputId, ViewId, WatermarkSpec};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row, Scalar};

#[test]
fn declare_watermark_before_push_and_reject_after() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_by(1)).unwrap();
    let spec = WatermarkSpec {
        time_col: 1,
        lag: 2,
    };
    core.declare_watermark(InputId(0), spec).unwrap();

    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(0), Scalar::I64(100)]), 1);
    core.push(InputId(0), &b).unwrap();

    // Ya arrancado: declarar tarde debe fallar.
    assert!(matches!(
        core.declare_watermark(InputId(0), spec),
        Err(CoreError::Unsupported(_))
    ));
}

#[test]
fn declare_watermark_unknown_input_errors() {
    let mut core = DifferentialCore::new().unwrap();
    let spec = WatermarkSpec {
        time_col: 0,
        lag: 1,
    };
    assert!(matches!(
        core.declare_watermark(InputId(7), spec),
        Err(CoreError::Unsupported(_))
    ));
}

#[test]
fn declare_watermark_duplicate_errors() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    let spec = WatermarkSpec {
        time_col: 0,
        lag: 1,
    };
    core.declare_watermark(InputId(0), spec).unwrap();
    assert!(matches!(
        core.declare_watermark(InputId(0), spec),
        Err(CoreError::Unsupported(_))
    ));
}

#[test]
fn declare_watermark_subset_rejected_at_build() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(ViewId(0), &group_by(0)).unwrap();
    let spec = WatermarkSpec {
        time_col: 1,
        lag: 0,
    };
    core.declare_watermark(InputId(0), spec).unwrap();

    // Only one of two inputs has a watermark: the build must refuse the mix.
    let batch = ChangeBatch::default();
    assert!(matches!(
        core.push(InputId(0), &batch),
        Err(CoreError::Unsupported(_))
    ));
}

#[test]
fn event_time_drops_late_and_advances_watermark() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(InputId(0), crate::core::WatermarkSpec { time_col: 0, lag: 0 })
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
    core.declare_watermark(InputId(0), crate::core::WatermarkSpec { time_col: 0, lag: 0 })
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
    core.declare_watermark(InputId(0), crate::core::WatermarkSpec { time_col: 0, lag: 0 })
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
