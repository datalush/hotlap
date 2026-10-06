//! Event-time clock, late-drop and drain-visibility tests.

mod clock;
mod late;

use super::super::{snapshot_pairs, source};
use crate::core::differential_dataflow::DifferentialCore;
use crate::core::{IncrementalCore, InputId, ViewId, WatermarkSpec};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row, Scalar};

#[test]
fn event_time_drops_late_and_advances_watermark() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        WatermarkSpec {
            time_col: 0,
            lag: 0,
        },
    )
    .unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupCount {
            input: Box::new(source()),
            key: vec![1],
        },
    )
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
    core.declare_watermark(
        InputId(0),
        WatermarkSpec {
            time_col: 0,
            lag: 0,
        },
    )
    .unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupCount {
            input: Box::new(source()),
            key: vec![1],
        },
    )
    .unwrap();
    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(100), Scalar::I64(1)]), 1);
    core.push(InputId(0), &first).unwrap();
    // ts == wm (100) is not late.
    let mut edge = ChangeBatch::default();
    edge.push(Row(vec![Scalar::I64(100), Scalar::I64(1)]), 1);
    core.push(InputId(0), &edge).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);
}

#[test]
fn event_time_empty_batch_does_not_move_watermark() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        WatermarkSpec {
            time_col: 0,
            lag: 0,
        },
    )
    .unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupCount {
            input: Box::new(source()),
            key: vec![1],
        },
    )
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
