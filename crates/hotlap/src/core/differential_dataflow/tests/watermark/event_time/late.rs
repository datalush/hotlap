//! Late insertion/retraction handling tests.

use super::super::super::{snapshot_pairs, source};
use crate::core::differential_dataflow::DifferentialCore;
use crate::core::{IncrementalCore, InputId, ViewId, WatermarkSpec};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row, Scalar};

/// Build a group-count core over input 0 with event-time on col 0 (lag 0).
fn group_count_event_time() -> DifferentialCore {
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
    core
}

#[test]
fn late_retraction_is_applied_and_not_counted() {
    let mut core = group_count_event_time();

    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(100), Scalar::I64(1)]), 1);
    core.push(InputId(0), &first).unwrap();
    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(1, 1)]);

    // ts=90 < wm=100, diff=-1: a late retraction must be kept (applied), not
    // dropped/counted, otherwise the key would stay at count 1.
    let mut late = ChangeBatch::default();
    late.push(Row(vec![Scalar::I64(90), Scalar::I64(1)]), -1);
    core.push(InputId(0), &late).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);
    assert!(snapshot_pairs(&mut core, ViewId(0)).is_empty());
}

#[test]
fn late_insertion_is_dropped_and_counted() {
    let mut core = group_count_event_time();

    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(100), Scalar::I64(1)]), 1);
    core.push(InputId(0), &first).unwrap();

    let mut late = ChangeBatch::default();
    late.push(Row(vec![Scalar::I64(90), Scalar::I64(1)]), 1);
    core.push(InputId(0), &late).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 1);
    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(1, 1)]);
}
