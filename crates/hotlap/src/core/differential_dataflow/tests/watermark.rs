//! Watermark-declaration core tests.

mod event_time;
mod window;

use super::super::DifferentialCore;
use super::group_by;
use crate::core::{CoreError, IncrementalCore, InputId, ViewId, WatermarkSpec};
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
fn declare_watermark_negative_lag_errors() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    assert!(matches!(
        core.declare_watermark(
            InputId(0),
            WatermarkSpec {
                time_col: 0,
                lag: -1,
            },
        ),
        Err(CoreError::Unsupported(_))
    ));
}

#[test]
fn declare_watermark_duplicate_keeps_original_spec() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        WatermarkSpec {
            time_col: 1,
            lag: 0,
        },
    )
    .unwrap();
    // A rejected duplicate must not overwrite the stored spec: had it done so the
    // event-time column would now be 9 (out of range, treated as 0).
    assert!(matches!(
        core.declare_watermark(
            InputId(0),
            WatermarkSpec {
                time_col: 9,
                lag: 9,
            },
        ),
        Err(CoreError::Unsupported(_))
    ));
    core.build_view(ViewId(0), &group_by(0)).unwrap();

    // Col 1 as event time: wm = 100, so a ts=90 insertion is late. Under the
    // overwritten col-9 spec (=> 0) it would not be classified late.
    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(0), Scalar::I64(100)]), 1);
    core.push(InputId(0), &first).unwrap();
    let mut late = ChangeBatch::default();
    late.push(Row(vec![Scalar::I64(0), Scalar::I64(90)]), 1);
    core.push(InputId(0), &late).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 1);
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
