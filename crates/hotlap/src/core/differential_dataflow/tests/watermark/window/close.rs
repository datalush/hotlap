//! Window close-boundary, large-time and state-GC tests.

use super::window_core;
use crate::core::{IncrementalCore, InputId, ViewId};
use crate::row::{ChangeBatch, Row, Scalar};

#[test]
fn window_closes_exactly_at_watermark_end() {
    let mut core = window_core();
    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(5)]), 1);
    core.push(InputId(0), &b).unwrap();
    let mut c = ChangeBatch::default();
    c.push(Row(vec![Scalar::I64(10), Scalar::I64(5)]), 1);
    core.push(InputId(0), &c).unwrap();
    let rows = core.snapshot(ViewId(0)).unwrap();
    assert_eq!(
        rows,
        vec![Row(vec![Scalar::I64(5), Scalar::I64(0), Scalar::I64(1)])],
        "window [0,10) must close with count 1"
    );
}

#[test]
fn window_large_event_time_does_not_panic() {
    let mut core = window_core();
    // `i64::MAX` scaled by TIME_SCALE overflows u64; the saturation guard must keep
    // `close_time` at/above the input capability so push returns Ok and does not panic.
    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(i64::MAX), Scalar::I64(5)]), 1);
    assert!(core.push(InputId(0), &b).is_ok());
}

#[test]
fn window_null_and_negative_time_bucket_to_window_zero() {
    let mut core = window_core();
    // `Null` and a negative event-time must not panic; both map to event-time 0 and
    // therefore land in the same `[0, 10)` bucket.
    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::Null, Scalar::I64(5)]), 1);
    b.push(Row(vec![Scalar::I64(-5), Scalar::I64(5)]), 1);
    core.push(InputId(0), &b).unwrap();

    // wm is still 0, so [0, 10) remains open; ts=10 closes it with count 2.
    let mut c = ChangeBatch::default();
    c.push(Row(vec![Scalar::I64(10), Scalar::I64(5)]), 1);
    core.push(InputId(0), &c).unwrap();
    assert_eq!(
        core.snapshot(ViewId(0)).unwrap(),
        vec![Row(vec![Scalar::I64(5), Scalar::I64(0), Scalar::I64(2)])]
    );
}

#[test]
fn closed_window_is_not_reemitted_and_state_freed() {
    let mut core = window_core();

    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(5)]), 1);
    b.push(Row(vec![Scalar::I64(3), Scalar::I64(5)]), 1);
    b.push(Row(vec![Scalar::I64(7), Scalar::I64(5)]), 1);
    core.push(InputId(0), &b).unwrap();
    // Closes [0,10) (count 3) and opens [10,20) with ts=12.
    let mut c = ChangeBatch::default();
    c.push(Row(vec![Scalar::I64(12), Scalar::I64(5)]), 1);
    core.push(InputId(0), &c).unwrap();
    assert_eq!(
        core.snapshot(ViewId(0)).unwrap(),
        vec![Row(vec![Scalar::I64(5), Scalar::I64(0), Scalar::I64(3)])]
    );

    // Advance the watermark well past [0,10): the [10,20) notification
    // fires `closed_buckets`; if [0,10) had not been freed it would be re-emitted.
    let mut d = ChangeBatch::default();
    d.push(Row(vec![Scalar::I64(25), Scalar::I64(5)]), 1);
    core.push(InputId(0), &d).unwrap();
    assert_eq!(
        core.snapshot(ViewId(0)).unwrap(),
        vec![
            Row(vec![Scalar::I64(5), Scalar::I64(0), Scalar::I64(3)]),
            Row(vec![Scalar::I64(5), Scalar::I64(10), Scalar::I64(1)]),
        ]
    );
}

#[test]
fn retraction_before_close_emits_nothing() {
    let mut core = window_core();

    // +1 followed by -1 of the same row before close: the [0,10) bucket consolidates to 0.
    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(5)]), 1);
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(5)]), -1);
    core.push(InputId(0), &b).unwrap();
    // Closes [0,10) and opens [10,20) (still open): nothing must be emitted.
    let mut c = ChangeBatch::default();
    c.push(Row(vec![Scalar::I64(12), Scalar::I64(5)]), 1);
    core.push(InputId(0), &c).unwrap();
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());
}
