//! Tumbling-window IR, validation and emit-on-close/GC core tests.

use std::collections::{HashMap, HashSet};

use super::super::{source, validate};
use crate::core::differential_dataflow::DifferentialCore;
use crate::core::{CoreError, IncrementalCore, InputId, ViewId, WatermarkSpec};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row, Scalar};

/// Core with input 0 watermarked on col 0 and a `TumbleCount` view (key col 1, size 10).
fn window_core() -> DifferentialCore {
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
        &Plan::TumbleCount {
            input: Box::new(source()),
            key: vec![1],
            time_col: 0,
            size: 10,
        },
    )
    .unwrap();
    core
}

#[test]
fn validate_tumble_count_bounds_and_size() {
    let inputs: HashSet<InputId> = HashSet::from([InputId(0)]);
    let arities = HashMap::from([(InputId(0), 2usize)]);
    let ok = Plan::TumbleCount {
        input: Box::new(source()),
        key: vec![1],
        time_col: 0,
        size: 10,
    };
    assert_eq!(validate(&ok, &arities, &inputs).unwrap(), Some(3));
    let bad_col = Plan::TumbleCount {
        input: Box::new(source()),
        key: vec![1],
        time_col: 9,
        size: 10,
    };
    assert!(validate(&bad_col, &arities, &inputs).is_err());
    let bad_size = Plan::TumbleCount {
        input: Box::new(source()),
        key: vec![1],
        time_col: 0,
        size: 0,
    };
    assert!(validate(&bad_size, &arities, &inputs).is_err());
}

#[test]
fn tumbling_window_emits_on_close_and_frees_state() {
    let mut core = window_core();

    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(5)]), 1);
    b.push(Row(vec![Scalar::I64(3), Scalar::I64(5)]), 1);
    b.push(Row(vec![Scalar::I64(7), Scalar::I64(5)]), 1);
    core.push(InputId(0), &b).unwrap();
    assert!(
        core.snapshot(ViewId(0)).unwrap().is_empty(),
        "ventana aún abierta"
    );

    let mut c = ChangeBatch::default();
    c.push(Row(vec![Scalar::I64(12), Scalar::I64(5)]), 1);
    core.push(InputId(0), &c).unwrap();
    assert_eq!(
        core.snapshot(ViewId(0)).unwrap(),
        vec![Row(vec![Scalar::I64(5), Scalar::I64(0), Scalar::I64(3)])]
    );
}

#[test]
fn window_rejected_without_event_time() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::TumbleCount {
            input: Box::new(source()),
            key: vec![1],
            time_col: 0,
            size: 10,
        },
    )
    .unwrap();
    // Sin watermark (modo epoch), el build debe rechazar un plan con ventana.
    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(0), Scalar::I64(1)]), 1);
    assert!(matches!(
        core.push(InputId(0), &b),
        Err(CoreError::Unsupported(_))
    ));
}

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
        "ventana [0,10) debe cerrar con count 1"
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
fn closed_window_is_not_reemitted_and_state_freed() {
    let mut core = window_core();

    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(5)]), 1);
    b.push(Row(vec![Scalar::I64(3), Scalar::I64(5)]), 1);
    b.push(Row(vec![Scalar::I64(7), Scalar::I64(5)]), 1);
    core.push(InputId(0), &b).unwrap();
    // Cierra [0,10) (count 3) y abre [10,20) con el ts=12.
    let mut c = ChangeBatch::default();
    c.push(Row(vec![Scalar::I64(12), Scalar::I64(5)]), 1);
    core.push(InputId(0), &c).unwrap();
    assert_eq!(
        core.snapshot(ViewId(0)).unwrap(),
        vec![Row(vec![Scalar::I64(5), Scalar::I64(0), Scalar::I64(3)])]
    );

    // Avanza el watermark muy por encima de [0,10): la notificación de [10,20)
    // dispara `closed_buckets`; si [0,10) no se hubiera liberado, se re-emitiría.
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

    // +1 seguido de -1 de la misma fila antes del cierre: el cubo de [0,10) consolida a 0.
    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(5)]), 1);
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(5)]), -1);
    core.push(InputId(0), &b).unwrap();
    // Cierra [0,10) y abre [10,20) (aún abierta): nada debe emitirse.
    let mut c = ChangeBatch::default();
    c.push(Row(vec![Scalar::I64(12), Scalar::I64(5)]), 1);
    core.push(InputId(0), &c).unwrap();
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());
}
