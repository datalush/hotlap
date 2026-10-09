//! Checkpoint/restore of `sum`/`avg` state that holds special float inputs.

use super::{current, reducer, zset};

#[test]
fn checkpoint_restore_keeps_special_state_then_retracts() {
    let mut source = reducer();
    source
        .apply(&zset(&[(1, f64::NAN, 1), (1, 2.0, 1)]))
        .unwrap();
    let state = source.export_state().unwrap();
    let bytes = crate::encode_framed(&state).unwrap();
    let decoded: hotlap_core::snapshot::GroupState = crate::decode_framed(&bytes).unwrap();

    let mut restored = reducer();
    restored.import_state(&decoded).unwrap();
    let out = restored.apply(&zset(&[(1, f64::NAN, -1)])).unwrap();
    assert_eq!(current(&out), vec![(1, Some(2.0), Some(2.0))]);
}

#[test]
fn checkpoint_restore_keeps_infinite_state_then_retracts() {
    let mut source = reducer();
    source
        .apply(&zset(&[
            (1, f64::INFINITY, 1),
            (1, f64::NEG_INFINITY, 1),
            (1, 5.0, 1),
        ]))
        .unwrap();
    let state = source.export_state().unwrap();
    let mut restored = reducer();
    restored.import_state(&state).unwrap();

    let out = restored
        .apply(&zset(&[(1, f64::INFINITY, -1), (1, f64::NEG_INFINITY, -1)]))
        .unwrap();
    assert_eq!(current(&out), vec![(1, Some(5.0), Some(5.0))]);
}
