//! Retracting null values must not churn the visible extremes.

use super::{int64_values, reducer, zset};

#[test]
fn retracting_a_null_value_emits_nothing() {
    let mut reducer = reducer();
    reducer
        .apply(&zset(
            &[1, 1, 1],
            int64_values(&[Some(5), None, Some(8)]),
            &[1, 1, 1],
        ))
        .unwrap();
    let out = reducer
        .apply(&zset(&[1], int64_values(&[None]), &[-1]))
        .unwrap();
    assert!(out.is_empty(), "a null value does not change min/max");
}
