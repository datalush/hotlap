//! Join-specific core tests.

use super::super::DifferentialCore;
use super::super::validate::validate;
use crate::core::{CoreError, IncrementalCore, InputId, ViewId};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row, Scalar};
use std::collections::HashSet;

/// Snapshot `view` as rows, sorted for a stable comparison.
fn snapshot_rows(core: &mut DifferentialCore, view: ViewId) -> Vec<Vec<Scalar>> {
    let mut rows: Vec<Vec<Scalar>> = core
        .snapshot(view)
        .unwrap()
        .into_iter()
        .map(|row| row.0)
        .collect();
    rows.sort();
    rows
}

#[test]
fn inner_join_matches_recompute_with_retraction() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap(); // users: [id, name]
    core.register_input(InputId(1)).unwrap(); // orders: [user_id, amount]
    let plan = Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![0],
        right_key: vec![0],
    };
    core.build_view(ViewId(0), &plan).unwrap();

    let mut users = ChangeBatch::default();
    users.push(Row(vec![Scalar::I64(1), Scalar::I64(100)]), 1);
    users.push(Row(vec![Scalar::I64(2), Scalar::I64(200)]), 1);
    core.push(InputId(0), &users).unwrap();

    let mut orders = ChangeBatch::default();
    orders.push(Row(vec![Scalar::I64(1), Scalar::I64(7)]), 1);
    orders.push(Row(vec![Scalar::I64(1), Scalar::I64(9)]), 1);
    orders.push(Row(vec![Scalar::I64(3), Scalar::I64(5)]), 1); // sin match
    core.push(InputId(1), &orders).unwrap();

    let mut retract = ChangeBatch::default();
    retract.push(Row(vec![Scalar::I64(1), Scalar::I64(9)]), -1);
    core.push(InputId(1), &retract).unwrap();

    assert_eq!(
        snapshot_rows(&mut core, ViewId(0)),
        vec![vec![
            Scalar::I64(1),
            Scalar::I64(100),
            Scalar::I64(1),
            Scalar::I64(7)
        ]]
    );
}

#[test]
fn validate_rejects_out_of_range_join_key() {
    let inputs: HashSet<InputId> = HashSet::from([InputId(0), InputId(1)]);
    let join = Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![9],
        right_key: vec![0],
    };
    assert!(validate(&join, Some(2), &inputs, None).is_err());
    assert_eq!(validate(&join, None, &inputs, None).unwrap(), None);
}

#[test]
fn join_accepts_sides_with_different_arities() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap(); // [id, name]          aridad 2
    core.register_input(InputId(1)).unwrap(); // [user_id, amt, tag] aridad 3
    let plan = Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![0],
        right_key: vec![0],
    };
    core.build_view(ViewId(0), &plan).unwrap();
    let mut l = ChangeBatch::default();
    l.push(Row(vec![Scalar::I64(1), Scalar::I64(100)]), 1);
    core.push(InputId(0), &l).unwrap();
    let mut r = ChangeBatch::default();
    r.push(Row(vec![Scalar::I64(1), Scalar::I64(7), Scalar::I64(0)]), 1);
    core.push(InputId(1), &r).unwrap();
    assert_eq!(
        snapshot_rows(&mut core, ViewId(0)),
        vec![vec![
            Scalar::I64(1),
            Scalar::I64(100),
            Scalar::I64(1),
            Scalar::I64(7),
            Scalar::I64(0)
        ]]
    );
}

#[test]
fn join_rejects_mismatched_key_lengths() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    let plan = Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![0, 1],
        right_key: vec![0],
    };
    assert!(matches!(
        core.build_view(ViewId(0), &plan),
        Err(CoreError::Unsupported(_))
    ));
}
