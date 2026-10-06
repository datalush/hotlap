//! Join-specific core tests.

use super::super::DifferentialCore;
use super::super::validate::validate;
use crate::core::{CoreError, IncrementalCore, InputId, ViewId};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row, Scalar};
use std::collections::{HashMap, HashSet};

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
    let left_known = HashMap::from([(InputId(0), 2usize)]);
    assert!(validate(&join, &left_known, &inputs).is_err());
    assert_eq!(validate(&join, &HashMap::new(), &inputs).unwrap(), None);
}

#[test]
fn out_of_range_column_above_join_does_not_kill_worker() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap(); // join left
    core.register_input(InputId(1)).unwrap(); // join right
    core.register_input(InputId(2)).unwrap(); // unrelated input, for liveness
    // A column index far past the join's output arity, projected *above* the join.
    let bad = Plan::GroupCount {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Join {
                left: Box::new(Plan::Source(InputId(0))),
                right: Box::new(Plan::Source(InputId(1))),
                left_key: vec![0],
                right_key: vec![0],
            }),
            cols: vec![99],
        }),
        key: vec![0],
    };
    core.build_view(ViewId(0), &bad).unwrap();
    let live = Plan::GroupCount {
        input: Box::new(Plan::Source(InputId(2))),
        key: vec![0],
    };
    core.build_view(ViewId(1), &live).unwrap();

    // Left arity learned; the join's output arity is still unknown (right unseen),
    // so the bad index above it is not yet determinable and this push is accepted.
    let mut left = ChangeBatch::default();
    left.push(Row(vec![Scalar::I64(1), Scalar::I64(10)]), 1);
    core.push(InputId(0), &left).unwrap();

    // Right arity arrives: the join output arity is now 2 + 2 = 4, so column 99 is
    // rejected before any data reaches the projection that would index it.
    let mut right = ChangeBatch::default();
    right.push(Row(vec![Scalar::I64(1), Scalar::I64(7)]), 1);
    assert!(matches!(
        core.push(InputId(1), &right),
        Err(CoreError::Unsupported(_))
    ));

    // The worker survived: an unrelated input still accepts data and snapshots.
    let mut other = ChangeBatch::default();
    other.push(Row(vec![Scalar::I64(5), Scalar::I64(1)]), 1);
    core.push(InputId(2), &other).unwrap();
    assert_eq!(
        snapshot_rows(&mut core, ViewId(1)),
        vec![vec![Scalar::I64(5), Scalar::I64(1)]]
    );

    // A repeated push to the bad input still reports Unsupported, not Infrastructure.
    let mut again = ChangeBatch::default();
    again.push(Row(vec![Scalar::I64(2), Scalar::I64(9)]), 1);
    assert!(matches!(
        core.push(InputId(1), &again),
        Err(CoreError::Unsupported(_))
    ));
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
