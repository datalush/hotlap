use super::DifferentialCore;
use super::circuit::validate;
use crate::core::{CoreError, IncrementalCore, ViewId};
use crate::harness::fixture;
use crate::plan::{Plan, Predicate};
use crate::row::{ChangeBatch, Row, Scalar};

#[test]
fn filtered_group_count_matches_recompute() {
    let mut core = DifferentialCore::new().unwrap();
    let plan = Plan::GroupCount {
        input: Box::new(Plan::Filter {
            input: Box::new(Plan::Scan),
            pred: Predicate::Gt(0, 1),
        }),
        key: vec![0],
    };
    core.build_view(ViewId(0), &plan).unwrap();
    let mut b = ChangeBatch::default();
    for f in fixture() {
        b.push(Row(vec![Scalar::I64(f.key), Scalar::I64(f.value)]), f.diff);
    }
    core.push(ViewId(0), &b).unwrap();
    let got: Vec<(i64, i64)> = core
        .snapshot(ViewId(0))
        .unwrap()
        .into_iter()
        .map(|r| match (&r.0[0], &r.0[1]) {
            (Scalar::I64(k), Scalar::I64(c)) => (*k, *c),
            _ => panic!("shape"),
        })
        .collect();
    // Filter key>1: rows for key 2 (+1,-1 => 0) and key 3 (+1) => [(3,1)]
    assert_eq!(got, vec![(3, 1)]);
}

#[test]
fn stateful_retraction_without_rebuild() {
    let mut core = DifferentialCore::new().unwrap();
    let plan = Plan::GroupCount {
        input: Box::new(Plan::Scan),
        key: vec![0],
    };
    core.build_view(ViewId(0), &plan).unwrap();

    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(1), Scalar::I64(10)]), 1);
    first.push(Row(vec![Scalar::I64(2), Scalar::I64(20)]), 1);
    core.push(ViewId(0), &first).unwrap();
    let after_first: Vec<(i64, i64)> = snapshot_pairs(&mut core);
    assert_eq!(after_first, vec![(1, 1), (2, 1)]);

    // A later push retracts key 2; the live session updates without rebuild/replay.
    let mut second = ChangeBatch::default();
    second.push(Row(vec![Scalar::I64(2), Scalar::I64(20)]), -1);
    core.push(ViewId(0), &second).unwrap();
    assert_eq!(snapshot_pairs(&mut core), vec![(1, 1)]);
}

/// Snapshot as `(key, count)` pairs, asserting the `[key, count]` row shape.
fn snapshot_pairs(core: &mut DifferentialCore) -> Vec<(i64, i64)> {
    core.snapshot(ViewId(0))
        .unwrap()
        .into_iter()
        .map(|r| match (&r.0[0], &r.0[1]) {
            (Scalar::I64(k), Scalar::I64(c)) => (*k, *c),
            _ => panic!("shape"),
        })
        .collect()
}

#[test]
fn validate_rejects_out_of_range_columns() {
    let project = Plan::Project {
        input: Box::new(Plan::Scan),
        cols: vec![5],
    };
    assert!(matches!(
        validate(&project, Some(2)),
        Err(CoreError::Unsupported(_))
    ));

    let group = Plan::GroupCount {
        input: Box::new(Plan::Scan),
        key: vec![9],
    };
    assert!(matches!(
        validate(&group, Some(2)),
        Err(CoreError::Unsupported(_))
    ));

    // In-range chains are accepted; `Project` narrows the arity seen downstream.
    let ok = Plan::GroupCount {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Scan),
            cols: vec![1, 0],
        }),
        key: vec![0, 1],
    };
    assert!(validate(&ok, Some(2)).is_ok());
}

#[test]
fn unknown_view_errors_on_push_and_snapshot() {
    let mut core = DifferentialCore::new().unwrap();
    let batch = ChangeBatch::default();
    assert!(matches!(
        core.push(ViewId(7), &batch),
        Err(CoreError::Unsupported(_))
    ));
    assert!(matches!(
        core.snapshot(ViewId(7)),
        Err(CoreError::Unsupported(_))
    ));
}

#[test]
fn project_then_group_count_snapshot() {
    let mut core = DifferentialCore::new().unwrap();
    // Group by the projected value column: `[k, v] -> [v] -> [v, count]`.
    let plan = Plan::GroupCount {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Scan),
            cols: vec![1],
        }),
        key: vec![0],
    };
    core.build_view(ViewId(0), &plan).unwrap();

    let mut batch = ChangeBatch::default();
    batch.push(Row(vec![Scalar::I64(1), Scalar::I64(10)]), 1);
    batch.push(Row(vec![Scalar::I64(2), Scalar::I64(10)]), 1);
    batch.push(Row(vec![Scalar::I64(3), Scalar::I64(20)]), 1);
    core.push(ViewId(0), &batch).unwrap();

    assert_eq!(snapshot_pairs(&mut core), vec![(10, 2), (20, 1)]);
}
