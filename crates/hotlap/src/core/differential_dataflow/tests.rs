use std::collections::HashSet;

use super::DifferentialCore;
use super::circuit::validate;
use crate::core::{CoreError, IncrementalCore, InputId, ViewId};
use crate::harness::fixture;
use crate::plan::{Plan, Predicate};
use crate::row::{ChangeBatch, Row, Scalar};

/// A source plan over the usual single registered input.
fn source() -> Plan {
    Plan::Source(InputId(0))
}

/// Snapshot `view` as `(key, count)` pairs, asserting the `[key, count]` row shape.
fn snapshot_pairs(core: &mut DifferentialCore, view: ViewId) -> Vec<(i64, i64)> {
    core.snapshot(view)
        .unwrap()
        .into_iter()
        .map(|r| match (&r.0[0], &r.0[1]) {
            (Scalar::I64(k), Scalar::I64(c)) => (*k, *c),
            _ => panic!("shape"),
        })
        .collect()
}

#[test]
fn two_views_share_one_input() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupCount {
            input: Box::new(source()),
            key: vec![0],
        },
    )
    .unwrap();
    core.build_view(
        ViewId(1),
        &Plan::GroupCount {
            input: Box::new(source()),
            key: vec![1],
        },
    )
    .unwrap();

    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(10)]), 1);
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(20)]), 1);
    core.push(InputId(0), &b).unwrap();

    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(1, 2)]);
    assert_eq!(snapshot_pairs(&mut core, ViewId(1)), vec![(10, 1), (20, 1)]);
}

#[test]
fn filtered_group_count_matches_recompute() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    let plan = Plan::GroupCount {
        input: Box::new(Plan::Filter {
            input: Box::new(source()),
            pred: Predicate::Gt(0, 1),
        }),
        key: vec![0],
    };
    core.build_view(ViewId(0), &plan).unwrap();
    let mut b = ChangeBatch::default();
    for f in fixture() {
        b.push(Row(vec![Scalar::I64(f.key), Scalar::I64(f.value)]), f.diff);
    }
    core.push(InputId(0), &b).unwrap();
    // Filter key>1: rows for key 2 (+1,-1 => 0) and key 3 (+1) => [(3,1)]
    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(3, 1)]);
}

#[test]
fn stateful_retraction_without_rebuild() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    let plan = Plan::GroupCount {
        input: Box::new(source()),
        key: vec![0],
    };
    core.build_view(ViewId(0), &plan).unwrap();

    let mut first = ChangeBatch::default();
    first.push(Row(vec![Scalar::I64(1), Scalar::I64(10)]), 1);
    first.push(Row(vec![Scalar::I64(2), Scalar::I64(20)]), 1);
    core.push(InputId(0), &first).unwrap();
    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(1, 1), (2, 1)]);

    // A later push retracts key 2; the live session updates without rebuild/replay.
    let mut second = ChangeBatch::default();
    second.push(Row(vec![Scalar::I64(2), Scalar::I64(20)]), -1);
    core.push(InputId(0), &second).unwrap();
    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(1, 1)]);
}

#[test]
fn validate_rejects_out_of_range_columns_and_unregistered_sources() {
    let inputs: HashSet<InputId> = HashSet::from([InputId(0)]);

    let project = Plan::Project {
        input: Box::new(source()),
        cols: vec![5],
    };
    assert!(matches!(
        validate(&project, Some(2), &inputs),
        Err(CoreError::Unsupported(_))
    ));

    let group = Plan::GroupCount {
        input: Box::new(source()),
        key: vec![9],
    };
    assert!(matches!(
        validate(&group, Some(2), &inputs),
        Err(CoreError::Unsupported(_))
    ));

    // A plan naming an input that was never registered is rejected.
    let unregistered = Plan::GroupCount {
        input: Box::new(Plan::Source(InputId(7))),
        key: vec![0],
    };
    assert!(matches!(
        validate(&unregistered, Some(2), &inputs),
        Err(CoreError::Unsupported(_))
    ));

    // In-range chains are accepted; `Project` narrows the arity seen downstream.
    let ok = Plan::GroupCount {
        input: Box::new(Plan::Project {
            input: Box::new(source()),
            cols: vec![1, 0],
        }),
        key: vec![0, 1],
    };
    assert!(validate(&ok, Some(2), &inputs).is_ok());
}

#[test]
fn unknown_input_and_view_error() {
    let mut core = DifferentialCore::new().unwrap();
    let batch = ChangeBatch::default();
    assert!(matches!(
        core.push(InputId(7), &batch),
        Err(CoreError::Unsupported(_))
    ));
    assert!(matches!(
        core.snapshot(ViewId(7)),
        Err(CoreError::Unsupported(_))
    ));
}

#[test]
fn commands_after_worker_stop_report_infrastructure_error() {
    let mut core = DifferentialCore::new().unwrap();
    core.stop_worker_for_test();
    let batch = ChangeBatch::default();
    assert!(matches!(
        core.push(InputId(0), &batch),
        Err(CoreError::Infrastructure(_))
    ));
    assert!(matches!(
        core.snapshot(ViewId(0)),
        Err(CoreError::Infrastructure(_))
    ));
}

#[test]
fn project_then_group_count_snapshot() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    // Group by the projected value column: `[k, v] -> [v] -> [v, count]`.
    let plan = Plan::GroupCount {
        input: Box::new(Plan::Project {
            input: Box::new(source()),
            cols: vec![1],
        }),
        key: vec![0],
    };
    core.build_view(ViewId(0), &plan).unwrap();

    let mut batch = ChangeBatch::default();
    batch.push(Row(vec![Scalar::I64(1), Scalar::I64(10)]), 1);
    batch.push(Row(vec![Scalar::I64(2), Scalar::I64(10)]), 1);
    batch.push(Row(vec![Scalar::I64(3), Scalar::I64(20)]), 1);
    core.push(InputId(0), &batch).unwrap();

    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(10, 2), (20, 1)]);
}
