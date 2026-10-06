mod join;

use std::collections::HashSet;

use super::DifferentialCore;
use super::validate::validate;
use crate::core::{CoreError, IncrementalCore, InputId, ViewId};
use crate::harness::fixture;
use crate::plan::{Plan, Predicate};
use crate::row::{ChangeBatch, Row, Scalar};

/// A source plan over the usual single registered input.
fn source() -> Plan {
    Plan::Source(InputId(0))
}

/// `GROUP BY key` over the usual source.
fn group_by(key: usize) -> Plan {
    Plan::GroupCount {
        input: Box::new(source()),
        key: vec![key],
    }
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
    core.build_view(ViewId(0), &group_by(0)).unwrap();
    core.build_view(ViewId(1), &group_by(1)).unwrap();

    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(10)]), 1);
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(20)]), 1);
    core.push(InputId(0), &b).unwrap();

    assert_eq!(snapshot_pairs(&mut core, ViewId(0)), vec![(1, 2)]);
    assert_eq!(snapshot_pairs(&mut core, ViewId(1)), vec![(10, 1), (20, 1)]);
}

#[test]
fn schema_freezes_after_first_push_at_core_level() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_by(0)).unwrap();
    let mut b = ChangeBatch::default();
    b.push(Row(vec![Scalar::I64(1), Scalar::I64(1)]), 1);
    core.push(InputId(0), &b).unwrap();

    assert!(matches!(
        core.register_input(InputId(1)),
        Err(CoreError::Unsupported(_))
    ));
    assert!(matches!(
        core.build_view(ViewId(1), &group_by(0)),
        Err(CoreError::Unsupported(_))
    ));
}

#[test]
fn build_view_rejects_unregistered_source() {
    let mut core = DifferentialCore::new().unwrap();
    let missing = Plan::GroupCount {
        input: Box::new(Plan::Source(InputId(9))),
        key: vec![0],
    };
    assert!(matches!(
        core.build_view(ViewId(0), &missing),
        Err(CoreError::Unsupported(_))
    ));
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
    core.build_view(ViewId(0), &group_by(0)).unwrap();

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
fn validate_rejects_out_of_range_columns() {
    let inputs: HashSet<InputId> = HashSet::from([InputId(0)]);

    let project = Plan::Project {
        input: Box::new(source()),
        cols: vec![5],
    };
    assert!(validate(&project, Some(2), &inputs, None).is_err());

    let group = Plan::GroupCount {
        input: Box::new(source()),
        key: vec![9],
    };
    assert!(validate(&group, Some(2), &inputs, None).is_err());
}

#[test]
fn unknown_input_and_view_error() {
    let mut core = DifferentialCore::new().unwrap();
    let batch = ChangeBatch::default();
    assert!(core.push(InputId(7), &batch).is_err());
    assert!(core.snapshot(ViewId(7)).is_err());
}

#[test]
fn declared_but_unbuilt_view_reports_specific_error() {
    let mut core = DifferentialCore::new().unwrap();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_by(0)).unwrap();
    match core.snapshot(ViewId(0)) {
        Err(CoreError::Unsupported(msg)) => assert_eq!(msg, "view not built yet"),
        other => panic!("expected 'view not built yet', got {other:?}"),
    }
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
