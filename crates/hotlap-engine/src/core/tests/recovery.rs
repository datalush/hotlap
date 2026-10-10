//! Restore, poisoning and counter-rejection tests.

use super::*;

#[test]
fn restore_rejects_unknown_format_version() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();

    let mut snapshot = core.checkpoint().unwrap();
    snapshot.format_version = u32::MAX;

    let mut target = EngineCore::new();
    assert!(target.restore(&snapshot).is_err());
}

#[test]
fn snapshot_validation_rebuilds_without_mutating_live_state_or_metrics() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    let snapshot = core.checkpoint().unwrap();
    let metrics = core.metrics().snapshot();

    core.validate_snapshot(&snapshot).unwrap();

    assert_eq!(core.metrics().snapshot(), metrics);
    assert_eq!(core.checkpoint().unwrap(), snapshot);
    let mut restored = EngineCore::new();
    restored.restore(&snapshot).unwrap();
    assert_eq!(restored.checkpoint().unwrap(), snapshot);
}

#[test]
fn snapshot_validation_rejects_corrupt_ipc_and_unknown_layout() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    let before = core.checkpoint().unwrap();
    let metrics = core.metrics().snapshot();

    let mut corrupt_ipc = before.clone();
    corrupt_ipc.views[0].output.as_mut().unwrap().ipc = b"broken Arrow IPC".to_vec();
    assert!(matches!(
        core.validate_snapshot(&corrupt_ipc),
        Err(crate::EngineError::Infrastructure(_))
    ));
    let mut corrupt_schema = before.clone();
    corrupt_schema.inputs[0].schema = Some(b"broken schema IPC".to_vec());
    assert!(matches!(
        core.validate_snapshot(&corrupt_schema),
        Err(crate::EngineError::Infrastructure(_))
    ));
    let mut mismatched_operator = before.clone();
    mismatched_operator.views[0].operators[0] = None;
    assert!(core.validate_snapshot(&mismatched_operator).is_err());
    let mut unknown = before.clone();
    unknown.format_version = u32::MAX;
    assert!(matches!(
        core.validate_snapshot(&unknown),
        Err(crate::EngineError::Unsupported(_))
    ));
    assert_eq!(core.metrics().snapshot(), metrics);
    assert_eq!(core.checkpoint().unwrap(), before);
}

/// A failing push in one view must not surface the other views' partial state.
#[test]
fn failed_push_poisons_snapshot_push_and_take_changes() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &Plan::Source(InputId(0)))
        .unwrap();
    core.build_view(ViewId(1), &Plan::Source(InputId(0)))
        .unwrap();

    // Both views retain `i64::MAX` for key 1; the next push overflows the
    // summed diff of whichever view is pushed last.
    let saturated = zset(&[(1, i64::MAX)]);
    core.push(InputId(0), &saturated).unwrap();
    assert!(core.push(InputId(0), &saturated).is_err());

    // The core is poisoned: no snapshot, push or change drain may observe or
    // compound the partially-applied state.
    assert!(core.failed);
    assert!(core.snapshot(ViewId(0)).is_err());
    assert!(core.snapshot(ViewId(1)).is_err());
    assert!(core.push(InputId(0), &zset(&[(2, 1)])).is_err());
    assert!(core.take_changes(ViewId(0)).is_err());
    // Checkpointing partial state and building a view over it are barred too:
    // recovery would otherwise restore the corruption.
    assert!(core.checkpoint().is_err());
    assert!(
        core.build_view(ViewId(2), &Plan::Source(InputId(0)))
            .is_err()
    );
    // Counter reads must not serve a partial count either.
    assert!(core.late_dropped(InputId(0)).is_err());
    assert!(core.window_late_closed(ViewId(0)).is_err());
}

/// `late_dropped` rejects an input the engine never registered.
#[test]
fn late_dropped_rejects_an_unknown_input() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();

    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);
    assert!(core.late_dropped(InputId(7)).is_err());
}

/// The multi-view path without a failure is unchanged (differential).
#[test]
fn multi_view_push_keeps_every_view_correct() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &Plan::Source(InputId(0)))
        .unwrap();
    core.build_view(ViewId(1), &group_plan()).unwrap();

    core.push(InputId(0), &zset(&[(1, 2), (2, 3)])).unwrap();

    assert!(!core.failed);
    assert_eq!(core.snapshot(ViewId(0)).unwrap().len(), 2);
    assert_eq!(core.snapshot(ViewId(1)).unwrap().len(), 2);
}
