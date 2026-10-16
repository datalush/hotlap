use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, ZSetBatch};

use super::{EngineCore, ForcePrivateSharedArrangements, diff_values};

fn float_rows(rows: &[(i64, f64, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))) as ArrayRef,
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|row| row.1))) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(
        batch,
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
    )
    .unwrap()
}

fn duplicate_root() -> Plan {
    Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![0],
        right_key: vec![0],
    }
}

fn values(core: &mut EngineCore, view: ViewId) -> Vec<(i64, u64, i64, u64, i64)> {
    let snapshot = core.snapshot(view).unwrap();
    let keys = snapshot
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left = snapshot
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let right_key = snapshot
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right = snapshot
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let diffs = diff_values(&snapshot);
    (0..snapshot.batch.num_rows())
        .map(|i| {
            (
                keys.value(i),
                left.value(i).to_bits(),
                right_key.value(i),
                right.value(i).to_bits(),
                diffs[i],
            )
        })
        .collect()
}

#[test]
fn shared_float_payload_bits_and_signed_taps_match_forced_private() {
    let mut shared = EngineCore::new();
    let mut private = EngineCore::new();
    for core in [&mut shared, &mut private] {
        core.register_input(InputId(0)).unwrap();
        core.register_input(InputId(1)).unwrap();
        for view in [ViewId(70), ViewId(71)] {
            core.build_view(view, &duplicate_root()).unwrap();
            core.tap_view(view).unwrap();
        }
    }
    {
        let _force = ForcePrivateSharedArrangements::new();
        private
            .push(
                InputId(0),
                &float_rows(&[
                    (1, 0.0, 1),
                    (2, f64::NAN, 1),
                    (3, f64::INFINITY, 1),
                    (4, -0.0, 1),
                ]),
            )
            .unwrap();
        private
            .push(
                InputId(1),
                &float_rows(&[
                    (1, 5.0, 1),
                    (2, f64::NEG_INFINITY, 1),
                    (3, 2.0, 1),
                    (4, f64::NAN, 1),
                ]),
            )
            .unwrap();
    }
    shared
        .push(
            InputId(0),
            &float_rows(&[
                (1, 0.0, 1),
                (2, f64::NAN, 1),
                (3, f64::INFINITY, 1),
                (4, -0.0, 1),
            ]),
        )
        .unwrap();
    shared
        .push(
            InputId(1),
            &float_rows(&[
                (1, 5.0, 1),
                (2, f64::NEG_INFINITY, 1),
                (3, 2.0, 1),
                (4, f64::NAN, 1),
            ]),
        )
        .unwrap();
    assert_eq!(shared.shared_arrangements.owner_count(), 2);
    assert_eq!(private.shared_arrangements.owner_count(), 0);
    let expected = vec![
        (1, 0.0f64.to_bits(), 1, 5.0f64.to_bits(), 1),
        (2, f64::NAN.to_bits(), 2, f64::NEG_INFINITY.to_bits(), 1),
        (3, f64::INFINITY.to_bits(), 3, 2.0f64.to_bits(), 1),
        (4, (-0.0f64).to_bits(), 4, f64::NAN.to_bits(), 1),
    ];
    for view in [ViewId(70), ViewId(71)] {
        assert_eq!(values(&mut shared, view), expected);
        assert_eq!(values(&mut private, view), expected);
        let shared_tap = shared.take_changes(view).unwrap();
        let private_tap = private.take_changes(view).unwrap();
        assert_eq!(shared_tap.batch, private_tap.batch);
        assert_eq!(diff_values(&shared_tap), vec![1, 1, 1, 1]);
        assert_eq!(diff_values(&private_tap), vec![1, 1, 1, 1]);
    }
    // A signed retraction must preserve the source payload identity and be observed per tap.
    let retract = float_rows(&[(2, f64::NAN, -1)]);
    shared.push(InputId(0), &retract).unwrap();
    {
        let _force = ForcePrivateSharedArrangements::new();
        private.push(InputId(0), &retract).unwrap();
    }
    for view in [ViewId(70), ViewId(71)] {
        assert_eq!(values(&mut shared, view), values(&mut private, view));
        assert_eq!(diff_values(&shared.take_changes(view).unwrap()), vec![-1]);
        assert_eq!(diff_values(&private.take_changes(view).unwrap()), vec![-1]);
    }
}

#[test]
fn failed_view_creation_does_not_publish_shared_candidate_or_leases() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    let plan = duplicate_root();
    core.build_view(ViewId(72), &plan).unwrap();
    core.build_view(ViewId(73), &plan).unwrap();
    core.push(InputId(0), &super::pairs(&[(7, 10, 1)])).unwrap();
    core.push(InputId(1), &super::pairs(&[(7, 20, 1)])).unwrap();
    let handles = core.views[&ViewId(72)]
        .graph
        .root_shared_handles_for_test()
        .unwrap();
    let leases = core
        .shared_arrangements
        .owner_lease_count(handles.0)
        .unwrap();
    let commits = core
        .shared_arrangements
        .owner_commit_count(handles.0)
        .unwrap();
    assert!(
        core.build_view(ViewId(74), &Plan::Source(InputId(999)))
            .is_err()
    );
    assert_eq!(core.shared_arrangements.owner_count(), 2);
    assert_eq!(
        core.shared_arrangements
            .owner_lease_count(handles.0)
            .unwrap(),
        leases
    );
    assert_eq!(
        core.shared_arrangements
            .owner_commit_count(handles.0)
            .unwrap(),
        commits
    );
    core.push(InputId(0), &super::pairs(&[(7, 11, 1)])).unwrap();
    assert_eq!(core.snapshot(ViewId(72)).unwrap().len(), 2);
    assert_eq!(core.snapshot(ViewId(73)).unwrap().len(), 2);
    assert_eq!(
        core.shared_arrangements
            .owner_commit_count(handles.0)
            .unwrap(),
        commits + 1
    );
}
