//! Matched multiview, Arrow conversion, and quiescent allocator workloads.

use arrow::array::Int64Array;
use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::EngineCore;
use std::collections::BTreeMap;
use std::hint::black_box;

use crate::baseline_workloads::{batch, int_rows, metric, schema};

fn plans() -> [Plan; 2] {
    [
        Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![AggSpec::count()],
        },
        Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![AggSpec::count(), AggSpec::sum(1)],
        },
    ]
}

pub(super) fn shared_core() -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    for (index, plan) in plans().iter().enumerate() {
        core.build_view(ViewId(index as u32), plan).unwrap();
        core.tap_view(ViewId(index as u32)).unwrap();
    }
    core
}

pub(super) fn isolated_cores() -> [EngineCore; 2] {
    plans().map(|plan| {
        let mut core = EngineCore::new();
        core.register_input(InputId(0)).unwrap();
        core.build_view(ViewId(0), &plan).unwrap();
        core.tap_view(ViewId(0)).unwrap();
        core
    })
}

fn assert_outputs(shared: &mut EngineCore, isolated: &mut [EngineCore; 2]) {
    for (index, core) in isolated.iter_mut().enumerate() {
        assert_eq!(
            int_rows(&shared.snapshot(ViewId(index as u32)).unwrap()),
            int_rows(&core.snapshot(ViewId(0)).unwrap()),
            "view {index} output differs from isolated core"
        );
    }
}

pub(super) fn assert_matched_multiview_correctness(n: usize) {
    let input_schema = schema(&["k", "v"]);
    let seed = batch(
        input_schema.clone(),
        vec![
            (0..n).map(|i| (i % 4) as i64).collect(),
            (0..n).map(|i| i as i64).collect(),
        ],
        vec![1; n],
    );
    let retract = batch(input_schema, vec![vec![0], vec![0]], vec![-1]);
    let mut shared = shared_core();
    let mut isolated = isolated_cores();
    shared.push(InputId(0), &seed).unwrap();
    for core in &mut isolated {
        core.push(InputId(0), &seed).unwrap();
    }
    assert_outputs(&mut shared, &mut isolated);
    for (index, core) in isolated.iter_mut().enumerate() {
        assert_eq!(
            int_rows(&shared.take_changes(ViewId(index as u32)).unwrap()),
            int_rows(&core.take_changes(ViewId(0)).unwrap()),
            "view {index} seed delta differs from isolated core"
        );
    }
    shared.push(InputId(0), &retract).unwrap();
    for core in &mut isolated {
        core.push(InputId(0), &retract).unwrap();
    }
    assert_outputs(&mut shared, &mut isolated);
    for (index, core) in isolated.iter_mut().enumerate() {
        assert_eq!(
            int_rows(&shared.take_changes(ViewId(index as u32)).unwrap()),
            int_rows(&core.take_changes(ViewId(0)).unwrap()),
            "view {index} retraction delta differs from isolated core"
        );
    }
}

pub(super) fn materialize_rows(zset: &ZSetBatch) -> BTreeMap<Vec<i64>, i64> {
    let columns: Vec<_> = zset
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let diffs = zset.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..zset.len())
        .map(|index| {
            (
                columns.iter().map(|column| column.value(index)).collect(),
                diffs.value(index),
            )
        })
        .collect()
}

pub(super) fn run(n: usize) {
    assert_matched_multiview_correctness(n);
    matched_multiview_workload(n);
}

fn matched_multiview_workload(n: usize) {
    let input_schema = schema(&["k", "v"]);
    let seed = batch(
        input_schema.clone(),
        vec![
            (0..n).map(|i| (i % 4) as i64).collect(),
            (0..n).map(|i| i as i64).collect(),
        ],
        vec![1; n],
    );
    let retract = batch(input_schema, vec![vec![0], vec![0]], vec![-1]);
    let mut shared = shared_core();
    let mut independent = isolated_cores();
    for (case, input, phase_rows) in [("seed", &seed, n), ("retract", &retract, n + 1)] {
        metric(
            "multiview_shared",
            case,
            phase_rows,
            input.len(),
            0,
            0,
            || {
                shared.push(InputId(0), input).unwrap();
            },
        );
        metric(
            "multiview_independent",
            case,
            phase_rows * 2,
            input.len() * 2,
            0,
            0,
            || {
                for core in &mut independent {
                    core.push(InputId(0), input).unwrap();
                }
            },
        );
        assert_outputs(&mut shared, &mut independent);
        for (index, core) in independent.iter_mut().enumerate() {
            assert_eq!(
                int_rows(&shared.take_changes(ViewId(index as u32)).unwrap()),
                int_rows(&core.take_changes(ViewId(0)).unwrap())
            );
        }
    }
    let shared_checkpoint = shared.checkpoint().unwrap();
    let shared_restored = metric(
        "multiview_shared",
        "checkpoint_encode_restore",
        n,
        0,
        0,
        0,
        || encode_restore(&shared_checkpoint),
    );
    let individual_checkpoints = independent
        .iter()
        .map(|core| core.checkpoint().unwrap())
        .collect::<Vec<_>>();
    let individual_restored = metric(
        "multiview_independent",
        "checkpoint_encode_restore",
        n * 2,
        0,
        0,
        0,
        || {
            individual_checkpoints
                .iter()
                .map(encode_restore)
                .collect::<Vec<_>>()
        },
    );
    assert_outputs_against_restored(
        &mut shared,
        shared_restored,
        &mut independent,
        individual_restored,
    );
}

fn encode_restore(snapshot: &hotlap_core::EngineSnapshot) -> EngineCore {
    let bytes = hotlap_engine::encode_snapshot(snapshot).unwrap();
    let decoded = hotlap_engine::decode_snapshot(&bytes).unwrap();
    let mut core = EngineCore::new();
    core.restore(&decoded).unwrap();
    core
}

fn assert_outputs_against_restored(
    shared: &mut EngineCore,
    mut restored_shared: EngineCore,
    independent: &mut [EngineCore; 2],
    mut restored_individual: Vec<EngineCore>,
) {
    for index in 0..2 {
        let shared_state = int_rows(&shared.snapshot(ViewId(index as u32)).unwrap());
        assert_eq!(
            shared_state,
            int_rows(&restored_shared.snapshot(ViewId(index as u32)).unwrap())
        );
        assert_eq!(
            shared_state,
            int_rows(&independent[index].snapshot(ViewId(0)).unwrap())
        );
        assert_eq!(
            shared_state,
            int_rows(&restored_individual[index].snapshot(ViewId(0)).unwrap())
        );
    }
    let continuation = batch(schema(&["k", "v"]), vec![vec![2], vec![9_999]], vec![1]);
    shared.push(InputId(0), &continuation).unwrap();
    restored_shared.push(InputId(0), &continuation).unwrap();
    for core in independent.iter_mut().chain(restored_individual.iter_mut()) {
        core.push(InputId(0), &continuation).unwrap();
    }
    for (index, core) in independent.iter_mut().enumerate() {
        let expected = int_rows(&shared.take_changes(ViewId(index as u32)).unwrap());
        assert_eq!(
            expected,
            int_rows(&restored_shared.take_changes(ViewId(index as u32)).unwrap())
        );
        assert_eq!(expected, int_rows(&core.take_changes(ViewId(0)).unwrap()));
        assert_eq!(
            expected,
            int_rows(&restored_individual[index].take_changes(ViewId(0)).unwrap())
        );
    }
    assert_outputs(shared, independent);
    for (index, core) in restored_individual.iter_mut().enumerate() {
        let shared_state = int_rows(&shared.snapshot(ViewId(index as u32)).unwrap());
        assert_eq!(
            shared_state,
            int_rows(&restored_shared.snapshot(ViewId(index as u32)).unwrap())
        );
        assert_eq!(shared_state, int_rows(&core.snapshot(ViewId(0)).unwrap()));
    }
    black_box(&mut restored_shared);
    black_box(&mut restored_individual);
}
