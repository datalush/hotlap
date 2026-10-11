//! Matched multiview, Arrow conversion, and quiescent allocator workloads.

use arrow::array::{Array, Int64Array};
use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::{EngineCore, encode_snapshot};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

use crate::baseline_alloc::{self, Stats};
use crate::baseline_workloads::{batch, int_rows, metric, rep, repeats, schema};

pub(super) fn plans() -> [Plan; 2] {
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

fn dense_aggregate(rows: &BTreeMap<(i64, i64), i64>, with_sum: bool) -> BTreeMap<Vec<i64>, i64> {
    let mut groups = BTreeMap::<i64, (i64, i64)>::new();
    for (&(key, value), &diff) in rows {
        let entry = groups.entry(key).or_default();
        entry.0 = entry.0.checked_add(diff).expect("oracle count overflow");
        entry.1 = entry
            .1
            .checked_add(value.checked_mul(diff).expect("oracle product overflow"))
            .expect("oracle sum overflow");
    }
    groups
        .into_iter()
        .filter_map(|(key, (count, sum))| {
            if count == 0 {
                None
            } else if with_sum {
                Some((vec![key, count, sum], 1))
            } else {
                Some((vec![key, count], 1))
            }
        })
        .collect()
}

fn assert_delta(actual: &ZSetBatch, expected: &BTreeMap<Vec<i64>, i64>, label: &str) {
    assert_eq!(
        int_rows(actual),
        *expected,
        "{label} signed changelog differs from dense oracle"
    );
}

pub(super) fn assert_dense_multiview_correctness() {
    let input_schema = schema(&["k", "v"]);
    let steps = [
        (vec![0, 0, 1, 2], vec![10, 10, 20, 30], vec![1, 2, 1, 1]),
        (vec![0, 1, 2], vec![10, 20, 30], vec![-1, 1, -1]),
        (vec![0, 1], vec![10, 20], vec![-2, -1]),
    ];
    let mut oracle = BTreeMap::<(i64, i64), i64>::new();
    let mut shared = shared_core();
    let mut independent = isolated_cores();
    let mut expected_state = [BTreeMap::new(), BTreeMap::new()];
    for (step, (keys, values, diffs)) in steps.into_iter().enumerate() {
        let input = batch(
            input_schema.clone(),
            vec![keys.clone(), values.clone()],
            diffs.clone(),
        );
        let before = [
            dense_aggregate(&oracle, false),
            dense_aggregate(&oracle, true),
        ];
        for ((key, value), diff) in keys.into_iter().zip(values).zip(diffs) {
            let entry = oracle.entry((key, value)).or_default();
            *entry = entry.checked_add(diff).expect("oracle input diff overflow");
            if *entry == 0 {
                oracle.remove(&(key, value));
            }
        }
        let after = [
            dense_aggregate(&oracle, false),
            dense_aggregate(&oracle, true),
        ];
        for index in 0..2 {
            let mut delta = after[index].clone();
            for (row, weight) in &before[index] {
                let value = delta.entry(row.clone()).or_default();
                *value = value.checked_sub(*weight).expect("oracle delta overflow");
                if *value == 0 {
                    delta.remove(row);
                }
            }
            expected_state[index] = after[index].clone();
        }
        shared.push(InputId(0), &input).unwrap();
        for core in &mut independent {
            core.push(InputId(0), &input).unwrap();
        }
        for index in 0..2 {
            let mut expected_delta = after[index].clone();
            for (row, weight) in &before[index] {
                let value = expected_delta.entry(row.clone()).or_default();
                *value = value.checked_sub(*weight).expect("oracle delta overflow");
                if *value == 0 {
                    expected_delta.remove(row);
                }
            }
            let shared_delta = shared.take_changes(ViewId(index as u32)).unwrap();
            assert_delta(
                &shared_delta,
                &expected_delta,
                &format!("shared view={index} step={step}"),
            );
            assert_eq!(
                int_rows(&independent[index].take_changes(ViewId(0)).unwrap()),
                expected_delta
            );
            assert_eq!(
                int_rows(&shared.snapshot(ViewId(index as u32)).unwrap()),
                expected_state[index]
            );
            assert_eq!(
                int_rows(&independent[index].snapshot(ViewId(0)).unwrap()),
                expected_state[index]
            );
        }
    }
    let checkpoint = shared.checkpoint().unwrap();
    let mut restored = encode_restore(&checkpoint);
    let continuation = batch(input_schema, vec![vec![1], vec![20]], vec![1]);
    let before = [
        dense_aggregate(&oracle, false),
        dense_aggregate(&oracle, true),
    ];
    *oracle.entry((1, 20)).or_default() += 1;
    let after = [
        dense_aggregate(&oracle, false),
        dense_aggregate(&oracle, true),
    ];
    shared.push(InputId(0), &continuation).unwrap();
    restored.push(InputId(0), &continuation).unwrap();
    for (index, old) in before.iter().enumerate() {
        let mut expected_delta = after[index].clone();
        for (row, weight) in old {
            let value = expected_delta.entry(row.clone()).or_default();
            *value -= *weight;
            if *value == 0 {
                expected_delta.remove(row);
            }
        }
        assert_delta(
            &shared.take_changes(ViewId(index as u32)).unwrap(),
            &expected_delta,
            "shared post-restore continuation",
        );
        assert_eq!(
            int_rows(&restored.take_changes(ViewId(index as u32)).unwrap()),
            expected_delta
        );
        assert_eq!(
            int_rows(&restored.snapshot(ViewId(index as u32)).unwrap()),
            after[index]
        );
    }
}

pub(super) fn serialized_snapshot_size(n: usize) -> usize {
    let setup_start = Instant::now();
    let input_schema = schema(&["k", "v"]);
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &plans()[0]).unwrap();
    let setup_ns = setup_start.elapsed().as_nanos();
    let fixture_start = Instant::now();
    let input = batch(
        input_schema,
        vec![
            (0..n).map(|i| (i % 4) as i64).collect(),
            (0..n).map(|i| i as i64).collect(),
        ],
        vec![1; n],
    );
    let fixture_setup_ns = fixture_start.elapsed().as_nanos();
    core.push(InputId(0), &input).unwrap();
    let snapshot = core.checkpoint().unwrap();
    let (head, allocated_live_before_bytes) = baseline_alloc::capture_baseline(|| {
        std::env::var("HOTLAP_BASELINE_HEAD").unwrap_or_else(|_| "unknown".into())
    });
    let started = Instant::now();
    let bytes = encode_snapshot(&snapshot).unwrap();
    let elapsed_ns = started.elapsed().as_nanos();
    let stats = Stats::read();
    println!(
        "{{\"case\":\"single_view_checkpoint\",\"phase\":\"single_view_one_core\",\"seed\":7,\"rep\":{},\"repeats\":{},\"head\":\"{head}\",\"profile\":\"{}\",\"arch\":\"{}\",\"os\":\"{}\",\"input_rows\":{n},\"setup_ns\":{setup_ns},\"fixture_setup_ns\":{fixture_setup_ns},\"elapsed_ns\":{elapsed_ns},\"setup_timing_semantics\":\"setup_and_fixture_creation_measured_once_outside_encoding_interval\",\"elapsed_semantics\":\"wall_clock_not_cpu\",\"allocated_live_before_bytes\":{allocated_live_before_bytes},\"peak_allocated_live_bytes\":{},\"allocated_live_after_bytes\":{},\"serialized_snapshot_bytes\":{},\"serialized_bytes_semantics\":\"encoded_checkpoint_payload_only_not_retained_heap\"}}",
        rep(),
        repeats(),
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        std::env::consts::ARCH,
        std::env::consts::OS,
        stats.peak,
        stats.live,
        bytes.len()
    );
    bytes.len()
}

pub(super) fn materialize_rows(zset: &ZSetBatch) -> BTreeMap<Vec<i64>, i64> {
    let columns: Vec<_> = zset
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let diffs = zset.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    let mut rows = BTreeMap::new();
    for index in 0..zset.len() {
        let row = columns
            .iter()
            .map(|column| column.value(index))
            .collect::<Vec<_>>();
        let weight = rows.entry(row.clone()).or_insert(0i64);
        *weight = weight
            .checked_add(diffs.value(index))
            .expect("materialized diff overflow");
        if *weight == 0 {
            rows.remove(&row);
        }
    }
    rows
}

pub(super) fn run(n: usize) {
    serialized_snapshot_size(n);
    assert_matched_multiview_correctness(n);
    assert_dense_multiview_correctness();
    matched_multiview_workload(n);
}

fn matched_multiview_workload(n: usize) {
    let input_schema = schema(&["k", "v"]);
    let fixture_started = Instant::now();
    let seed = batch(
        input_schema.clone(),
        vec![
            (0..n).map(|i| (i % 4) as i64).collect(),
            (0..n).map(|i| i as i64).collect(),
        ],
        vec![1; n],
    );
    let retract = batch(input_schema, vec![vec![0], vec![0]], vec![-1]);
    let fixture_setup_ns = fixture_started.elapsed().as_nanos();
    let setup_started = Instant::now();
    let mut shared = shared_core();
    let mut independent = isolated_cores();
    let setup_ns = setup_started.elapsed().as_nanos();
    for (case, input, phase_rows) in [("seed", &seed, n), ("retract", &retract, n + 1)] {
        metric(
            "one_core_two_views",
            case,
            phase_rows,
            input.len(),
            setup_ns,
            fixture_setup_ns,
            || {
                shared.push(InputId(0), input).unwrap();
            },
        );
        metric(
            "two_cores_one_view_each",
            case,
            phase_rows * 2,
            input.len() * 2,
            setup_ns,
            fixture_setup_ns,
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
        "one_core_two_views",
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
        "two_cores_one_view_each",
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
    let single_checkpoint = independent[0].checkpoint().unwrap();
    let _single_view_bytes = metric(
        "single_view_checkpoint",
        "single_view_one_core",
        n,
        0,
        0,
        0,
        || encode_restore(&single_checkpoint),
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
