//! Ignored, paired end-to-end benchmark for the direct grouped join cut.

#[path = "paired_bench/avg_bench.rs"]
mod avg_bench;
#[path = "paired_bench/shared_arena_bench.rs"]
mod shared_arena_bench;
#[path = "paired_bench/typed_key_cost.rs"]
mod typed_key_cost;

use std::collections::HashMap;
use std::time::Instant;
use std::{cell::Cell, fs, sync::Arc};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde::Serialize;
#[cfg(feature = "bench-allocation-observer")]
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
#[cfg(feature = "bench-allocation-observer")]
use std::alloc::System;

use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, ZSetBatch};

use crate::core::EngineCore;

#[cfg(feature = "bench-allocation-observer")]
#[global_allocator]
static TEST_ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn resident_pages() -> usize {
    fs::read_to_string("/proc/self/statm")
        .expect("Linux procfs statm is available for this benchmark")
        .split_whitespace()
        .nth(1)
        .expect("statm contains resident pages")
        .parse()
        .expect("resident page count is numeric")
}

#[derive(Clone, Copy)]
struct Fixture {
    name: &'static str,
    keys: usize,
    fanout: usize,
    duplicate_payloads: bool,
    skewed: bool,
    mixed_flat_view: bool,
    unsafe_avg_history: bool,
    subset_group: bool,
    update: Update,
}

#[derive(Clone, Copy, Debug)]
enum Update {
    ReplaceLeft,
    ReplaceRight,
    AppendLeft,
    DeleteLeft,
    AppendRight,
    DeleteRight,
    NoMatchLeft,
}

impl Fixture {
    fn fanout_for(self, key: usize) -> usize {
        if self.skewed && key != 0 {
            1
        } else {
            self.fanout
        }
    }

    fn payload(self, key: usize, row: usize) -> i64 {
        if self.unsafe_avg_history {
            (1_i64 << 53) + 1
        } else if self.duplicate_payloads {
            (row / 2) as i64
        } else {
            (key * self.fanout + row) as i64
        }
    }

    fn source_value(self, right: bool, key: usize, row: usize) -> Option<i64> {
        if right && row.is_multiple_of(7) && !self.unsafe_avg_history {
            None
        } else if right && row == 1 && key < 2 {
            Some(0)
        } else {
            Some(self.payload(key, row))
        }
    }

    fn initial_left(self) -> ZSetBatch {
        self.rows(false)
    }

    fn initial_right(self) -> ZSetBatch {
        self.rows(true)
    }

    fn rows(self, right: bool) -> ZSetBatch {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut diffs = Vec::new();
        for key in 0..self.keys {
            for row in 0..self.fanout_for(key) {
                keys.push(key as i64);
                values.push(self.source_value(right, key, row));
                diffs.push(1_i64);
                if self.duplicate_payloads && row == 0 {
                    keys.push(key as i64);
                    values.push(self.source_value(right, key, row));
                    diffs.push(1_i64);
                }
                if self.duplicate_payloads && row % 2 == 1 {
                    // A repeated identical source row is a real signed input;
                    // arrangement identity consolidates it to multiplicity 2.
                    keys.push(key as i64);
                    values.push(self.source_value(right, key, row));
                    diffs.push(1_i64);
                }
            }
        }
        zset(keys, values, diffs)
    }

    fn update(self) -> (InputId, ZSetBatch) {
        self.update_with(self.update)
    }

    fn update_with(self, update: Update) -> (InputId, ZSetBatch) {
        let (input, key, old_row, row, diffs) = match update {
            Update::ReplaceLeft => (
                InputId(0),
                0,
                0,
                self.payload(0, 0).wrapping_add(1_000_003),
                vec![-1, 1],
            ),
            Update::ReplaceRight => (
                InputId(1),
                0,
                1,
                self.payload(0, 1).wrapping_add(2_000_003),
                vec![-1, 1],
            ),
            Update::AppendLeft => (InputId(0), 0, 0, 1_000_019, vec![1]),
            Update::AppendRight => (InputId(1), 0, 0, 1_000_033, vec![1]),
            Update::DeleteLeft => (InputId(0), 2, 2, self.payload(2, 2), vec![-1]),
            Update::DeleteRight => (InputId(1), 2, 2, self.payload(2, 2), vec![-1]),
            Update::NoMatchLeft => (InputId(0), self.keys as i64 + 9, 0, 1_000_037, vec![1]),
        };
        let is_right = input == InputId(1);
        let (keys, values) = match update {
            Update::ReplaceLeft | Update::ReplaceRight => (
                vec![key, key],
                vec![
                    self.source_value(is_right, key as usize, old_row),
                    Some(row),
                ],
            ),
            Update::DeleteLeft | Update::DeleteRight => (
                vec![key],
                vec![self.source_value(is_right, key as usize, old_row)],
            ),
            Update::NoMatchLeft => (vec![key], vec![Some(row)]),
            _ => (vec![key], vec![Some(row)]),
        };
        (input, zset(keys, values, diffs))
    }

    fn plan(self) -> Plan {
        let joined = Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0, 1],
            right_key: vec![0, 1],
        };
        let (input, group_key, count_input, left_sum, right_sum, avg_input) = if self.subset_group {
            (
                Plan::Project {
                    input: Box::new(joined),
                    cols: vec![3, 0, 5, 2],
                },
                vec![1, 0, 1],
                2,
                3,
                2,
                2,
            )
        } else {
            (joined, vec![0], 5, 2, 5, 5)
        };
        let aggregates = if self.unsafe_avg_history {
            vec![
                AggSpec::count(),
                AggSpec::sum(left_sum),
                AggSpec::avg(avg_input),
            ]
        } else {
            vec![
                AggSpec::count(),
                AggSpec {
                    func: hotlap_core::AggFunc::Count,
                    input: Some(count_input),
                },
                AggSpec::sum(left_sum),
                AggSpec::sum(right_sum),
            ]
        };
        Plan::GroupAggregate {
            input: Box::new(input),
            key: group_key,
            aggs: aggregates,
        }
    }
}

fn zset(keys: Vec<i64>, values: Vec<Option<i64>>, diffs: Vec<i64>) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("join_key_hi", DataType::Int64, false),
        Field::new("join_key_lo", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let key_hi = keys.iter().map(|key| key.div_euclid(2)).collect::<Vec<_>>();
    let key_lo = keys.iter().map(|key| key.rem_euclid(2)).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(key_hi)) as ArrayRef,
            Arc::new(Int64Array::from(key_lo)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn build_seeded(fixture: Fixture, force_dense: bool) -> EngineCore {
    let mut core = build_empty(fixture);
    let _mode = ModeGuard::new(force_dense);
    core.push(InputId(0), &fixture.initial_left()).unwrap();
    core.push(InputId(1), &fixture.initial_right()).unwrap();
    let _ = core.take_changes(ViewId(0)).unwrap();
    if fixture.mixed_flat_view {
        let _ = core.take_changes(ViewId(1)).unwrap();
    }
    core
}

#[test]
fn paired_fixtures_use_composite_keys_real_subset_groups_and_nullable_right_values() {
    let direct = Fixture {
        name: "contract-direct",
        keys: 4,
        fanout: 2,
        duplicate_payloads: false,
        skewed: false,
        mixed_flat_view: false,
        unsafe_avg_history: false,
        subset_group: false,
        update: Update::ReplaceLeft,
    };
    let subset = Fixture {
        subset_group: true,
        ..direct
    };

    for (fixture, expected_group, expected_inputs, expected_project) in [
        (direct, vec![0], vec![None, Some(5), Some(2), Some(5)], None),
        (
            subset,
            vec![0, 0, 0],
            vec![None, Some(5), Some(2), Some(5)],
            Some(vec![3, 0, 5, 2]),
        ),
    ] {
        let left = fixture.initial_left();
        let right = fixture.initial_right();
        assert_eq!(left.schema().fields().len(), 3);
        assert_eq!(right.schema().fields().len(), 3);
        assert!(left.schema().field(2).is_nullable());
        assert!(right.schema().field(2).is_nullable());
        assert_eq!(
            right.batch.column(2).null_count(),
            fixture.keys * fixture.fanout.div_ceil(7)
        );
        if fixture.fanout > 1 {
            let right_values = right
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(right_values.value(fixture.fanout + 1), 0);
        }
        assert_eq!(
            left.batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );

        let first_hi = left
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let first_lo = left
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let second_key_row = fixture.fanout;
        assert_eq!(first_hi.value(0), first_hi.value(second_key_row));
        assert_ne!(first_lo.value(0), first_lo.value(second_key_row));

        let schemas = [(InputId(0), left.schema()), (InputId(1), right.schema())]
            .into_iter()
            .collect::<HashMap<_, _>>();
        let hotlap_core::plan_normalize::Recognition::Eligible(mapping) =
            hotlap_core::plan_normalize::recognize(&fixture.plan(), &schemas)
        else {
            panic!("paired fixture plan must normalize to the maintained join cut");
        };
        assert_eq!(mapping.left_join_key, vec![0, 1]);
        assert_eq!(mapping.right_join_key, vec![0, 1]);
        assert_eq!(mapping.group_key, expected_group);
        assert_eq!(mapping.aggregate_inputs, expected_inputs);

        let Plan::GroupAggregate { input, key, aggs } = fixture.plan() else {
            unreachable!();
        };
        assert_eq!(
            key,
            if fixture.subset_group {
                vec![1, 0, 1]
            } else {
                vec![0]
            }
        );
        assert_eq!(
            aggs[1].input,
            Some(if fixture.subset_group { 2 } else { 5 })
        );
        if let Some(expected_cols) = expected_project {
            let Plan::Project { input, cols } = *input else {
                panic!("subset fixture must project the actual joined schema");
            };
            assert_eq!(cols, expected_cols);
            let Plan::Join {
                left_key,
                right_key,
                ..
            } = *input
            else {
                panic!("subset projection must wrap the composite Join");
            };
            assert_eq!(left_key, vec![0, 1]);
            assert_eq!(right_key, vec![0, 1]);
        } else {
            let Plan::Join {
                left_key,
                right_key,
                ..
            } = *input
            else {
                panic!("direct fixture must group the Join without projection");
            };
            assert_eq!(left_key, vec![0, 1]);
            assert_eq!(right_key, vec![0, 1]);
        }
    }

    let all_null_right = Fixture {
        fanout: 1,
        ..direct
    };
    let mut null_core = build_seeded(all_null_right, false);
    let null_snapshot = null_core.snapshot(ViewId(0)).unwrap();
    let group_keys = null_snapshot
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let null_group = (0..null_snapshot.len())
        .find(|&row| group_keys.value(row) == 0)
        .unwrap();
    let counts = null_snapshot
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(null_group), 0);
    assert!(null_snapshot.batch.column(4).is_null(null_group));

    let zero_right = Fixture { ..direct };
    let mut zero_core = build_seeded(zero_right, false);
    let zero_snapshot = zero_core.snapshot(ViewId(0)).unwrap();
    let zero_group = (0..zero_snapshot.len())
        .find(|&row| {
            zero_snapshot
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
                == 0
        })
        .unwrap();
    let zero_counts = zero_snapshot
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let zero_sums = zero_snapshot
        .batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(zero_counts.value(zero_group) > 0);
    assert_eq!(zero_sums.value(zero_group), 0);
}

#[test]
fn paired_updates_replace_append_and_delete_nonnull_sum_inputs_on_both_sides() {
    let fixture = Fixture {
        name: "update-contract",
        keys: 8,
        fanout: 4,
        duplicate_payloads: false,
        skewed: false,
        mixed_flat_view: false,
        unsafe_avg_history: false,
        subset_group: true,
        update: Update::ReplaceLeft,
    };
    for update in [
        Update::ReplaceLeft,
        Update::ReplaceRight,
        Update::AppendLeft,
        Update::AppendRight,
        Update::DeleteLeft,
        Update::DeleteRight,
        Update::NoMatchLeft,
    ] {
        let (input, batch) = fixture.update_with(update);
        let value = batch
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let diffs = batch.diff().as_any().downcast_ref::<Int64Array>().unwrap();
        match update {
            Update::ReplaceLeft | Update::ReplaceRight => {
                assert_eq!(
                    input,
                    if matches!(update, Update::ReplaceLeft) {
                        InputId(0)
                    } else {
                        InputId(1)
                    }
                );
                assert_eq!(batch.len(), 2);
                assert_eq!(diffs.value(0), -1);
                assert_eq!(diffs.value(1), 1);
                assert!(value.value(1) > 0);
                assert!(!value.is_null(1));
            }
            Update::AppendLeft | Update::AppendRight | Update::NoMatchLeft => {
                assert_eq!(batch.len(), 1);
                assert_eq!(diffs.value(0), 1);
                assert!(value.value(0) > 0);
                assert!(!value.is_null(0));
            }
            Update::DeleteLeft | Update::DeleteRight => {
                assert_eq!(batch.len(), 1);
                assert_eq!(diffs.value(0), -1);
                assert!(value.value(0) > 0);
                assert!(!value.is_null(0));
            }
        }
    }
}

fn build_empty(fixture: Fixture) -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(ViewId(0), &fixture.plan()).unwrap();
    if fixture.mixed_flat_view {
        core.build_view(
            ViewId(1),
            &Plan::Join {
                left: Box::new(Plan::Source(InputId(0))),
                right: Box::new(Plan::Source(InputId(1))),
                left_key: vec![0, 1],
                right_key: vec![0, 1],
            },
        )
        .unwrap();
    }
    core.tap_view(ViewId(0)).unwrap();
    if fixture.mixed_flat_view {
        core.tap_view(ViewId(1)).unwrap();
    }
    core
}

struct ModeGuard(bool);

impl ModeGuard {
    fn new(force_dense: bool) -> Self {
        let previous =
            crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.replace(force_dense));
        Self(previous)
    }
}

impl Drop for ModeGuard {
    fn drop(&mut self) {
        crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|flag| flag.set(self.0));
    }
}

#[derive(Debug, Serialize)]
struct Sample {
    mode: &'static str,
    setup_ns: u128,
    elapsed_ns: u128,
    factorized_commits: u64,
    flat_rows_cuts: u64,
    dense_join_calls: u64,
    dense_join_rows: u64,
    dense_join_array_bytes: u64,
    process_rss_pages_after_seed: usize,
    process_rss_pages_before: usize,
    process_rss_pages_after: usize,
    process_rss_pages_after_drain: usize,
    allocator_observer_enabled: bool,
    allocator_allocations: Option<usize>,
    allocator_deallocations: Option<usize>,
    allocator_reallocations: Option<usize>,
    allocator_bytes_allocated: Option<usize>,
    allocator_bytes_deallocated: Option<usize>,
    allocator_bytes_reallocated: Option<isize>,
    allocator_signed_alloc_free_delta_bytes: Option<i128>,
    allocator_observation_scope: &'static str,
    engine_known_capacity_before_bytes: usize,
    engine_known_capacity_after_bytes: usize,
    engine_memory_report_before: crate::core::memory::MemoryReport,
    engine_memory_report_after_push: crate::core::memory::MemoryReport,
    engine_memory_report_after_drain: crate::core::memory::MemoryReport,
    group_output_rows: usize,
    group_output_array_bytes: usize,
    flat_output_rows: usize,
    flat_output_array_bytes: usize,
    old_history_rows_scanned: usize,
    old_history_summarize_ns: u128,
    old_history_summarize_calls: usize,
    prepare_stage_ns: [u128; 7],
    old_bucket_decodes: usize,
}

fn reset_path_counters() {
    crate::core::graph::FACTORIZED_CUTS.with(|value| value.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|value| value.set(0));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|value| value.set(0));
    crate::core::graph::DENSE_JOIN_ROWS.with(|value| value.set(0));
    crate::core::graph::DENSE_JOIN_BYTES.with(|value| value.set(0));
    crate::ops::join::factorized::OLD_HISTORY_ROWS_SCANNED.with(|value| value.set(0));
    crate::ops::join::factorized::OLD_HISTORY_SUMMARIZE_NS.with(|value| value.set(0));
    crate::ops::join::factorized::OLD_HISTORY_SUMMARIZE_CALLS.with(|value| value.set(0));
    crate::ops::join::factorized::PREPARE_STAGE_NS.with(|value| value.set([0; 7]));
    crate::ops::join::factorized::OLD_BUCKET_DECODES.with(|value| value.set(0));
}

fn array_bytes(batch: &ZSetBatch) -> usize {
    batch
        .batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size())
        .chain(std::iter::once(batch.diff().get_array_memory_size()))
        .fold(0usize, usize::saturating_add)
}

fn sample(fixture: Fixture, force_dense: bool) -> (Sample, Vec<ZSetBatch>, Vec<ZSetBatch>) {
    let setup_started = Instant::now();
    let mut core = build_seeded(fixture, force_dense);
    let setup_ns = setup_started.elapsed().as_nanos();
    let rss_after_seed = resident_pages();
    let (input, update) = fixture.update();
    let before_report = core.memory_report().unwrap();
    reset_path_counters();
    crate::ops::join::test_support::reset_counts();
    let rss_before = resident_pages();
    #[cfg(feature = "bench-allocation-observer")]
    let allocation_region = Region::new(TEST_ALLOCATOR);
    let mode = ModeGuard::new(force_dense);
    let started = Instant::now();
    core.push(input, &update).unwrap();
    let elapsed_ns = started.elapsed().as_nanos();
    drop(mode);
    #[cfg(feature = "bench-allocation-observer")]
    let allocator_stats = Some(allocation_region.change());
    #[cfg(not(feature = "bench-allocation-observer"))]
    let allocator_stats: Option<stats_alloc::Stats> = None;
    let rss_after = resident_pages();
    let factorized_commits = crate::core::graph::FACTORIZED_CUTS.with(Cell::get);
    let flat_rows_cuts = crate::core::graph::FLAT_ROWS_CUTS.with(Cell::get);
    let dense_join_calls = crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(Cell::get);
    let dense_join_rows = crate::core::graph::DENSE_JOIN_ROWS.with(Cell::get);
    let dense_join_array_bytes = crate::core::graph::DENSE_JOIN_BYTES.with(Cell::get);
    let after_report = core.memory_report().unwrap();

    let mut changes = vec![core.take_changes(ViewId(0)).unwrap()];
    let mut snapshots = vec![core.snapshot(ViewId(0)).unwrap()];
    if fixture.mixed_flat_view {
        changes.push(core.take_changes(ViewId(1)).unwrap());
        snapshots.push(core.snapshot(ViewId(1)).unwrap());
    }
    let rss_after_drain = resident_pages();
    let after_drain_report = core.memory_report().unwrap();
    let result = Sample {
        mode: if force_dense { "forced_dense" } else { "auto" },
        setup_ns,
        elapsed_ns,
        factorized_commits,
        flat_rows_cuts,
        dense_join_calls,
        dense_join_rows,
        dense_join_array_bytes,
        process_rss_pages_before: rss_before,
        process_rss_pages_after: rss_after,
        process_rss_pages_after_seed: rss_after_seed,
        process_rss_pages_after_drain: rss_after_drain,
        allocator_observer_enabled: cfg!(feature = "bench-allocation-observer"),
        allocator_allocations: allocator_stats.map(|stats| stats.allocations),
        allocator_deallocations: allocator_stats.map(|stats| stats.deallocations),
        allocator_reallocations: allocator_stats.map(|stats| stats.reallocations),
        allocator_bytes_allocated: allocator_stats.map(|stats| stats.bytes_allocated),
        allocator_bytes_deallocated: allocator_stats.map(|stats| stats.bytes_deallocated),
        allocator_bytes_reallocated: allocator_stats.map(|stats| stats.bytes_reallocated),
        allocator_signed_alloc_free_delta_bytes: allocator_stats.map(signed_alloc_free_delta),
        allocator_observation_scope: allocator_observation_scope(),
        engine_known_capacity_before_bytes: before_report.known_backing_capacity_bytes,
        engine_known_capacity_after_bytes: after_report.known_backing_capacity_bytes,
        engine_memory_report_before: before_report,
        engine_memory_report_after_push: after_report,
        engine_memory_report_after_drain: after_drain_report,
        group_output_rows: snapshots[0].len(),
        group_output_array_bytes: array_bytes(&snapshots[0]),
        flat_output_rows: snapshots.get(1).map_or(0, ZSetBatch::len),
        flat_output_array_bytes: snapshots.get(1).map_or(0, array_bytes),
        old_history_rows_scanned: crate::ops::join::factorized::OLD_HISTORY_ROWS_SCANNED
            .with(Cell::get),
        old_history_summarize_ns: crate::ops::join::factorized::OLD_HISTORY_SUMMARIZE_NS
            .with(Cell::get),
        old_history_summarize_calls: crate::ops::join::factorized::OLD_HISTORY_SUMMARIZE_CALLS
            .with(Cell::get),
        prepare_stage_ns: crate::ops::join::factorized::PREPARE_STAGE_NS.with(Cell::get),
        old_bucket_decodes: crate::ops::join::factorized::OLD_BUCKET_DECODES.with(Cell::get),
    };
    (result, changes, snapshots)
}

fn recovery_sample(fixture: Fixture, force_dense: bool) -> serde_json::Value {
    let setup_started = Instant::now();
    let mut original = build_seeded(fixture, force_dense);
    let setup_ns = setup_started.elapsed().as_nanos();
    let checkpoint_started = Instant::now();
    let checkpoint = original.checkpoint().unwrap();
    let checkpoint_ns = checkpoint_started.elapsed().as_nanos();
    let expected_before = original.snapshot(ViewId(0)).unwrap();
    let (input, update) = fixture.update();
    let _mode = ModeGuard::new(force_dense);
    original.push(input, &update).unwrap();
    let expected_change = original.take_changes(ViewId(0)).unwrap();
    let expected_after = original.snapshot(ViewId(0)).unwrap();
    drop(original);

    let rss_before_restore = resident_pages();
    let mut restored = build_empty(fixture);
    let restore_started = Instant::now();
    restored.restore(&checkpoint).unwrap();
    let restore_ns = restore_started.elapsed().as_nanos();
    let rss_after_restore = resident_pages();
    let restored_snapshot = restored.snapshot(ViewId(0)).unwrap();
    assert_same_batch(&expected_before, &restored_snapshot);

    reset_path_counters();
    let followup_started = Instant::now();
    restored.push(input, &update).unwrap();
    let followup_ns = followup_started.elapsed().as_nanos();
    let restored_change = restored.take_changes(ViewId(0)).unwrap();
    assert_same_batch(&expected_change, &restored_change);
    assert_same_batch(&expected_after, &restored.snapshot(ViewId(0)).unwrap());
    serde_json::json!({
        "record":"recovery_sample",
        "run_id":run_id(),
        "fixture":fixture.name,
        "mode":if force_dense { "forced_dense" } else { "auto" },
        "setup_ns":setup_ns,
        "checkpoint_ns":checkpoint_ns,
        "restore_ns":restore_ns,
        "continuation_push_ns":followup_ns,
        "factorized_commits":crate::core::graph::FACTORIZED_CUTS.with(Cell::get),
        "dense_join_rows":crate::core::graph::DENSE_JOIN_ROWS.with(Cell::get),
        "rss_before_restore_pages":rss_before_restore,
        "rss_after_restore_pages":rss_after_restore,
        "rss_after_continuation_pages":resident_pages(),
        "known_capacity_bytes":restored.memory_report().unwrap().known_backing_capacity_bytes,
        "engine_memory_report":restored.memory_report().unwrap(),
        "parity":"full typed checkpoint snapshot, continuation changelog and post-update snapshot matched",
    })
}

fn recovery_paired_experiment(fixture: Fixture) {
    let _mode = ModeGuard::new(false);
    let mut original = build_seeded(fixture, false);
    let checkpoint = original.checkpoint().unwrap();
    let reference_before = original.snapshot(ViewId(0)).unwrap();
    let (input, update) = fixture.update();
    original.push(input, &update).unwrap();
    let reference_change = original.take_changes(ViewId(0)).unwrap();
    let reference_after = original.snapshot(ViewId(0)).unwrap();
    let checkpoint_identity = format!("v1-{:016x}", checkpoint_hash(&checkpoint));
    drop(original);

    let run_id = run_id();
    let mut samples: HashMap<&str, Vec<u128>> = HashMap::new();
    for trial in 0..6 {
        for mode in if trial % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let mut restored = build_empty(fixture);
            let _mode = ModeGuard::new(mode);
            let start = Instant::now();
            // Actual timed API: public EngineCore::restore only.
            restored.restore(&checkpoint).unwrap();
            let elapsed_ns = start.elapsed().as_nanos();
            assert_same_batch(&reference_before, &restored.snapshot(ViewId(0)).unwrap());
            restored.push(input, &update).unwrap();
            assert_same_batch(
                &reference_change,
                &restored.take_changes(ViewId(0)).unwrap(),
            );
            assert_same_batch(&reference_after, &restored.snapshot(ViewId(0)).unwrap());
            let name = if mode { "forced_dense" } else { "auto" };
            if trial > 0 {
                samples.entry(name).or_default().push(elapsed_ns);
            }
            println!(
                "{{\"record\":\"recovery_pair\",\"run_id\":\"{run_id}\",\"fixture\":\"{}\",\"mode\":\"{name}\",\"trial\":{trial},\"restore_ns\":{elapsed_ns},\"checkpoint_identity\":\"{checkpoint_identity}\"}}",
                fixture.name
            );
            drop(_mode);
            drop(restored);
        }
    }
    for (mode, mut values) in samples {
        values.sort_unstable();
        let median = median(values.iter().copied());
        println!(
            "{{\"record\":\"recovery_summary\",\"run_id\":\"{run_id}\",\"fixture\":\"{}\",\"mode\":\"{mode}\",\"measured_pairs\":5,\"median_restore_ns\":{median},\"min_ns\":{},\"max_ns\":{}}}",
            fixture.name, values[0], values[4]
        );
    }
}

#[test]
fn recovery_paired_median_uses_middle_sample_for_odd_count() {
    assert_eq!(median([1_u128, 2, 3, 100, 200].into_iter()), 3);
    assert_eq!(median([6_564_565_u128, 6_597_520].into_iter()), 6_597_520);
    assert_eq!(median([1_u128, 2, 3, 4].into_iter()), 3);
}

fn checkpoint_hash(checkpoint: &hotlap_core::snapshot::EngineSnapshot) -> u64 {
    let bytes = bincode::serialize(checkpoint).unwrap();
    bytes.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn assert_same_batch(left: &ZSetBatch, right: &ZSetBatch) {
    assert_eq!(left.len(), right.len());
    assert_eq!(left.schema(), right.schema());
    assert_eq!(left.diff().to_data(), right.diff().to_data());
    if left.is_empty() {
        return;
    }
    for (left, right) in left.batch.columns().iter().zip(right.batch.columns()) {
        assert_eq!(left.to_data(), right.to_data());
    }
}

fn assert_same_views(left: &[ZSetBatch], right: &[ZSetBatch]) {
    assert_eq!(left.len(), right.len());
    for (left, right) in left.iter().zip(right) {
        assert_same_batch(left, right);
    }
}

fn median<T: Ord + Copy>(values: impl Iterator<Item = T>) -> T {
    let mut values = values.collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

fn signed_alloc_free_delta(stats: stats_alloc::Stats) -> i128 {
    stats.bytes_allocated as i128 - stats.bytes_deallocated as i128
}

fn active_features() -> &'static [&'static str] {
    if cfg!(feature = "bench-allocation-observer") {
        &["bench-allocation-observer"]
    } else {
        &[]
    }
}

fn allocator_observation_scope() -> &'static str {
    if cfg!(feature = "bench-allocation-observer") {
        "process-wide allocator aggregate during region; may include unrelated allocations/threads; not EngineCore-only or retained bytes"
    } else {
        "disabled"
    }
}

#[cfg(test)]
mod observer_contract_tests {
    use super::*;

    #[cfg(feature = "bench-allocation-observer")]
    #[test]
    #[ignore = "isolated full EngineCore Dense join allocation comparison; run with one test thread"]
    fn dense_join_direct_index_allocation_regression() {
        let fixture = Fixture {
            name: "direct-index-allocation-1x256",
            keys: 1,
            fanout: 256,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::AppendLeft,
        };
        let mut direct = build_seeded(fixture, true);
        let (input, update) = fixture.update();
        let _mode = ModeGuard::new(true);
        reset_path_counters();
        crate::ops::join::test_support::reset_counts();
        let direct_region = Region::new(TEST_ALLOCATOR);
        direct.push(input, &update).unwrap();
        let direct_stats = direct_region.change();
        let direct_routes = crate::ops::join::test_support::counts();
        let direct_dense_calls = crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(Cell::get);
        let direct_dense_rows = crate::core::graph::DENSE_JOIN_ROWS.with(Cell::get);
        drop(_mode);
        let direct_changes = vec![direct.take_changes(ViewId(0)).unwrap()];
        let direct_snapshots = vec![direct.snapshot(ViewId(0)).unwrap()];
        let (mut legacy, legacy_stats, legacy_routes, legacy_dense_calls, legacy_dense_rows) = {
            let _mode = ModeGuard::new(true);
            let _legacy = crate::ops::join::test_support::LegacyDenseJoinGuard::force();
            let mut legacy = build_seeded(fixture, true);
            reset_path_counters();
            crate::ops::join::test_support::reset_counts();
            let legacy_region = Region::new(TEST_ALLOCATOR);
            legacy.push(input, &update).unwrap();
            let stats = legacy_region.change();
            let routes = crate::ops::join::test_support::counts();
            let calls = crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(Cell::get);
            let rows = crate::core::graph::DENSE_JOIN_ROWS.with(Cell::get);
            (legacy, stats, routes, calls, rows)
        };
        let legacy_changes = vec![legacy.take_changes(ViewId(0)).unwrap()];
        let legacy_snapshots = vec![legacy.snapshot(ViewId(0)).unwrap()];

        assert!(direct_dense_calls > 0 && direct_dense_rows > 0);
        assert!(direct_routes.direct > 0);
        assert_eq!(direct_routes.forced_legacy, 0);
        assert!(legacy_routes.forced_legacy > 0);
        assert!(legacy_dense_calls > 0 && legacy_dense_rows > 0);
        assert_eq!(legacy_routes.direct, 0);
        assert!(legacy_routes.forced_legacy > 0);
        assert!(legacy_dense_calls > 0 && legacy_dense_rows > 0);
        assert_same_views(&direct_changes, &legacy_changes);
        assert_same_views(&direct_snapshots, &legacy_snapshots);

        let direct_bytes = direct_stats.bytes_allocated;
        let legacy_bytes = legacy_stats.bytes_allocated;
        let expected_pairs = fixture.fanout;
        let tuple_payload = expected_pairs * std::mem::size_of::<(usize, usize)>();
        assert!(
            direct_bytes < legacy_bytes,
            "same public-push fixture should allocate fewer bytes on direct path: legacy={legacy_bytes}, direct={direct_bytes}"
        );
        println!(
            "dense_engine_push_allocations pairs={expected_pairs} direct_bytes={direct_bytes} legacy_bytes={legacy_bytes} net_difference_bytes={} operator_tuple_payload_bytes={tuple_payload} direct_calls={} legacy_calls={} dense_rows={}",
            legacy_bytes - direct_bytes,
            direct_routes.direct,
            legacy_routes.forced_legacy,
            direct_dense_rows,
        );
    }

    #[cfg(feature = "bench-allocation-observer")]
    #[test]
    #[ignore = "negative control: forced legacy public-push path must violate direct allocation assertion"]
    fn dense_join_direct_index_allocation_legacy_negative_control() {
        let fixture = Fixture {
            name: "direct-index-allocation-1x256",
            keys: 1,
            fanout: 256,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::AppendLeft,
        };
        let (input, update) = fixture.update();
        let (bytes, routes) = {
            let _mode = ModeGuard::new(true);
            let _legacy = crate::ops::join::test_support::LegacyDenseJoinGuard::force();
            let mut core = build_seeded(fixture, true);
            crate::ops::join::test_support::reset_counts();
            let region = Region::new(TEST_ALLOCATOR);
            core.push(input, &update).unwrap();
            (
                region.change().bytes_allocated,
                crate::ops::join::test_support::counts(),
            )
        };
        assert!(routes.forced_legacy > 0);
        let direct_budget = 353_999usize;
        assert!(
            bytes < direct_budget,
            "negative control expects forced legacy route to exceed direct budget {direct_budget}; observed={bytes}"
        );
    }

    #[path = "public_push_fixture.rs"]
    mod public_push_fixture;

    #[test]
    fn signed_allocator_delta_excludes_reallocation_adjustment() {
        let stats = stats_alloc::Stats {
            bytes_allocated: 128,
            bytes_deallocated: 64,
            bytes_reallocated: 64,
            ..stats_alloc::Stats::default()
        };
        assert_eq!(signed_alloc_free_delta(stats), 64);
        assert_eq!(
            active_features(),
            if cfg!(feature = "bench-allocation-observer") {
                &["bench-allocation-observer"][..]
            } else {
                &[][..]
            }
        );
        assert_eq!(
            allocator_observation_scope(),
            if cfg!(feature = "bench-allocation-observer") {
                "process-wide allocator aggregate during region; may include unrelated allocations/threads; not EngineCore-only or retained bytes"
            } else {
                "disabled"
            }
        );
    }

    #[cfg(feature = "bench-allocation-observer")]
    #[test]
    #[ignore = "process-wide allocator observation; run CARGO_BUILD_JOBS=1 RUST_TEST_THREADS=1 cargo test -p hotlap-engine --lib --features bench-allocation-observer core::tests::paired_bench::observer_contract_tests::stats_alloc_region_reports_reallocation_growth_and_shrink -- --ignored --exact --test-threads=1"]
    fn stats_alloc_region_reports_reallocation_growth_and_shrink() {
        let mut bytes = Vec::with_capacity(8);
        bytes.extend_from_slice(&[1; 8]);
        let grow = {
            let region = Region::new(TEST_ALLOCATOR);
            bytes.reserve_exact(128);
            region.change()
        };
        let shrink = {
            let region = Region::new(TEST_ALLOCATOR);
            bytes.truncate(4);
            bytes.shrink_to_fit();
            region.change()
        };
        assert!(grow.reallocations > 0);
        assert!(grow.bytes_allocated > 0);
        assert!(grow.bytes_reallocated > 0);
        assert!(shrink.reallocations > 0);
        assert!(shrink.bytes_deallocated > 0);
        assert!(shrink.bytes_reallocated < 0);
        assert_eq!(
            signed_alloc_free_delta(grow),
            grow.bytes_allocated as i128 - grow.bytes_deallocated as i128
        );
        assert_eq!(
            signed_alloc_free_delta(shrink),
            shrink.bytes_allocated as i128 - shrink.bytes_deallocated as i128
        );
    }

    #[cfg(feature = "bench-allocation-observer")]
    #[test]
    #[ignore = "process-wide allocator observation; run alone with one test thread"]
    fn row_state_final_retraction_deallocates_owned_payload_buffer() {
        use crate::arrange::state::RowState;
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["large", "small"])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "x".repeat(1 << 20),
                    "tiny".to_owned(),
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let key_converter = crate::keys::KeyConverter::new(schema.as_ref(), &[0]).unwrap();
        let payload_converter = crate::keys::KeyConverter::new(schema.as_ref(), &[1]).unwrap();
        let keys = key_converter.convert(batch.columns()).unwrap();
        let payloads = payload_converter.convert(batch.columns()).unwrap();
        let large_key = keys.row(0).owned();
        let large_payload = payloads.row(0).owned();
        let small_key = keys.row(1).owned();
        let small_payload = payloads.row(1).owned();
        let mut state = RowState::default();
        state
            .add(large_key.clone(), large_payload.clone(), 2)
            .unwrap();
        state
            .add(small_key.clone(), small_payload.clone(), 1)
            .unwrap();
        drop(batch);
        drop(keys);
        drop(payloads);

        let first_retract_key = large_key.clone();
        let first_retract_payload = large_payload.clone();
        let region = Region::new(TEST_ALLOCATOR);
        state
            .add(first_retract_key, first_retract_payload, -1)
            .unwrap();
        let non_final_deallocation = region.change();

        let final_retract_key = large_key;
        let final_retract_payload = large_payload;
        let region = Region::new(TEST_ALLOCATOR);
        state
            .add(final_retract_key, final_retract_payload, -1)
            .unwrap();
        let final_deallocation = region.change();

        eprintln!(
            "non_final_large_retraction_deallocated_bytes={} final_large_retraction_deallocated_bytes={}",
            non_final_deallocation.bytes_deallocated, final_deallocation.bytes_deallocated
        );
        let remaining = state.sorted();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0.as_slice(), small_key.as_ref());
        assert_eq!(remaining[0].1.as_ref(), small_payload.as_ref());
        assert_eq!(remaining[0].2, 1);
        assert!(final_deallocation.bytes_deallocated > non_final_deallocation.bytes_deallocated);
        assert!(
            final_deallocation.bytes_deallocated - non_final_deallocation.bytes_deallocated
                >= (1 << 20)
        );
    }
}

fn run_id() -> String {
    std::env::var("PAG3_RUN_ID").unwrap_or_else(|_| "unset".to_owned())
}

fn print_sample(fixture: Fixture, sample: &Sample, trial: &str) {
    println!(
        "{}",
        serde_json::json!({
            "record": "sample",
            "run_id": run_id(),
            "fixture": fixture.name,
            "trial": trial,
            "keys": fixture.keys,
            "fanout": fixture.fanout,
            "duplicate_payloads": fixture.duplicate_payloads,
            "skewed": fixture.skewed,
            "mixed_flat_view": fixture.mixed_flat_view,
            "unsafe_avg_history": fixture.unsafe_avg_history,
            "subset_group": fixture.subset_group,
            "update": format!("{:?}", fixture.update),
            "sample": sample,
        })
    );
}

#[test]
#[ignore = "paired release benchmark; run explicitly with --release --ignored --test-threads=1 --nocapture"]
fn paired_engine_core_group_join_benchmark() {
    println!(
        "{}",
        serde_json::json!({
            "record": "run",
            "run_id": run_id(),
            "source_commit": std::env::var("PAG3_SOURCE_COMMIT").unwrap_or_else(|_| "unset".into()),
            "source_manifest_sha256": std::env::var("PAG3_SOURCE_MANIFEST_SHA256").unwrap_or_else(|_| "unset".into()),
            "worktree_diff_sha256": std::env::var("PAG3_WORKTREE_DIFF_SHA256").unwrap_or_else(|_| "unset".into()),
            "active_features": active_features(),
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "timer": "Instant wall time around one public EngineCore::push only; setup, memory reports, take_changes, snapshots, and comparisons are outside the interval",
            "wall_time_comparable": !cfg!(feature = "bench-allocation-observer"),
            "memory_observation": if cfg!(feature = "bench-allocation-observer") { "test-only stats_alloc region reports process-wide allocation/deallocation/reallocation counters and signed allocated-minus-freed bytes during push; unrelated allocator activity/threads may contribute; this is not EngineCore-attributed or retained/live bytes; RSS pages and engine memory_report are separate proxies" } else { "allocator observer disabled in timing build; process RSS pages and engine memory_report are retained-state proxies" },
            "peak_observation": "stats_alloc does not expose peak; peak is not claimed. Dense join output rows/Arrow-array bytes are separately counted after join.apply and are not total temporary peak",
            "allocation_observer_enabled": cfg!(feature = "bench-allocation-observer"),
            "debug_assertions": cfg!(debug_assertions),
        })
    );
    let fixtures = [
        Fixture {
            name: "128x1-replace-left",
            keys: 128,
            fanout: 1,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::ReplaceLeft,
        },
        Fixture {
            name: "128x1-append-left-subset",
            keys: 128,
            fanout: 1,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: true,
            update: Update::AppendLeft,
        },
        Fixture {
            name: "128x1-append-right",
            keys: 128,
            fanout: 1,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::AppendRight,
        },
        Fixture {
            name: "128x4-replace-right",
            keys: 128,
            fanout: 4,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::ReplaceRight,
        },
        Fixture {
            name: "128x16-append-left",
            keys: 128,
            fanout: 16,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::AppendLeft,
        },
        Fixture {
            name: "128x16-replace-right-subset",
            keys: 128,
            fanout: 16,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: true,
            update: Update::ReplaceRight,
        },
        Fixture {
            name: "512x16-delete-right",
            keys: 512,
            fanout: 16,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::DeleteRight,
        },
        Fixture {
            name: "128x16-append-right",
            keys: 128,
            fanout: 16,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::AppendRight,
        },
        Fixture {
            name: "128x16-delete-left",
            keys: 128,
            fanout: 16,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::DeleteLeft,
        },
        Fixture {
            name: "128x16-delete-right",
            keys: 128,
            fanout: 16,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::DeleteRight,
        },
        Fixture {
            name: "128x4-duplicates-delete-left",
            keys: 128,
            fanout: 4,
            duplicate_payloads: true,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::DeleteLeft,
        },
        Fixture {
            name: "128x4-skew",
            keys: 128,
            fanout: 4,
            duplicate_payloads: false,
            skewed: true,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::ReplaceLeft,
        },
        Fixture {
            name: "128x4-subset-append-right",
            keys: 128,
            fanout: 4,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: true,
            update: Update::AppendRight,
        },
        Fixture {
            name: "128x64-append-left-subset",
            keys: 128,
            fanout: 64,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: true,
            update: Update::AppendLeft,
        },
        Fixture {
            name: "128x256-replace-left",
            keys: 128,
            fanout: 256,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::ReplaceLeft,
        },
        Fixture {
            name: "128x256-append-right-subset",
            keys: 128,
            fanout: 256,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: true,
            update: Update::AppendRight,
        },
        Fixture {
            name: "128x4-mixed-flat",
            keys: 128,
            fanout: 4,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: true,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::DeleteLeft,
        },
        Fixture {
            name: "1x1-unsafe-avg-row-control",
            keys: 1,
            fanout: 1,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: true,
            subset_group: false,
            update: Update::ReplaceLeft,
        },
    ];

    if let Ok(selected) = std::env::var("PAG3_MEMORY_FIXTURE") {
        let fixture = fixtures
            .iter()
            .copied()
            .find(|fixture| fixture.name == selected)
            .expect("PAG3_MEMORY_FIXTURE names a known fixture");
        let mode = std::env::var("PAG3_MEMORY_MODE").expect("PAG3_MEMORY_MODE=auto|forced_dense");
        let force_dense = match mode.as_str() {
            "auto" => false,
            "forced_dense" => true,
            _ => panic!("PAG3_MEMORY_MODE must be auto or forced_dense"),
        };
        if std::env::var_os("PAG3_MEMORY_RECOVERY").is_some() {
            if std::env::var_os("PAG3_PAIRED_RECOVERY").is_some() {
                recovery_paired_experiment(fixture);
                return;
            }
            println!("{}", recovery_sample(fixture, force_dense));
            return;
        }
        let (sample, changes, snapshots) = sample(fixture, force_dense);
        assert_eq!(changes.len(), snapshots.len());
        println!(
            "{{\"record\":\"memory_sample\",\"run_id\":\"{}\",\"fixture\":\"{}\",\"sample\":{}}}",
            run_id(),
            fixture.name,
            serde_json::to_string(&sample).unwrap()
        );
        return;
    }

    for fixture in fixtures {
        {
            let (warm_auto, warm_auto_changes, warm_auto_snapshots) = sample(fixture, false);
            let (warm_dense, warm_dense_changes, warm_dense_snapshots) = sample(fixture, true);
            print_sample(fixture, &warm_auto, "warmup-auto");
            print_sample(fixture, &warm_dense, "warmup-forced-dense");
            assert_same_views(&warm_auto_changes, &warm_dense_changes);
            assert_same_views(&warm_auto_snapshots, &warm_dense_snapshots);
        }
        let mut auto_samples = Vec::new();
        let mut dense_samples = Vec::new();
        for pair in 0..3 {
            let order = if pair % 2 == 0 {
                [(false, "auto"), (true, "forced_dense")]
            } else {
                [(true, "forced_dense"), (false, "auto")]
            };
            let mut outputs = Vec::new();
            for (force_dense, _) in order {
                let (sample, changes, snapshots) = sample(fixture, force_dense);
                print_sample(fixture, &sample, &format!("pair-{}", pair + 1));
                if force_dense {
                    assert_eq!(sample.factorized_commits, 0);
                    assert!(sample.dense_join_calls > 0);
                    assert_eq!(sample.flat_rows_cuts, u64::from(fixture.mixed_flat_view));
                    dense_samples.push(sample);
                } else if fixture.unsafe_avg_history {
                    assert_eq!(sample.factorized_commits, 0, "unsafe AVG must choose dense");
                    assert!(sample.dense_join_calls > 0);
                    auto_samples.push(sample);
                } else {
                    assert!(
                        sample.factorized_commits == 1,
                        "eligible fixture must execute factorized commit"
                    );
                    if !fixture.mixed_flat_view {
                        assert_eq!(sample.flat_rows_cuts, 0);
                        assert_eq!(
                            sample.dense_join_rows, 0,
                            "aggregate cut should not materialize dense join rows"
                        );
                        assert_eq!(sample.dense_join_array_bytes, 0);
                    } else {
                        assert_eq!(sample.flat_rows_cuts, 1);
                        assert!(
                            sample.dense_join_rows > 0,
                            "flat view must materialize its actual join output"
                        );
                    }
                    auto_samples.push(sample);
                }
                outputs.push((changes, snapshots));
            }
            assert_same_views(&outputs[0].0, &outputs[1].0);
            assert_same_views(&outputs[0].1, &outputs[1].1);
        }

        let median_ns = |samples: &[Sample]| median(samples.iter().map(|s| s.elapsed_ns));
        let median_rss_before =
            |samples: &[Sample]| median(samples.iter().map(|s| s.process_rss_pages_before));
        let median_rss_after =
            |samples: &[Sample]| median(samples.iter().map(|s| s.process_rss_pages_after));
        let median_dense_rows =
            |samples: &[Sample]| median(samples.iter().map(|s| s.dense_join_rows));
        let median_dense_bytes =
            |samples: &[Sample]| median(samples.iter().map(|s| s.dense_join_array_bytes));
        println!(
            "{}",
            serde_json::json!({
                "record": "paired_median",
                "run_id": run_id(),
                "fixture": fixture.name,
                "update": format!("{:?}", fixture.update),
                "subset_group": fixture.subset_group,
                "pairs": 3,
                "warmups": 1,
                "timed_trials": [1, 2, 3],
                "wall_time_comparable": !cfg!(feature = "bench-allocation-observer"),
                "dense_to_auto_wall_ratio": median_ns(&dense_samples) as f64 / median_ns(&auto_samples) as f64,
                "auto_wall_ns_median": median_ns(&auto_samples),
                "forced_dense_wall_ns_median": median_ns(&dense_samples),
                "auto_process_rss_pages_before_median": median_rss_before(&auto_samples),
                "auto_process_rss_pages_after_median": median_rss_after(&auto_samples),
                "dense_process_rss_pages_before_median": median_rss_before(&dense_samples),
                "dense_process_rss_pages_after_median": median_rss_after(&dense_samples),
                "auto_allocator_signed_alloc_free_delta_bytes_median": median(auto_samples.iter().map(|s| s.allocator_signed_alloc_free_delta_bytes)),
                "dense_allocator_signed_alloc_free_delta_bytes_median": median(dense_samples.iter().map(|s| s.allocator_signed_alloc_free_delta_bytes)),
                "auto_allocator_bytes_allocated_median": median(auto_samples.iter().map(|s| s.allocator_bytes_allocated)),
                "dense_allocator_bytes_allocated_median": median(dense_samples.iter().map(|s| s.allocator_bytes_allocated)),
                "auto_join_rows_median": median_dense_rows(&auto_samples),
                "auto_factorized_commits_median": median(auto_samples.iter().map(|s| s.factorized_commits)),
                "auto_flat_rows_cuts_median": median(auto_samples.iter().map(|s| s.flat_rows_cuts)),
                "dense_join_rows_median": median_dense_rows(&dense_samples),
                "auto_join_array_bytes_median": median_dense_bytes(&auto_samples),
                "dense_join_array_bytes_median": median_dense_bytes(&dense_samples),
                "auto_group_output_rows": auto_samples[0].group_output_rows,
                "dense_group_output_rows": dense_samples[0].group_output_rows,
                "auto_flat_output_rows": auto_samples[0].flat_output_rows,
                "dense_flat_output_rows": dense_samples[0].flat_output_rows,
                "engine_known_capacity_before_bytes": auto_samples[0].engine_known_capacity_before_bytes,
                "engine_known_capacity_after_bytes": auto_samples[0].engine_known_capacity_after_bytes,
            })
        );
    }
}
