//! Unit tests for the bounded view state.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, ZSetBatch};

use super::EngineCore;

mod avg_consume;
mod filter_project_bench;
mod filter_project_graph;
mod graph_review;
mod paired_bench;
mod recovery;
mod recovery_signed_history;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

/// Builds a single-column Z-set of `(key, diff)` rows.
fn zset(rows: &[(i64, i64)]) -> ZSetBatch {
    let keys: Vec<i64> = rows.iter().map(|row| row.0).collect();
    let diffs: Vec<i64> = rows.iter().map(|row| row.1).collect();
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(keys))];
    let batch = RecordBatch::try_new(schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn time_zset(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let keys = rows.iter().map(|row| row.0).collect::<Vec<_>>();
    let times = rows.iter().map(|row| row.1).collect::<Vec<_>>();
    let diffs = rows.iter().map(|row| row.2).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(times)),
        ],
    )
    .unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn aggregate_zset(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let keys = rows.iter().map(|row| row.0).collect::<Vec<_>>();
    let values = rows.iter().map(|row| row.1).collect::<Vec<_>>();
    let diffs = rows.iter().map(|row| row.2).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn joined_group_plan() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    }
}

fn pair_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn pairs(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    let batch = RecordBatch::try_new(
        pair_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ZSetBatch::new(
        batch,
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
    )
    .unwrap()
}

#[test]
fn grouped_join_push_and_checkpoint_continuation_preserve_dense_results() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(ViewId(0), &joined_group_plan()).unwrap();
    core.push(InputId(0), &pairs(&[(1, 10, 1), (1, 20, 1)]))
        .unwrap();
    core.push(InputId(1), &pairs(&[(1, 3, 1), (1, 4, 1)]))
        .unwrap();
    let checkpoint = core.checkpoint().unwrap();
    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    restored.push(InputId(0), &pairs(&[(1, 30, 1)])).unwrap();
    let actual = crate::zset::consolidate(&restored.snapshot(ViewId(0)).unwrap()).unwrap();
    let count = actual
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.values(), &[6]);
}

#[test]
fn direct_count_sum_group_join_uses_certified_path_for_safe_updates() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count(), AggSpec::sum(1)],
    };
    core.build_view(ViewId(0), &plan).unwrap();
    core.push(InputId(0), &pairs(&[(1, 2, 1)])).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|n| n.set(0));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|n| n.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|n| n.set(0));
    core.push(InputId(1), &pairs(&[(1, 3, 1)])).unwrap();
    crate::core::graph::FACTORIZED_CUTS
        .with(|n| assert!(n.get() > 0, "certified COUNT/SUM cut is used"));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS
        .with(|n| assert_eq!(n.get(), 0, "certified cut avoids row materialization"));
    crate::core::graph::FLAT_ROWS_CUTS.with(|n| assert_eq!(n.get(), 0));
    let actual = crate::zset::consolidate(&core.snapshot(ViewId(0)).unwrap()).unwrap();
    assert_eq!(actual.len(), 1);
    let count = actual
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = actual
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.values(), &[1]);
    assert_eq!(sum.values(), &[2]);
    core.push(InputId(0), &pairs(&[(1, 2, -1), (1, 5, 1)]))
        .unwrap();
    let checkpoint = core.checkpoint().unwrap();
    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    restored
        .push(InputId(1), &pairs(&[(1, 3, -1), (1, 8, 1)]))
        .unwrap();
    let continued = crate::zset::consolidate(&restored.snapshot(ViewId(0)).unwrap()).unwrap();
    let count = continued
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = continued
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.values(), &[1]);
    assert_eq!(sum.values(), &[5]);
}

#[test]
fn projected_integer_avg_group_join_certifies_and_preserves_null_keys() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Project {
            input: Box::new(Plan::Join {
                left: Box::new(Plan::Project {
                    input: Box::new(Plan::Source(InputId(0))),
                    cols: vec![1, 0],
                }),
                right: Box::new(Plan::Project {
                    input: Box::new(Plan::Source(InputId(1))),
                    cols: vec![1, 0],
                }),
                left_key: vec![0],
                right_key: vec![0],
            }),
            cols: vec![3, 0, 1, 2],
        }),
        key: vec![1],
        aggs: vec![AggSpec::count(), AggSpec::sum(2), AggSpec::avg(2)],
    };
    core.build_view(ViewId(0), &plan).unwrap();
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, true),
        Field::new("key", DataType::Int64, false),
    ]));
    let source = |rows: &[(Option<i64>, i64, i64)]| {
        let batch = RecordBatch::try_new(
            source_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        ZSetBatch::new(
            batch,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        )
        .unwrap()
    };
    core.push(InputId(0), &source(&[(Some(2), 1, 1), (None, 1, 1)]))
        .unwrap();
    core.push(InputId(1), &source(&[(Some(9), 1, 1)])).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|n| assert!(n.get() > 0));
    let actual = crate::zset::consolidate(&core.snapshot(ViewId(0)).unwrap()).unwrap();
    assert_eq!(actual.len(), 1);
    let count = actual
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = actual
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let avg = actual
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(count.values(), &[2]);
    assert_eq!(sum.values(), &[2]);
    assert_eq!(sum.null_count(), 0);
    assert_eq!(avg.values(), &[2.0]);
    assert_eq!(avg.null_count(), 0);
}

#[test]
fn integer_avg_and_forced_dense_group_join_match_each_delta_and_pending_restore() {
    fn core() -> EngineCore {
        let mut core = EngineCore::new();
        core.register_input(InputId(0)).unwrap();
        core.register_input(InputId(1)).unwrap();
        core.build_view(
            ViewId(0),
            &Plan::GroupAggregate {
                input: Box::new(Plan::Join {
                    left: Box::new(Plan::Source(InputId(0))),
                    right: Box::new(Plan::Source(InputId(1))),
                    left_key: vec![0],
                    right_key: vec![0],
                }),
                key: vec![0],
                aggs: vec![AggSpec::count(), AggSpec::sum(1), AggSpec::avg(1)],
            },
        )
        .unwrap();
        core.tap_view(ViewId(0)).unwrap();
        core
    }
    fn same(a: &ZSetBatch, b: &ZSetBatch) {
        assert_eq!(a.schema(), b.schema());
        assert_eq!(a.len(), b.len());
        assert_eq!(a.diff().to_data(), b.diff().to_data());
        for (a, b) in a.batch.columns().iter().zip(b.batch.columns()) {
            assert_eq!(a.to_data(), b.to_data());
        }
    }
    let mut factor = core();
    let mut dense = core();
    crate::core::graph::FACTORIZED_CUTS.with(|n| n.set(0));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|n| n.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|n| n.set(0));
    factor.push(InputId(0), &pairs(&[(1, 2, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(true));
    dense.push(InputId(0), &pairs(&[(1, 2, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(false));
    same(
        &factor.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    let checkpoint = factor.checkpoint().unwrap();
    factor = EngineCore::new();
    factor.register_input(InputId(0)).unwrap();
    factor.register_input(InputId(1)).unwrap();
    factor
        .build_view(
            ViewId(0),
            &Plan::GroupAggregate {
                input: Box::new(Plan::Join {
                    left: Box::new(Plan::Source(InputId(0))),
                    right: Box::new(Plan::Source(InputId(1))),
                    left_key: vec![0],
                    right_key: vec![0],
                }),
                key: vec![0],
                aggs: vec![AggSpec::count(), AggSpec::sum(1), AggSpec::avg(1)],
            },
        )
        .unwrap();
    factor.tap_view(ViewId(0)).unwrap();
    factor.restore(&checkpoint).unwrap();
    factor.push(InputId(1), &pairs(&[(1, 3, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(true));
    dense.push(InputId(1), &pairs(&[(1, 3, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(false));
    same(
        &factor.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same(
        &factor.snapshot(ViewId(0)).unwrap(),
        &dense.snapshot(ViewId(0)).unwrap(),
    );
    factor
        .push(InputId(0), &pairs(&[(1, 2, -1), (1, 5, 1)]))
        .unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(true));
    dense
        .push(InputId(0), &pairs(&[(1, 2, -1), (1, 5, 1)]))
        .unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(false));
    same(
        &factor.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same(
        &factor.snapshot(ViewId(0)).unwrap(),
        &dense.snapshot(ViewId(0)).unwrap(),
    );
    crate::core::graph::FACTORIZED_CUTS.with(|n| assert!(n.get() > 0));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|n| assert!(n.get() > 0));
}

#[test]
fn unsafe_avg_history_stays_on_flat_fallback() {
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count(), AggSpec::sum(1), AggSpec::avg(1)],
    };
    let make_core = || {
        let mut c = EngineCore::new();
        c.register_input(InputId(0)).unwrap();
        c.register_input(InputId(1)).unwrap();
        c.build_view(ViewId(0), &plan).unwrap();
        c.tap_view(ViewId(0)).unwrap();
        c
    };
    let mut factor = make_core();
    let mut dense = make_core();
    crate::core::graph::FACTORIZED_CUTS.with(|n| n.set(0));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|n| n.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|n| n.set(0));
    factor
        .push(InputId(0), &pairs(&[(1, (1_i64 << 53) + 1, 1)]))
        .unwrap();
    dense
        .push(InputId(0), &pairs(&[(1, (1_i64 << 53) + 1, 1)]))
        .unwrap();
    factor.push(InputId(1), &pairs(&[(1, 0, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(true));
    dense.push(InputId(1), &pairs(&[(1, 0, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(false));
    crate::core::graph::FACTORIZED_CUTS
        .with(|n| assert_eq!(n.get(), 0, "unsafe AVG history must fallback"));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|n| assert!(n.get() > 0));
    same_batches(
        &factor.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same_group_aggregate_state(&factor, &dense);
    let replacement = pairs(&[(1, (1_i64 << 53) + 1, -1), (1, 4, 1)]);
    factor.push(InputId(0), &replacement).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(true));
    dense.push(InputId(0), &replacement).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(false));
    same_batches(
        &factor.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same_group_aggregate_state(&factor, &dense);
    crate::core::graph::FACTORIZED_CUTS.with(|n| n.set(0));
    factor.push(InputId(0), &pairs(&[(1, 2, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(true));
    dense.push(InputId(0), &pairs(&[(1, 2, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|f| f.set(false));
    same_batches(
        &factor.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same_group_aggregate_state(&factor, &dense);
    same_batches(
        &factor.snapshot(ViewId(0)).unwrap(),
        &dense.snapshot(ViewId(0)).unwrap(),
    );
    crate::core::graph::FACTORIZED_CUTS.with(|n| {
        assert!(
            n.get() > 0,
            "exact history should become eligible after Dense fallback"
        )
    });
    crate::core::graph::FLAT_ROWS_CUTS.with(|n| assert!(n.get() > 0));
}

fn same_batches(a: &ZSetBatch, b: &ZSetBatch) {
    if a.is_empty() || b.is_empty() {
        assert!(a.is_empty() && b.is_empty());
        return;
    }
    assert_eq!(a.schema(), b.schema());
    assert_eq!(a.len(), b.len());
    assert_eq!(a.diff().to_data(), b.diff().to_data());
    for (a, b) in a.batch.columns().iter().zip(b.batch.columns()) {
        assert_eq!(a.to_data(), b.to_data());
    }
}

fn same_group_aggregate_state(a: &EngineCore, b: &EngineCore) {
    fn group(
        snapshot: &hotlap_core::snapshot::EngineSnapshot,
    ) -> &hotlap_core::snapshot::GroupState {
        snapshot.views[0]
            .operators
            .iter()
            .flatten()
            .find_map(|operator| match operator {
                hotlap_core::snapshot::OperatorState::Group(state) => Some(state),
                _ => None,
            })
            .expect("view contains GroupAggregate")
    }
    let a = a.checkpoint().unwrap();
    let b = b.checkpoint().unwrap();
    assert_eq!(group(&a), group(&b));
}

fn group_plan() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    }
}

/// Number of consolidated rows retained by a view's output map.
fn output_rows(core: &EngineCore, view: ViewId) -> usize {
    core.views[&view].output.len()
}

#[test]
fn view_output_rejects_diff_sum_overflow() {
    // The output map sums multiplicities; two i64::MAX pushes cannot fit.
    let mut output = super::output::ViewOutput::default();
    output.update(&zset(&[(1, i64::MAX)])).unwrap();
    assert!(output.update(&zset(&[(1, i64::MAX)])).is_err());
}

#[test]
fn memory_report_counts_group_state_and_keeps_unsupported_families_explicit() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &zset(&[(1, 1), (2, 1)])).unwrap();

    let report = core.memory_report().unwrap();

    assert_eq!(report.logical_payload_bytes, 16 + 2 * 19);
    assert!(
        report
            .covered_categories
            .contains(&"aggregates".to_string())
    );
    assert!(
        report
            .covered_categories
            .contains(&"consolidated_outputs".to_string())
    );
    assert_eq!(report.exact_heap_bytes, None);
    assert_eq!(
        report.known_backing_capacity_bytes,
        2 * (19 + std::mem::size_of::<hotlap_core::snapshot::AggValue>() + 8)
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<AggSpec>()
    );
    assert!(
        report.estimated_structural_bytes
            >= 2 * std::mem::size_of::<(Vec<u8>, hotlap_core::snapshot::GroupEntry)>()
    );
    assert!(
        report
            .unknown_categories
            .contains(&"aggregate_hash_map_allocator_overhead".to_string())
    );
    assert!(
        report
            .unknown_categories
            .contains(&"view_plans_and_taps".to_string())
    );
    assert_eq!(report.arrow_buffer_capacity_bytes, 0);
    assert!(!report.complete);
}

#[test]
fn memory_report_rejects_poisoned_core_without_mutating_checkpoint() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    let before = core.checkpoint().unwrap();
    let report = core.memory_report().unwrap();
    let after = core.checkpoint().unwrap();
    assert_eq!(before, after);
    assert_eq!(report.logical_payload_bytes, 8 + 19);

    core.failed = true;
    assert!(core.memory_report().is_err());
}

#[test]
fn memory_report_query_does_not_change_output_after_retraction() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &zset(&[(1, 2)])).unwrap();
    core.push(InputId(0), &zset(&[(1, -1)])).unwrap();
    let before = core.snapshot(ViewId(0)).unwrap();
    let first = core.memory_report().unwrap();
    let second = core.memory_report().unwrap();
    let after = core.snapshot(ViewId(0)).unwrap();
    assert_eq!(
        crate::zset::consolidate(&before).unwrap().batch,
        crate::zset::consolidate(&after).unwrap().batch
    );
    assert_eq!(first, second);
}

#[test]
fn memory_report_checked_counter_rejects_overflow() {
    assert!(super::memory::checked_add(usize::MAX, 1, "capacity").is_err());
    assert!(super::memory::checked_capacity_bytes(usize::MAX, 2).is_err());
}

#[test]
fn memory_report_counts_projection_capacity_in_bytes_not_element_count() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::Project {
            input: Box::new(Plan::Source(InputId(0))),
            cols: vec![0],
        },
    )
    .unwrap();
    let report = core.memory_report().unwrap();
    assert_eq!(
        report.known_backing_capacity_bytes,
        std::mem::size_of::<usize>()
    );
}

#[test]
fn memory_report_counts_aggregate_values_vec_capacity_in_bytes() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    let report = core.memory_report().unwrap();
    let aggregate_capacity = std::mem::size_of::<hotlap_core::snapshot::AggValue>();
    assert_eq!(
        report.known_backing_capacity_bytes,
        19 + aggregate_capacity + 8 + std::mem::size_of::<usize>() + std::mem::size_of::<AggSpec>()
    );
}

#[test]
fn memory_report_debug_does_not_expose_allocation_identities() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    let debug = format!("{:?}", core.memory_report().unwrap());
    assert!(!debug.contains("allocations"));
    assert!(!debug.contains("0x"));
}

#[test]
fn memory_report_tracks_pending_tap_bytes_only_until_the_tap_is_drained() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    let pending = core.memory_report().unwrap();
    assert!(pending.arrow_buffer_capacity_bytes > 0);
    assert!(
        pending
            .unknown_categories
            .iter()
            .any(|item| item == "view_pending")
    );
    drop(core.take_changes(ViewId(0)).unwrap());
    let drained = core.memory_report().unwrap();
    assert_eq!(drained.arrow_buffer_capacity_bytes, 0);
    assert!(
        !drained
            .unknown_categories
            .iter()
            .any(|item| item == "view_pending")
    );
}

#[test]
fn memory_report_counts_arrow_buffers_retained_by_pending_taps() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();

    let report = core.memory_report().unwrap();

    assert!(report.arrow_buffer_capacity_bytes >= 3 * std::mem::size_of::<i64>());
    assert!(
        !report
            .opaque_external_categories
            .contains(&"arrow_external_buffers".to_string())
    );
}

#[test]
fn ungrouped_join_view_uses_canonical_flat_materializer() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        },
    )
    .unwrap();
    let left = pairs(&[(1, 10, 2), (1, 11, -1), (2, 20, 1)]);
    let right = pairs(&[(1, 30, 1), (1, 31, 2), (2, 40, -1)]);
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| count.set(0));
    core.push(InputId(0), &left).unwrap();
    core.push(InputId(1), &right).unwrap();

    let mut dense = crate::ops::join::Join::new(&[0], &[0]);
    let expected = dense.apply(&left, &right).unwrap();
    let actual = crate::zset::consolidate(&core.snapshot(ViewId(0)).unwrap()).unwrap();
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| assert!(count.get() > 0));
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.len(), expected.len());
    assert_eq!(actual.diff().to_data(), expected.diff().to_data());
    for (actual, expected) in actual.batch.columns().iter().zip(expected.batch.columns()) {
        assert_eq!(actual.to_data(), expected.to_data());
    }
}

#[test]
fn memory_report_visits_current_join_arrangements_and_tracks_retractions() {
    let join_plan = Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![0],
        right_key: vec![0],
    };
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(ViewId(0), &join_plan).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    core.push(InputId(1), &zset(&[(1, 1)])).unwrap();

    let checkpoint_before_report = core.checkpoint().unwrap();
    let retained = core.memory_report().unwrap();
    assert_eq!(core.checkpoint().unwrap(), checkpoint_before_report);
    assert!(!retained.unknown_categories.contains(&"joins".to_string()));
    assert!(retained.join_logical_payload_bytes >= 16);
    assert!(
        retained.join_known_key_capacity_bytes >= 5 * std::mem::size_of::<usize>(),
        "join key capacity: {}",
        retained.join_known_key_capacity_bytes
    );

    core.push(InputId(0), &zset(&[(1, -1)])).unwrap();
    let retracted = core.memory_report().unwrap();
    assert!(!retracted.unknown_categories.contains(&"joins".to_string()));
    assert_eq!(retracted.join_logical_payload_bytes, 9);
}

#[test]
fn memory_report_tracks_join_pending_batches_until_the_sibling_schema_arrives() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        },
    )
    .unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();

    let pending = core.memory_report().unwrap();
    assert!(pending.arrow_buffer_capacity_bytes > 0);
    assert!(
        pending
            .unknown_categories
            .contains(&"join_pending_batch_metadata".to_string())
    );

    core.push(InputId(1), &zset(&[(1, 1)])).unwrap();
    let initialized = core.memory_report().unwrap();
    assert_eq!(initialized.arrow_buffer_capacity_bytes, 0);
    assert!(initialized.join_logical_payload_bytes > 0);
}

#[test]
fn memory_report_visits_open_windows_and_drops_closed_window_payloads() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        hotlap_core::WatermarkSpec {
            time_col: 1,
            lag: 100,
        },
    )
    .unwrap();
    core.build_view(
        ViewId(0),
        &Plan::TumbleCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            time_col: 1,
            size: 10,
        },
    )
    .unwrap();
    core.push(InputId(0), &time_zset(&[(1, 1, 1)])).unwrap();
    core.push(InputId(0), &time_zset(&[(1, 11, 1)])).unwrap();
    core.push(InputId(0), &time_zset(&[(1, 21, 1)])).unwrap();

    let open = core.memory_report().unwrap();
    assert!(!open.unknown_categories.contains(&"windows".to_string()));
    assert_eq!(open.window_logical_payload_bytes, 54);

    core.push(InputId(0), &time_zset(&[(1, 131, 1)])).unwrap();
    let closed = core.memory_report().unwrap();
    assert_eq!(closed.window_logical_payload_bytes, 18);
}

#[test]
fn memory_report_visits_minmax_multiset_entries_across_retractions() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![AggSpec::min(1), AggSpec::max(1)],
        },
    )
    .unwrap();
    core.push(InputId(0), &aggregate_zset(&[(1, 4, 1), (1, 8, 1)]))
        .unwrap();

    let before = core.memory_report().unwrap();
    assert!(
        !before
            .unknown_categories
            .contains(&"aggregate_minmax".to_string())
    );
    assert_eq!(before.minmax_distinct_entries, 4);

    core.push(InputId(0), &aggregate_zset(&[(1, 4, -1)]))
        .unwrap();
    let after = core.memory_report().unwrap();
    assert!(
        !after
            .unknown_categories
            .contains(&"aggregate_minmax".to_string())
    );
    assert_eq!(after.minmax_distinct_entries, 2);
}

#[test]
fn memory_report_counts_arrow_buffers_held_by_input_retention() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.set_input_retention(2).unwrap();
    core.build_view(ViewId(0), &Plan::Source(InputId(0)))
        .unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();

    let report = core.memory_report().unwrap();

    assert_eq!(
        report.arrow_buffer_capacity_bytes,
        2 * std::mem::size_of::<i64>()
    );
    assert!(
        report
            .unknown_categories
            .contains(&"input_retention_batch_metadata".to_string())
    );
}

#[test]
fn view_output_state_does_not_grow_with_history() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &Plan::Source(InputId(0)))
        .unwrap();

    // Every push is a single non-zero row. Insert each key once, then retract
    // and re-insert as the pattern cycles: the output map never exceeds the
    // distinct-key count no matter how many pushes run.
    let keys = 10i64;
    let epochs = 200;
    for epoch in 0..epochs {
        let key = epoch % keys;
        let diff = if (epoch / keys) % 2 == 0 { 1 } else { -1 };
        core.push(InputId(0), &zset(&[(key, diff)])).unwrap();
    }

    assert!(
        output_rows(&core, ViewId(0)) <= keys as usize,
        "view output state grew with history"
    );
    // The final cycle was a retraction block, so the net state is empty.
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());
}

#[test]
fn row_converter_is_built_once_not_per_push() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();

    // After the first push freezes the schema, further pushes must reuse the
    // cached converters instead of building O(pushes) of them.
    crate::work::reset();
    for _ in 0..200 {
        core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    }
    assert_eq!(
        crate::work::converter_builds(),
        0,
        "row converters were rebuilt per push"
    );
}

#[test]
fn view_output_state_tracks_current_state_not_history() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();

    // One logical row (`key 1`) updated many times: the current state is one
    // row regardless of how many deltas produced it.
    for _ in 0..100 {
        core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    }

    assert_eq!(output_rows(&core, ViewId(0)), 1);
    assert_eq!(core.snapshot(ViewId(0)).unwrap().len(), 1);
}

/// A one-row push must encode only its delta, not the resident state.
///
/// With a large resident state this fails as soon as push falls back to
/// consolidating the whole state (which re-encodes every retained row).
#[test]
fn push_encodes_only_the_delta() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();

    // Seed a large resident state with distinct keys.
    let seed: Vec<(i64, i64)> = (0..2_000).map(|key| (key, 1)).collect();
    core.push(InputId(0), &zset(&seed)).unwrap();
    assert_eq!(output_rows(&core, ViewId(0)), 2_000);

    // A push of one new key must encode only its delta, not the resident state.
    crate::work::reset();
    core.push(InputId(0), &zset(&[(10_000, 1)])).unwrap();
    assert_eq!(output_rows(&core, ViewId(0)), 2_001);
    assert!(
        crate::work::encoded_rows() <= 2,
        "1-row push re-encoded the resident state"
    );
}
