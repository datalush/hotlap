//! Matched ignored push probe for pre-start duplicate root joins.

use super::*;
use crate::core::tests::{ForcePrivateSharedArrangements, diff_values};
use arrow::array::StringArray;

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    rows: usize,
    width: usize,
    views: usize,
}

const CASES: [Case; 3] = [
    Case {
        name: "2-wide",
        rows: 256,
        width: 2048,
        views: 2,
    },
    Case {
        name: "8-wide",
        rows: 256,
        width: 2048,
        views: 8,
    },
    Case {
        name: "2-narrow",
        rows: 4,
        width: 32,
        views: 2,
    },
];

fn selected_case(name: &str) -> Case {
    CASES
        .into_iter()
        .find(|case| case.name == name)
        .expect("unknown FLUSS_SHARED_CASE; expected 2-wide, 8-wide, or 2-narrow")
}

fn payload(side: &str, row: usize, width: usize) -> String {
    let prefix = format!("{side}-{row:06}-");
    assert!(prefix.len() <= width);
    format!("{prefix}{}", "x".repeat(width - prefix.len()))
}

fn rows(case: Case, side: &str, count: usize, delta: bool) -> ZSetBatch {
    let start = if delta { case.rows } else { 0 };
    let key_start = if delta { 0 } else { start };
    let keys = (key_start..key_start + count)
        .map(|i| i as i64)
        .collect::<Vec<_>>();
    let values = (start..start + count)
        .map(|i| payload(side, i, case.width))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)) as ArrayRef,
            Arc::new(StringArray::from(values)) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1_i64; count]))).unwrap()
}

fn root_plan() -> Plan {
    Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![0],
        right_key: vec![0],
    }
}

fn contract(case: Case, shared: bool) {
    let private_guard = (!shared).then(ForcePrivateSharedArrangements::new);
    let mut core = EngineCore::new();
    core.set_input_retention(8).unwrap();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    for id in 0..case.views {
        core.build_view(ViewId(id as u32), &root_plan()).unwrap();
        core.tap_view(ViewId(id as u32)).unwrap();
    }
    let left = rows(case, "left", case.rows, false);
    let right = rows(case, "right", case.rows, false);
    core.push(InputId(0), &left).unwrap();
    core.push(InputId(1), &right).unwrap();
    for id in 0..case.views {
        let drained = core.take_changes(ViewId(id as u32)).unwrap();
        assert_output_batch(&drained, case, "left", "right", 0..case.rows);
    }
    assert_physical_arrangements(&core, case, shared, 0);
    let delta_count = if case.name == "2-narrow" { 2 } else { 32 };
    let delta = rows(case, "left-new", delta_count, true);
    core.push(InputId(0), &delta).unwrap();
    assert_physical_arrangements(&core, case, shared, delta_count);
    for id in 0..case.views {
        let view = ViewId(id as u32);
        let changes = core.take_changes(view).unwrap();
        assert_output_batch(&changes, case, "left-new", "right", 0..delta_count);
        assert_output_snapshot(&mut core, view, case, delta_count);
    }
    drop(private_guard);
}

fn assert_output_batch(
    batch: &ZSetBatch,
    case: Case,
    left_side: &str,
    right_side: &str,
    rows: std::ops::Range<usize>,
) {
    assert_root_schema(batch);
    let keys = batch
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left_payload = batch
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let right_keys = batch
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right_payload = batch
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let diffs = diff_values(batch);
    assert_eq!(batch.batch.num_rows(), rows.len());
    let actual = (0..batch.batch.num_rows())
        .map(|i| {
            (
                keys.value(i),
                left_payload.value(i).to_owned(),
                right_keys.value(i),
                right_payload.value(i).to_owned(),
                diffs[i],
            )
        })
        .collect::<Vec<_>>();
    let expected = rows
        .map(|i| {
            let left_index = if left_side == "left-new" {
                case.rows + i
            } else {
                i
            };
            (
                i as i64,
                payload(left_side, left_index, case.width),
                i as i64,
                payload(right_side, i, case.width),
                1,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn assert_root_schema(batch: &ZSetBatch) {
    assert_eq!(batch.batch.schema().fields().len(), 4);
    assert_eq!(batch.batch.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(batch.batch.schema().field(1).data_type(), &DataType::Utf8);
    assert_eq!(batch.batch.schema().field(2).data_type(), &DataType::Int64);
    assert_eq!(batch.batch.schema().field(3).data_type(), &DataType::Utf8);
    assert!(
        batch
            .batch
            .schema()
            .fields()
            .iter()
            .all(|field| !field.is_nullable())
    );
}

fn assert_output_snapshot(core: &mut EngineCore, view: ViewId, case: Case, delta_count: usize) {
    let snapshot = core.snapshot(view).unwrap();
    assert_root_schema(&snapshot);
    let keys = snapshot
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left_payload = snapshot
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let right_keys = snapshot
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right_payload = snapshot
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let diffs = diff_values(&snapshot);
    let actual = (0..snapshot.batch.num_rows())
        .map(|i| {
            (
                keys.value(i),
                left_payload.value(i).to_owned(),
                right_keys.value(i),
                right_payload.value(i).to_owned(),
                diffs[i],
            )
        })
        .collect::<Vec<_>>();
    let mut expected = (0..case.rows)
        .map(|i| {
            (
                i as i64,
                payload("left", i, case.width),
                i as i64,
                payload("right", i, case.width),
                1,
            )
        })
        .collect::<Vec<_>>();
    expected.extend((0..delta_count).map(|i| {
        (
            i as i64,
            payload("left-new", case.rows + i, case.width),
            i as i64,
            payload("right", i, case.width),
            1,
        )
    }));
    expected.sort();
    assert_eq!(actual, expected);
}

fn assert_physical_arrangements(core: &EngineCore, case: Case, shared: bool, delta_count: usize) {
    assert_eq!(
        core.shared_arrangements.owner_count(),
        if shared { 2 } else { 0 }
    );
    let expected_left = case.rows + delta_count;
    for index in 0..case.views {
        let graph = &core.views[&ViewId(index as u32)].graph;
        assert_eq!(
            graph.root_join_sources_for_test(),
            Some((InputId(0), InputId(1)))
        );
        if shared {
            let handles = graph
                .root_shared_handles_for_test()
                .expect("eligible root view has arena bindings");
            if index == 0 {
                assert_eq!(
                    core.shared_arrangements
                        .owner_distinct_row_count(handles.0)
                        .unwrap(),
                    expected_left
                );
                assert_eq!(
                    core.shared_arrangements
                        .owner_distinct_row_count(handles.1)
                        .unwrap(),
                    case.rows
                );
            } else {
                assert_eq!(
                    graph.root_shared_handles_for_test(),
                    core.views[&ViewId(0)].graph.root_shared_handles_for_test()
                );
            }
            assert_eq!(
                core.shared_arrangements
                    .owner_lease_count(handles.0)
                    .unwrap(),
                case.views
            );
            assert_eq!(
                core.shared_arrangements
                    .owner_lease_count(handles.1)
                    .unwrap(),
                case.views
            );
            assert_eq!(graph.root_private_arrangement_rows_for_test(), Some((0, 0)));
        } else {
            assert!(graph.root_shared_handles_for_test().is_none());
            assert_eq!(
                graph.root_private_arrangement_rows_for_test(),
                Some((expected_left, case.rows))
            );
        }
    }
}

#[test]
fn shared_arena_push_contracts_all_cases_both_modes() {
    for case in CASES {
        contract(case, true);
        contract(case, false);
    }
}

#[test]
#[ignore = "one fresh process per FLUSS_SHARED_CASE × FLUSS_SHARED_MODE; isolated matched root-join push probe"]
fn shared_arena_child_push_probe() {
    let case =
        selected_case(&std::env::var("FLUSS_SHARED_CASE").expect("FLUSS_SHARED_CASE required"));
    let mode = std::env::var("FLUSS_SHARED_MODE").expect("FLUSS_SHARED_MODE required");
    assert!(
        mode == "shared" || mode == "private",
        "FLUSS_SHARED_MODE must be shared or private"
    );
    let shared = mode == "shared";

    let private_guard = (!shared).then(ForcePrivateSharedArrangements::new);
    let mut core = EngineCore::new();
    core.set_input_retention(8).unwrap();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    for id in 0..case.views {
        core.build_view(ViewId(id as u32), &root_plan()).unwrap();
        core.tap_view(ViewId(id as u32)).unwrap();
    }
    let left = rows(case, "left", case.rows, false);
    let right = rows(case, "right", case.rows, false);
    core.push(InputId(0), &left).unwrap();
    core.push(InputId(1), &right).unwrap();
    for id in 0..case.views {
        let seed_changes = core.take_changes(ViewId(id as u32)).unwrap();
        assert_output_batch(&seed_changes, case, "left", "right", 0..case.rows);
    }
    assert_physical_arrangements(&core, case, shared, 0);
    let delta_count = if case.name == "2-narrow" { 2 } else { 32 };
    let delta = rows(case, "left-new", delta_count, true);
    let handles = core.views[&ViewId(0)].graph.root_shared_handles_for_test();
    let owner_commits_before = handles.map(|(left, right)| {
        (
            core.shared_arrangements.owner_commit_count(left).unwrap(),
            core.shared_arrangements.owner_commit_count(right).unwrap(),
        )
    });
    let source_revision_before = core.source_revisions[&InputId(0)];
    let right_revision_before = core.source_revisions[&InputId(1)];
    #[cfg(feature = "bench-allocation-observer")]
    let allocation_region = Region::new(TEST_ALLOCATOR);
    let start = Instant::now();
    let push_result = core.push(InputId(0), &delta);
    let elapsed_ns = start.elapsed().as_nanos();
    #[cfg(feature = "bench-allocation-observer")]
    let allocation_stats = Some(allocation_region.change());
    #[cfg(not(feature = "bench-allocation-observer"))]
    let allocation_stats: Option<stats_alloc::Stats> = None;
    push_result.unwrap();
    let owner_commits_after = handles.map(|(left, right)| {
        (
            core.shared_arrangements.owner_commit_count(left).unwrap(),
            core.shared_arrangements.owner_commit_count(right).unwrap(),
        )
    });
    assert_physical_arrangements(&core, case, shared, delta_count);
    let mut output_rows = Vec::new();
    for id in 0..case.views {
        let view = ViewId(id as u32);
        let changes = core.take_changes(view).unwrap();
        assert_output_batch(&changes, case, "left-new", "right", 0..delta_count);
        output_rows.push(changes.batch.num_rows());
        assert_output_snapshot(&mut core, view, case, delta_count);
    }
    let report = core.memory_report().unwrap();
    let handles_now = core.views[&ViewId(0)].graph.root_shared_handles_for_test();
    let arena_owner_payload_bytes = handles_now
        .map(|(left, right)| {
            core.shared_arrangements
                .owner(left)
                .unwrap()
                .memory_usage()
                .unwrap()
                .0
                + if left == right {
                    0
                } else {
                    core.shared_arrangements
                        .owner(right)
                        .unwrap()
                        .memory_usage()
                        .unwrap()
                        .0
                }
        })
        .unwrap_or(0);
    let private_arrangement_payload_bytes = report
        .join_logical_payload_bytes
        .checked_sub(arena_owner_payload_bytes)
        .expect("owner payload must be included in join payload coverage");
    let per_view_output_payload_bytes = (0..case.views)
        .map(|id| {
            core.views[&ViewId(id as u32)]
                .output
                .memory_usage()
                .unwrap()
                .payload
        })
        .collect::<Vec<_>>();
    let leases = handles_now.map(|(left, right)| {
        (
            core.shared_arrangements.owner_lease_count(left).unwrap(),
            if left == right {
                0
            } else {
                core.shared_arrangements.owner_lease_count(right).unwrap()
            },
        )
    });
    let leases_json = leases
        .map(|(left, right)| format!("[{left},{right}]"))
        .unwrap_or_else(|| "null".to_owned());
    let commits_before_json = owner_commits_before
        .map(|(left, right)| format!("[{left},{right}]"))
        .unwrap_or_else(|| "null".to_owned());
    let commits_after_json = owner_commits_after
        .map(|(left, right)| format!("[{left},{right}]"))
        .unwrap_or_else(|| "null".to_owned());
    let exact_heap_json = report
        .exact_heap_bytes
        .map(|bytes| bytes.to_string())
        .unwrap_or_else(|| "null".to_owned());
    let source_revision_delta = core.source_revisions[&InputId(0)] - source_revision_before;
    let right_revision_delta = core.source_revisions[&InputId(1)] - right_revision_before;
    assert_eq!(source_revision_delta, 1);
    assert_eq!(right_revision_delta, 0);
    if shared {
        assert_eq!(owner_commits_before, Some((1, 1)));
        assert_eq!(owner_commits_after, Some((2, 1)));
    }
    let owner_rows = handles_now.map(|(left, right)| {
        (
            core.shared_arrangements
                .owner_distinct_row_count(left)
                .unwrap(),
            core.shared_arrangements
                .owner_distinct_row_count(right)
                .unwrap(),
        )
    });
    let private_rows = (0..case.views)
        .map(|id| {
            core.views[&ViewId(id as u32)]
                .graph
                .root_private_arrangement_rows_for_test()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let private_rows_json = format!(
        "[{}]",
        private_rows
            .iter()
            .map(|(left, right)| format!("[{left},{right}]"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let private_copy_totals = private_rows.iter().fold((0usize, 0usize), |total, rows| {
        (total.0 + rows.0, total.1 + rows.1)
    });
    let bindings_json = root_bindings_json(&core, case);
    let arena_container_lower_bound = core
        .shared_arrangements
        .container_capacity_lower_bound_for_test()
        .unwrap();
    println!(
        "{{\"case\":\"{}\",\"mode\":\"{}\",\"views\":{},\"seed_rows_per_source\":{},\"left_rows_after_delta\":{},\"right_rows_after_delta\":{},\"payload_width_bytes\":{},\"delta_rows\":{},\"output_rows_per_view\":{:?},\"elapsed_ns\":{},\"owner_count\":{},\"leases\":{},\"source_commit_before\":{},\"source_commit_after\":{},\"source_revision_delta\":{},\"right_source_revision_delta\":{},\"root_view_bindings\":{},\"shared_owner_distinct_rows\":{},\"per_view_private_arrangement_distinct_rows\":{},\"private_arrangement_row_copy_totals\":[{},{}],\"retained_accounted_categories\":{{\"arena_owner_payload_bytes\":{},\"private_arrangement_payload_bytes\":{},\"per_view_output_payload_bytes\":{:?},\"arena_container_capacity_lower_bound_bytes\":{},\"global_input_retention_arrow_capacity_bytes\":{},\"residual_schema_converter_allocator_metadata\":\"unknown categories reported below\",\"frame_staging\":\"transient; not retained after commit; peak not measured\"}},\"memory_report_fields\":{{\"join_logical_payload_bytes\":{},\"logical_payload_bytes\":{},\"known_backing_capacity_bytes\":{},\"estimated_structural_bytes\":{},\"known_backing_capacity_bytes_arrow\":{},\"arrow_visible_buffer_bytes\":{},\"covered_categories\":{:?},\"unknown_categories\":{:?},\"opaque_external_categories\":{:?},\"complete\":{},\"exact_heap_bytes\":{}}}{} }}",
        case.name,
        mode,
        case.views,
        case.rows,
        case.rows + delta_count,
        case.rows,
        case.width,
        delta_count,
        output_rows,
        elapsed_ns,
        core.shared_arrangements.owner_count(),
        leases_json,
        commits_before_json,
        commits_after_json,
        source_revision_delta,
        right_revision_delta,
        bindings_json,
        owner_rows
            .map(|(left, right)| format!("[{left},{right}]"))
            .unwrap_or_else(|| "null".to_owned()),
        private_rows_json,
        private_copy_totals.0,
        private_copy_totals.1,
        arena_owner_payload_bytes,
        private_arrangement_payload_bytes,
        per_view_output_payload_bytes,
        arena_container_lower_bound,
        report.arrow_buffer_capacity_bytes,
        report.join_logical_payload_bytes,
        report.logical_payload_bytes,
        report.known_backing_capacity_bytes,
        report.estimated_structural_bytes,
        report.arrow_buffer_capacity_bytes,
        report.arrow_visible_buffer_bytes,
        report.covered_categories,
        report.unknown_categories,
        report.opaque_external_categories,
        report.complete,
        exact_heap_json,
        allocation_suffix(&allocation_stats)
    );
    drop(private_guard);
}

fn root_bindings_json(core: &EngineCore, case: Case) -> String {
    let entries = (0..case.views)
        .map(|index| {
            let graph = &core.views[&ViewId(index as u32)].graph;
            assert_eq!(graph.root_join_sources_for_test(), Some((InputId(0), InputId(1))));
            let handles = graph.root_shared_handles_for_test().map(|(left, right)| {
                format!("[[{},{}],[{},{}]]", left.slot, left.generation, right.slot, right.generation)
            }).unwrap_or_else(|| "null".to_owned());
            format!("{{\"view\":{index},\"left_input\":0,\"right_input\":1,\"shared_handles\":{handles}}}")
        })
        .collect::<Vec<_>>();
    format!("[{}]", entries.join(","))
}

fn allocation_suffix(stats: &Option<stats_alloc::Stats>) -> String {
    match stats {
        Some(stats) => format!(
            ",\"allocator_allocations\":{},\"allocator_deallocations\":{},\"allocator_reallocations\":{},\"allocator_bytes_allocated\":{},\"allocator_bytes_deallocated\":{},\"allocator_bytes_reallocated\":{}",
            stats.allocations,
            stats.deallocations,
            stats.reallocations,
            stats.bytes_allocated,
            stats.bytes_deallocated,
            stats.bytes_reallocated
        ),
        None => ",\"allocator_observer\":\"disabled\"".to_owned(),
    }
}
