#[path = "support/baseline_alloc.rs"]
mod baseline_alloc;
#[path = "support/baseline_conversion.rs"]
mod baseline_conversion;
#[path = "support/baseline_lifecycle.rs"]
mod baseline_lifecycle;
#[path = "support/baseline_memory.rs"]
mod baseline_memory;
#[path = "support/baseline_views.rs"]
mod baseline_views;
#[path = "support/baseline_workloads.rs"]
mod baseline_workloads;

#[test]
fn minimum_three_row_fixture_can_retract_a_filtered_out_row() {
    let rows = [(0, 7, 0, 1), (1, 24, 0, 1), (2, 41, 0, 1)];
    let selected = baseline_workloads::filter_retract_index(&rows);
    assert_eq!(selected, 0);
    assert!(rows[selected].1 < 50);
}

#[test]
fn matched_multiview_outputs_equal_independent_view_cores() {
    baseline_views::assert_matched_multiview_correctness(8);
}

#[test]
fn multiview_deltas_and_recovery_match_dense_signed_oracle() {
    baseline_views::assert_dense_multiview_correctness();
}

#[test]
fn signed_arrow_row_conversion_preserves_duplicate_and_negative_weights() {
    baseline_conversion::assert_signed_conversion_fixture();
}

#[test]
fn checkpoint_encoding_reports_nonzero_serialized_payload_size() {
    assert!(baseline_views::serialized_snapshot_size(8) > 0);
}

#[test]
fn memory_observability_baseline_separates_live_categories_and_serialized_size() {
    use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId};
    use hotlap_engine::EngineCore;

    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![AggSpec::count()],
        },
    )
    .unwrap();
    let report = core.memory_report().unwrap();
    assert!(!report.complete);
    assert!(report.known_backing_capacity_bytes > 0);
    assert!(
        report
            .unknown_categories
            .contains(&"core_registries_schemas_and_watermarks".to_string())
    );
    assert!(
        !report
            .opaque_external_categories
            .contains(&"arrow_buffers".to_string())
    );
    assert!(
        !report
            .unknown_categories
            .iter()
            .any(|category| category == "joins")
    );
}

#[test]
#[ignore = "manual CPU/allocation baseline; run with --ignored --nocapture"]
fn kernel_families_cpu_and_allocation_baseline() {
    baseline_workloads::run();
}

#[test]
#[ignore = "manual memory coverage probe; run with --ignored --nocapture"]
fn kernel_memory_coverage_probe() {
    let rows = std::env::var("HOTLAP_BASELINE_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(128);
    let repeats = std::env::var("HOTLAP_BASELINE_REPEATS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);
    baseline_memory::run(rows);
    for rep in 0..repeats {
        baseline_memory::run_family_coverage(rows, rep, repeats);
    }
}
