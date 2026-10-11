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
#[ignore = "manual CPU/allocation baseline; run with --ignored --nocapture"]
fn kernel_families_cpu_and_allocation_baseline() {
    baseline_workloads::run();
}
