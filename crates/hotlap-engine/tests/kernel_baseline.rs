#[path = "support/baseline_alloc.rs"]
mod baseline_alloc;
#[path = "support/baseline_lifecycle.rs"]
mod baseline_lifecycle;
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
#[ignore = "manual CPU/allocation baseline; run with --ignored --nocapture"]
fn kernel_families_cpu_and_allocation_baseline() {
    baseline_workloads::run();
}
