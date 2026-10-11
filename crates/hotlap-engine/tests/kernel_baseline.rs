#[path = "support/baseline_alloc.rs"]
mod baseline_alloc;
#[path = "support/baseline_lifecycle.rs"]
mod baseline_lifecycle;
#[path = "support/baseline_workloads.rs"]
mod baseline_workloads;

#[test]
#[ignore = "manual CPU/allocation baseline; run with --ignored --nocapture"]
fn kernel_families_cpu_and_allocation_baseline() {
    baseline_workloads::run();
}
