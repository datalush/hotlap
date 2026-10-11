#[path = "support/baseline_alloc.rs"]
mod baseline_alloc;
#[path = "support/baseline_conversion.rs"]
#[allow(dead_code)]
mod baseline_conversion;
#[path = "support/baseline_lifecycle.rs"]
#[allow(dead_code)]
mod baseline_lifecycle;
#[path = "support/baseline_memory.rs"]
mod baseline_memory;
#[path = "support/baseline_views.rs"]
#[allow(dead_code)]
mod baseline_views;
#[path = "support/baseline_workloads.rs"]
#[allow(dead_code)]
mod baseline_workloads;

#[test]
#[ignore = "manual isolated allocator memory probe; run serially with --ignored --nocapture"]
fn quiescent_state_allocator_proxy() {
    let rows = std::env::var("HOTLAP_BASELINE_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(128);
    baseline_memory::run_isolated(rows);
}
