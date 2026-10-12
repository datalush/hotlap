#[path = "support/baseline_alloc.rs"]
mod baseline_alloc;
#[path = "support/baseline_conversion.rs"]
#[allow(dead_code)]
mod baseline_conversion;
#[path = "support/baseline_lifecycle.rs"]
#[allow(dead_code)]
mod baseline_lifecycle;
#[path = "support/baseline_memory.rs"]
#[allow(dead_code)]
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

#[test]
#[ignore = "manual isolated direct arrangement retention probe; run serially with --ignored --nocapture"]
fn direct_arrangement_fanout_allocator_proxy() {
    let keys = std::env::var("HOTLAP_BASELINE_KEYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(512);
    let fanout = std::env::var("HOTLAP_BASELINE_FANOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let repeats = std::env::var("HOTLAP_BASELINE_REPEATS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    baseline_memory::run_direct_arrangement(keys, fanout, repeats);
}

#[test]
#[ignore = "manual singleton churn timing probe; run serially with --ignored --nocapture"]
fn arrangement_layout_comparison() {
    baseline_memory::run_singleton_churn_exact_reference();
}
