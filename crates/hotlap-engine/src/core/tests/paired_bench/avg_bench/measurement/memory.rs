use super::measurement::*;
use super::*;
pub(super) fn process_memory_case(name: &str) {
    let case = case_by_name(name);
    let dense = std::env::var("F7ZV_PROCESS_MODE").as_deref() == Ok("forced_dense");
    let (mut core, setup_ns, left, right) = seed(case, dense);
    let rss_after_seed = resident_pages();
    let (input, delta) = update(case, &left, &right);
    let before = avg_output_signature(&snapshots(&mut core, case));
    let history_before = history_signature(&core);
    let result = timed_push(&mut core, case, input, &delta, dense);
    if !case.name.contains("control") {
        assert_ne!(before, avg_output_signature(&result.snapshots));
    }
    count_contract(&result, case, dense);
    let public_push_wall_ns = result.elapsed_ns;
    let left_rows = left.len();
    let right_rows = right.len();
    let rss_after_push_and_drain = resident_pages();
    println!(
        "{}",
        serde_json::json!({
            "record":"f7zv_process_sample","fixture":case.name,
            "mode":if dense{"forced_dense"}else{"auto"},
            "setup_wall_ns_separate":setup_ns,"public_push_wall_ns":public_push_wall_ns,
            "rss_pages_after_seed":rss_after_seed,"rss_pages_after_push_drain":rss_after_push_and_drain,
            "process_cpu_scope":"whole child process, including setup and assertions; collected by wait4 wrapper",
            "known_engine_capacity_bytes":core.memory_report().unwrap().known_backing_capacity_bytes,
            "path":result.path,"left_rows":left_rows,"right_rows":right_rows,
            "source_shape":actual_counts(case,&left,&right),
            "avg_history_before":history_record(&history_before),
            "avg_history":history_record(&result.history),
            "observer":cfg!(feature="bench-allocation-observer"),
        })
    );
    drop(before);
    drop(history_before);
    drop(delta);
    drop(result);
    drop(left);
    drop(right);
    println!(
        "{}",
        serde_json::json!({
            "record":"f7zv_process_post_drop_rss",
            "fixture":case.name,
            "mode":if dense{"forced_dense"}else{"auto"},
            "rss_pages_after_temporary_copies_dropped":resident_pages(),
            "rss_post_drop_note":"EngineCore retained; materialized view/state retained; process allocator may retain freed pages, so RSS is not live allocation",
        })
    );
}

fn case_by_name(name: &str) -> AvgCase {
    cases()
        .into_iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("unknown f7zv fixture {name}"))
}

fn resident_pages() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}
