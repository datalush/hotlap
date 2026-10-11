//! Process live-allocation proxy after materialized outputs are dropped.

use std::time::Instant;

use crate::baseline_alloc::{self, Stats};
use crate::baseline_views::{isolated_cores, materialize_rows, shared_core};
use crate::baseline_workloads::batch;
use crate::baseline_workloads::schema;
use arrow::array::Array;
use hotlap_core::{IncrementalCore, InputId, ViewId, ZSetBatch};

fn schema_fingerprint(batch: &ZSetBatch) -> String {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn run(n: usize) {
    run_isolated(n);
}

pub(super) fn run_isolated(n: usize) {
    let repeats = std::env::var("HOTLAP_BASELINE_REPEATS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);
    for rep in 0..repeats {
        quiescent_live_metric(n, true, rep, repeats);
        quiescent_live_metric(n, false, rep, repeats);
        single_view_checkpoint_metric(n, rep, repeats);
    }
}

fn single_view_checkpoint_metric(n: usize, rep: usize, repeats: usize) {
    let (head, baseline) = baseline_alloc::capture_baseline(|| {
        std::env::var("HOTLAP_BASELINE_HEAD").unwrap_or_else(|_| "unknown".into())
    });
    let fixture_started = Instant::now();
    let input = batch(
        schema(&["k", "v"]),
        vec![
            (0..n).map(|i| (i % 4) as i64).collect(),
            (0..n).map(|i| i as i64).collect(),
        ],
        vec![1; n],
    );
    let fixture_ns = fixture_started.elapsed().as_nanos();
    let setup_started = Instant::now();
    let mut core = isolated_cores().into_iter().next().unwrap();
    let setup_ns = setup_started.elapsed().as_nanos();
    let empty_live = Stats::read().live;
    let empty = core.checkpoint().unwrap();
    let empty_bytes = hotlap_engine::encode_snapshot(&empty).unwrap().len();
    baseline_alloc::reset_peak();
    core.push(InputId(0), &input).unwrap();
    drop(input);
    let output = core.snapshot(ViewId(0)).unwrap();
    let output_rows = output.len();
    let schema_fingerprint = schema_fingerprint(&output);
    drop(output);
    let tap = core.take_changes(ViewId(0)).unwrap();
    let drained = tap.len();
    drop(tap);
    let state = core.checkpoint().unwrap();
    let serialized = hotlap_engine::encode_snapshot(&state).unwrap().len();
    let stats = Stats::read();
    drop(core);
    let live_after_core_drop = Stats::read().live;
    println!(
        "{{\"case\":\"single_view_checkpoint\",\"phase\":\"single_view_checkpoint\",\"topology\":\"single_view_one_core\",\"seed\":7,\"rep\":{rep},\"repeats\":{repeats},\"head\":\"{head}\",\"profile\":\"{profile}\",\"rows\":{n},\"setup_ns\":{setup_ns},\"fixture_setup_ns\":{fixture_ns},\"empty_core_live_bytes\":{empty_live},\"empty_state_serialized_bytes\":{empty_bytes},\"output_rows\":{output_rows},\"schema_fingerprint\":\"{schema_fingerprint}\",\"serialized_snapshot_bytes\":{serialized},\"live_after_outputs_taps_drained_core_state_bytes\":{state_live},\"live_after_core_drop_bytes\":{live_after_core_drop},\"live_delta_from_process_baseline_bytes\":{delta},\"peak_live_since_baseline_bytes\":{peak},\"logical_retained_bytes\":null,\"memory_semantics\":\"process_live_allocator_state_proxy_not_logical_retained_heap\",\"drained_tap_rows\":{drained}}}",
        profile = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        state_live = stats.live,
        delta = stats.live as i128 - baseline as i128,
        peak = stats.peak
    );
}

const REQUIRED_MEMORY_FIELDS: &[&str] = &[
    "setup_ns",
    "fixture_setup_ns",
    "empty_core_live_bytes",
    "live_after_outputs_taps_drained_bytes",
    "live_delta_from_process_baseline_bytes",
    "peak_live_since_process_baseline_bytes",
    "live_after_core_drop_bytes",
    "logical_retained_bytes",
    "drained_tap_rows",
    "schema_fingerprint",
    "output_rows",
];

fn report_phases(repeats: usize) -> Vec<&'static str> {
    let mut phases = Vec::with_capacity(repeats * 3);
    for _ in 0..repeats {
        phases.push("one_core_two_views");
        phases.push("two_cores_one_view_each");
        phases.push("single_view_checkpoint");
    }
    phases
}

fn quiescent_live_metric(n: usize, shared: bool, rep: usize, repeats: usize) {
    let (head, before_live) = baseline_alloc::capture_baseline(|| {
        std::env::var("HOTLAP_BASELINE_HEAD").unwrap_or_else(|_| "unknown".into())
    });
    let process_baseline_live = before_live;
    let setup_started = Instant::now();
    let mut shared_core = if shared { Some(shared_core()) } else { None };
    let mut independent = if shared { None } else { Some(isolated_cores()) };
    let setup_ns = setup_started.elapsed().as_nanos();
    let live_before_push_empty_core = Stats::read().live;
    baseline_alloc::reset_peak();
    let fixture_started = Instant::now();
    let input = batch(
        schema(&["k", "v"]),
        vec![
            (0..n).map(|i| (i % 4) as i64).collect(),
            (0..n).map(|i| i as i64).collect(),
        ],
        vec![1; n],
    );
    let fixture_setup_ns = fixture_started.elapsed().as_nanos();
    let mut tap_rows = 0usize;
    if let Some(core) = &mut shared_core {
        for view in 0..2 {
            tap_rows += core.take_changes(ViewId(view)).unwrap().len();
        }
    }
    if let Some(cores) = &mut independent {
        for core in cores {
            tap_rows += core.take_changes(ViewId(0)).unwrap().len();
        }
    }
    let started = Instant::now();
    if let Some(core) = &mut shared_core {
        core.push(InputId(0), &input).unwrap();
    }
    if let Some(cores) = &mut independent {
        for core in cores {
            core.push(InputId(0), &input).unwrap();
        }
    }
    let elapsed_ns = started.elapsed().as_nanos();
    drop(input);
    let outputs = if let Some(core) = &mut shared_core {
        (0..2)
            .map(|index| core.snapshot(ViewId(index)).unwrap())
            .collect::<Vec<_>>()
    } else {
        independent
            .as_mut()
            .unwrap()
            .iter_mut()
            .map(|core| core.snapshot(ViewId(0)).unwrap())
            .collect()
    };
    let mut output_map = materialize_rows(&outputs[0]);
    for output in outputs.iter().skip(1) {
        output_map.extend(materialize_rows(output));
    }
    let snapshot_array_bytes = outputs
        .iter()
        .map(|output| output.batch.get_array_memory_size() + output.diff.get_array_memory_size())
        .sum::<usize>();
    let snapshot_rows = outputs.iter().map(ZSetBatch::len).sum::<usize>();
    let schema_fingerprint = outputs
        .iter()
        .map(schema_fingerprint)
        .collect::<Vec<_>>()
        .join("|");
    drop(outputs);
    drop(output_map);
    let mut output_rows = snapshot_rows;
    if let Some(core) = &mut shared_core {
        for view in 0..2 {
            let changes = core.take_changes(ViewId(view)).unwrap();
            output_rows += changes.len();
            tap_rows += changes.len();
        }
    }
    if let Some(cores) = &mut independent {
        for core in cores {
            let changes = core.take_changes(ViewId(0)).unwrap();
            output_rows += changes.len();
            tap_rows += changes.len();
        }
    }
    let live_after_drop = Stats::read().live;
    let live_growth = live_after_drop as i128 - before_live as i128;
    let peak_live = Stats::read().peak;
    drop(shared_core);
    drop(independent);
    let live_after_core_drop = Stats::read().live;
    println!(
        "{{\"case\":\"multiview_state_proxy\",\"phase\":\"{}\",\"topology\":\"{}\",\"seed\":7,\"rep\":{rep},\"repeats\":{repeats},\"head\":\"{head}\",\"profile\":\"{}\",\"arch\":\"{}\",\"os\":\"{}\",\"rows\":{n},\"setup_ns\":{setup_ns},\"fixture_setup_ns\":{fixture_setup_ns},\"snapshot_rows\":{snapshot_rows},\"output_rows\":{output_rows},\"schema_fingerprint\":\"{schema_fingerprint}\",\"snapshot_array_bytes_sum_may_double_count_shared_buffers\":{snapshot_array_bytes},\"elapsed_push_ns_two_views\":{elapsed_ns},\"process_baseline_live_bytes\":{process_baseline_live},\"empty_core_baseline_live_bytes\":{live_before_push_empty_core},\"peak_live_since_process_baseline_bytes\":{peak_live},\"live_after_outputs_taps_drained_core_state_bytes\":{live_after_drop},\"live_after_core_drop_bytes\":{live_after_core_drop},\"live_delta_from_process_baseline_bytes\":{live_growth},\"logical_retained_bytes\":null,\"memory_semantics\":\"process_live_allocator_state_proxy_not_logical_retained_heap\",\"drained_tap_rows\":{tap_rows},\"cross_view_arrangement_sharing\":false}}",
        if shared {
            "one_core_two_views"
        } else {
            "two_cores_one_view_each"
        },
        if shared {
            "one_core_two_views"
        } else {
            "two_cores_one_view_each"
        },
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        std::env::consts::ARCH,
        std::env::consts::OS
    );
}

#[cfg(test)]
mod report_contract_tests {
    use super::*;

    #[test]
    fn measured_report_contract_requires_all_topologies_and_quiescent_fields() {
        assert_eq!(
            report_phases(2),
            [
                "one_core_two_views",
                "two_cores_one_view_each",
                "single_view_checkpoint",
                "one_core_two_views",
                "two_cores_one_view_each",
                "single_view_checkpoint"
            ]
        );
        for field in [
            "setup_ns",
            "fixture_setup_ns",
            "empty_core_live_bytes",
            "live_after_outputs_taps_drained_bytes",
            "live_after_core_drop_bytes",
            "logical_retained_bytes",
            "drained_tap_rows",
            "schema_fingerprint",
            "output_rows",
            "peak_live_since_process_baseline_bytes",
        ] {
            assert!(
                REQUIRED_MEMORY_FIELDS.contains(&field),
                "missing report field {field}"
            );
        }
        let one = batch(schema(&["k", "v"]), vec![vec![1], vec![10]], vec![1]);
        assert_eq!(schema_fingerprint(&one), "k,v");
    }
}
