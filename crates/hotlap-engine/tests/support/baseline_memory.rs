//! Process live-allocation proxy after materialized outputs are dropped.

use std::time::Instant;

use crate::baseline_alloc::{self, Stats};
use crate::baseline_views::{isolated_cores, materialize_rows, shared_core};
use crate::baseline_workloads::{batch, rep, repeats, schema};
use arrow::array::Array;
use hotlap_core::{IncrementalCore, InputId, ViewId, ZSetBatch};

pub(super) fn run(n: usize) {
    quiescent_live_metric(n, true);
    quiescent_live_metric(n, false);
}

fn quiescent_live_metric(n: usize, shared: bool) {
    let input = batch(
        schema(&["k", "v"]),
        vec![
            (0..n).map(|i| (i % 4) as i64).collect(),
            (0..n).map(|i| i as i64).collect(),
        ],
        vec![1; n],
    );
    let mut shared_core = if shared { Some(shared_core()) } else { None };
    let mut independent = if shared { None } else { Some(isolated_cores()) };
    let (head, before_live) = baseline_alloc::capture_baseline(|| {
        std::env::var("HOTLAP_BASELINE_HEAD").unwrap_or_else(|_| "unknown".into())
    });
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
    drop(outputs);
    drop(output_map);
    let live_after_drop = Stats::read().live;
    let peak_live = Stats::read().peak;
    println!(
        "{{\"case\":\"multiview_state_proxy\",\"phase\":\"{}\",\"seed\":7,\"rep\":{},\"repeats\":{},\"head\":\"{head}\",\"profile\":\"{}\",\"arch\":\"{}\",\"os\":\"{}\",\"rows\":{n},\"snapshot_rows\":{snapshot_rows},\"snapshot_array_bytes\":{snapshot_array_bytes},\"elapsed_push_ns_two_views\":{elapsed_ns},\"allocated_live_before_bytes\":{before_live},\"peak_allocated_live_bytes\":{peak_live},\"allocated_live_after_outputs_dropped_bytes\":{live_after_drop},\"allocated_live_delta_bytes\":{},\"logical_retained_bytes\":null,\"memory_semantics\":\"process_live_allocation_proxy_not_kernel_retained\",\"cross_view_arrangement_sharing\":false}}",
        if shared {
            "two_views_one_core"
        } else {
            "two_views_two_cores"
        },
        rep(),
        repeats(),
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        std::env::consts::ARCH,
        std::env::consts::OS,
        live_after_drop as i128 - before_live as i128
    );
    drop(shared_core);
    drop(independent);
}
