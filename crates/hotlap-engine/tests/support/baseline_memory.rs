//! Process live-allocation proxy after materialized outputs are dropped.

use std::time::Instant;

use crate::baseline_alloc::{self, Stats};
use crate::baseline_views::{isolated_cores, materialize_rows, shared_core};
use crate::baseline_workloads::batch;
use crate::baseline_workloads::schema;
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

#[derive(Clone, Debug)]
struct MemoryReport {
    case: &'static str,
    phase: &'static str,
    topology: &'static str,
    rep: usize,
    repeats: usize,
    head: String,
    profile: &'static str,
    rows: usize,
    setup_ns: u128,
    fixture_setup_ns: u128,
    empty_core_live_bytes: usize,
    fixture_live_bytes: usize,
    live_after_outputs_taps_drained_core_state_bytes: usize,
    live_after_core_drop_bytes: usize,
    process_baseline_live_bytes: usize,
    live_delta_from_empty_core_bytes: i128,
    live_delta_from_process_baseline_bytes: i128,
    peak_live_since_process_baseline_bytes: usize,
    output_rows: usize,
    schema_fingerprint: String,
    drained_tap_rows: usize,
    logical_retained_bytes: Option<usize>,
    memory_semantics: &'static str,
}

impl MemoryReport {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "case": self.case, "phase": self.phase, "topology": self.topology,
            "rep": self.rep, "repeats": self.repeats, "head": self.head,
            "profile": self.profile, "rows": self.rows, "setup_ns": self.setup_ns,
            "fixture_setup_ns": self.fixture_setup_ns,
            "process_baseline_live_bytes": self.process_baseline_live_bytes,
            "empty_core_live_bytes": self.empty_core_live_bytes,
            "fixture_live_bytes": self.fixture_live_bytes,
            "live_after_outputs_taps_drained_core_state_bytes": self.live_after_outputs_taps_drained_core_state_bytes,
            "live_after_core_drop_bytes": self.live_after_core_drop_bytes,
            "live_delta_from_empty_core_bytes": self.live_delta_from_empty_core_bytes,
            "live_delta_from_process_baseline_bytes": self.live_delta_from_process_baseline_bytes,
            "peak_live_since_process_baseline_bytes": self.peak_live_since_process_baseline_bytes,
            "output_rows": self.output_rows, "schema_fingerprint": self.schema_fingerprint,
            "drained_tap_rows": self.drained_tap_rows,
            "logical_retained_bytes": self.logical_retained_bytes,
            "memory_semantics": self.memory_semantics,
        })
    }
}

fn validate_emitted_report(value: &serde_json::Value) -> Result<(), String> {
    let required = [
        "case",
        "phase",
        "topology",
        "rep",
        "repeats",
        "head",
        "profile",
        "rows",
        "setup_ns",
        "fixture_setup_ns",
        "process_baseline_live_bytes",
        "empty_core_live_bytes",
        "fixture_live_bytes",
        "live_after_outputs_taps_drained_core_state_bytes",
        "live_after_core_drop_bytes",
        "live_delta_from_empty_core_bytes",
        "live_delta_from_process_baseline_bytes",
        "peak_live_since_process_baseline_bytes",
        "output_rows",
        "schema_fingerprint",
        "drained_tap_rows",
        "logical_retained_bytes",
        "memory_semantics",
    ];
    for key in required {
        if !value.get(key).is_some() {
            return Err(format!("missing {key}"));
        }
    }
    let phase = value["phase"].as_str().ok_or("phase is not a string")?;
    let topology = value["topology"]
        .as_str()
        .ok_or("topology is not a string")?;
    if phase != topology
        || ![
            "one_core_two_views",
            "two_cores_one_view_each",
            "single_view_checkpoint",
        ]
        .contains(&phase)
    {
        return Err("unknown/mismatched phase and topology".into());
    }
    let schema = value["schema_fingerprint"]
        .as_str()
        .ok_or("schema is not a string")?;
    let expected_schema = match phase {
        "one_core_two_views" => "k,count|k,count,sum",
        "two_cores_one_view_each" => "k,count|k,count,sum",
        "single_view_checkpoint" => "k,count",
        _ => unreachable!(),
    };
    if schema != expected_schema {
        return Err("unexpected schema fingerprint".into());
    }
    if value["memory_semantics"] != "process_live_allocator_state_proxy_not_logical_retained_heap"
        || !value["logical_retained_bytes"].is_null()
    {
        return Err("memory semantics must not claim logical retained bytes".into());
    }
    if value["live_after_core_drop_bytes"].as_i64().is_none()
        || value["process_baseline_live_bytes"].as_u64().is_none()
        || value["empty_core_live_bytes"].as_u64().is_none()
        || value["fixture_live_bytes"].as_u64().is_none()
    {
        return Err("missing lifecycle allocator sample".into());
    }
    if value["output_rows"].as_u64().is_none() || value["drained_tap_rows"].as_u64().is_none() {
        return Err("missing output/tap row count".into());
    }
    Ok(())
}

fn emit_validated(report: MemoryReport) {
    let line = serde_json::to_string(&report.to_json()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
    validate_emitted_report(&parsed).expect("invalid memory probe report");
    println!("{line}");
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
    let (fixture_ns, fixture_live, input) = {
        let started = Instant::now();
        let input = batch(
            schema(&["k", "v"]),
            vec![
                (0..n).map(|i| (i % 4) as i64).collect(),
                (0..n).map(|i| i as i64).collect(),
            ],
            vec![1; n],
        );
        let elapsed = started.elapsed().as_nanos();
        let live = Stats::read().live;
        (elapsed, live, input)
    };
    let setup_core_started = Instant::now();
    let mut core = isolated_cores().into_iter().next().unwrap();
    let setup_ns = setup_core_started.elapsed().as_nanos();
    let empty_core_live = Stats::read().live;
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
    let after_drain_live = Stats::read().live;
    drop(core);
    let live_after_core_drop = Stats::read().live;
    let peak_live = Stats::read().peak;
    let process_baseline_live_bytes = baseline;
    let live_delta_from_process_baseline_bytes = live_after_core_drop as i128 - baseline as i128;
    let live_delta_from_empty_core_bytes = live_after_core_drop as i128 - empty_core_live as i128;
    emit_validated(MemoryReport {
        case: "multiview_state_proxy",
        phase: "single_view_checkpoint",
        topology: "single_view_checkpoint",
        rep,
        repeats,
        head,
        profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        rows: n,
        setup_ns,
        fixture_setup_ns: fixture_ns,
        empty_core_live_bytes: empty_core_live,
        fixture_live_bytes: fixture_live,
        live_after_outputs_taps_drained_core_state_bytes: after_drain_live,
        live_after_core_drop_bytes: live_after_core_drop,
        process_baseline_live_bytes,
        live_delta_from_empty_core_bytes,
        live_delta_from_process_baseline_bytes,
        peak_live_since_process_baseline_bytes: peak_live,
        output_rows,
        schema_fingerprint,
        drained_tap_rows: drained,
        logical_retained_bytes: None,
        memory_semantics: "process_live_allocator_state_proxy_not_logical_retained_heap",
    });
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
    let fixture_live = Stats::read().live;
    baseline_alloc::reset_peak();
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
    let snapshot_rows = outputs.iter().map(ZSetBatch::len).sum::<usize>();
    let schema_fingerprint = outputs
        .iter()
        .map(schema_fingerprint)
        .collect::<Vec<_>>()
        .join("|");
    drop(outputs);
    drop(output_map);
    let after_snapshot_drop_live = Stats::read().live;
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
    let after_tap_drain_live = Stats::read().live;
    drop(shared_core);
    drop(independent);
    let live_after_core_drop = Stats::read().live;
    let peak_live = Stats::read().peak;
    emit_validated(MemoryReport {
        case: "multiview_state_proxy",
        phase: if shared {
            "one_core_two_views"
        } else {
            "two_cores_one_view_each"
        },
        topology: if shared {
            "one_core_two_views"
        } else {
            "two_cores_one_view_each"
        },
        rep,
        repeats,
        head,
        profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        rows: n,
        setup_ns,
        fixture_setup_ns,
        empty_core_live_bytes: live_before_push_empty_core,
        fixture_live_bytes: fixture_live,
        live_after_outputs_taps_drained_core_state_bytes: after_tap_drain_live,
        live_after_core_drop_bytes: live_after_core_drop,
        process_baseline_live_bytes: process_baseline_live,
        live_delta_from_empty_core_bytes: live_after_core_drop as i128
            - live_before_push_empty_core as i128,
        live_delta_from_process_baseline_bytes: live_after_core_drop as i128
            - process_baseline_live as i128,
        peak_live_since_process_baseline_bytes: peak_live,
        output_rows,
        schema_fingerprint,
        drained_tap_rows: tap_rows,
        logical_retained_bytes: None,
        memory_semantics: "process_live_allocator_state_proxy_not_logical_retained_heap",
    });
    let _ = (
        snapshot_rows,
        elapsed_ns,
        fixture_live,
        after_snapshot_drop_live,
    );
}

#[cfg(test)]
mod report_contract_tests {
    use super::*;

    #[test]
    fn emitted_json_validator_checks_phases_schema_and_cleanup_metrics() {
        let valid = MemoryReport {
            case: "multiview_state_proxy",
            phase: "one_core_two_views",
            topology: "one_core_two_views",
            rep: 0,
            repeats: 3,
            head: "abc".into(),
            profile: "release",
            rows: 128,
            setup_ns: 1,
            fixture_setup_ns: 2,
            process_baseline_live_bytes: 5,
            empty_core_live_bytes: 10,
            fixture_live_bytes: 12,
            live_after_outputs_taps_drained_core_state_bytes: 20,
            live_after_core_drop_bytes: 10,
            live_delta_from_empty_core_bytes: 0,
            live_delta_from_process_baseline_bytes: 5,
            peak_live_since_process_baseline_bytes: 22,
            output_rows: 16,
            schema_fingerprint: "k,count|k,count,sum".into(),
            drained_tap_rows: 8,
            logical_retained_bytes: None,
            memory_semantics: "process_live_allocator_state_proxy_not_logical_retained_heap",
        };
        let valid = valid.to_json();
        assert!(validate_emitted_report(&valid).is_ok());
        let mut missing_phase = valid.clone();
        missing_phase.as_object_mut().unwrap().remove("phase");
        assert!(validate_emitted_report(&missing_phase).is_err());
        let mut wrong_schema = valid.clone();
        wrong_schema["schema_fingerprint"] = serde_json::json!("wrong");
        assert!(validate_emitted_report(&wrong_schema).is_err());
        let mut missing_metric = valid;
        missing_metric
            .as_object_mut()
            .unwrap()
            .remove("live_after_outputs_taps_drained_core_state_bytes");
        assert!(validate_emitted_report(&missing_metric).is_err());
        assert!(serde_json::from_str::<serde_json::Value>("{\"phase\":").is_err());
        let phases = [
            ("one_core_two_views", "k,count|k,count,sum"),
            ("two_cores_one_view_each", "k,count|k,count,sum"),
            ("single_view_checkpoint", "k,count"),
        ];
        for (phase, schema) in phases {
            let mut report = serde_json::json!({
                "case":"multiview_state_proxy", "phase":phase, "topology":phase,
                "rep":0, "repeats":1, "head":"test", "profile":"release", "rows":1,
                "setup_ns":0, "fixture_setup_ns":0, "process_baseline_live_bytes":1,
                "empty_core_live_bytes":1, "fixture_live_bytes":1,
                "live_after_outputs_taps_drained_core_state_bytes":1, "live_after_core_drop_bytes":1,
                "live_delta_from_empty_core_bytes":0, "live_delta_from_process_baseline_bytes":0,
                "peak_live_since_process_baseline_bytes":1, "output_rows":1, "schema_fingerprint":schema,
                "drained_tap_rows":1, "logical_retained_bytes":null,
                "memory_semantics":"process_live_allocator_state_proxy_not_logical_retained_heap"
            });
            assert!(validate_emitted_report(&report).is_ok(), "phase {phase}");
            report["topology"] = serde_json::json!("wrong");
            assert!(validate_emitted_report(&report).is_err());
        }
    }
}
