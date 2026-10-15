//! Correctness contracts and a one-sample-at-a-time paired measurement entry.

mod fixtures;

use std::time::Instant;
use std::{fs, io};

use hotlap_core::{IncrementalCore, InputId, ViewId};
use serde::Serialize;

use super::EngineCore;
use fixtures::{CASES, CaseSpec, EngineMode, case_spec, generated_inputs};

fn emit(record: impl Serialize) {
    println!("{}", serde_json::to_string(&record).unwrap());
}

fn current_rss_kib() -> io::Result<(usize, usize)> {
    let status = fs::read_to_string("/proc/self/status")?;
    parse_proc_status_memory_kib(&status)
}

fn parse_proc_status_memory_kib(status: &str) -> io::Result<(usize, usize)> {
    fn value(status: &str, key: &str) -> io::Result<usize> {
        let line = status
            .lines()
            .find(|line| line.starts_with(key))
            .ok_or_else(|| io::Error::other(format!("{key} missing from proc status")))?;
        line.split_whitespace()
            .nth(1)
            .ok_or_else(|| io::Error::other(format!("{key} value missing")))?
            .parse()
            .map_err(io::Error::other)
    }
    Ok((value(status, "VmRSS:")?, value(status, "VmHWM:")?))
}

#[test]
fn proc_status_memory_parser_requires_rss_and_hwm() {
    assert_eq!(
        parse_proc_status_memory_kib("VmRSS:\t12 kB\nVmHWM:\t34 kB\n").unwrap(),
        (12, 34)
    );
    assert!(parse_proc_status_memory_kib("VmRSS:\t12 kB\n").is_err());
    assert!(parse_proc_status_memory_kib("VmHWM:\t34 kB\n").is_err());
}

#[test]
fn measurement_contracts_match_candidate_and_eager_reference() {
    for case in CASES {
        let spec = case_spec(case);
        let eager = run_contract(&spec, EngineMode::ForceEager);
        match eager {
            ContractRun::Accepted(reference) => {
                assert_eq!(reference.actual_kernel_entries, 0, "{}", case.name);
                let candidate = run_contract(&spec, EngineMode::Candidate);
                let ContractRun::Accepted(candidate) = candidate else {
                    panic!(
                        "candidate rejected source accepted by reference: {}",
                        case.name
                    );
                };
                assert_eq!(candidate.pushes, reference.pushes, "{}", case.name);
                assert_eq!(candidate.checkpoint, reference.checkpoint, "{}", case.name);
                if case.expected_fused {
                    assert!(candidate.actual_kernel_entries > 0, "{}", case.name);
                } else {
                    assert_eq!(candidate.actual_kernel_entries, 0, "{}", case.name);
                }
                emit(serde_json::json!({
                    "record": "measurement_contract",
                    "case": case.name,
                    "source_accepted": true,
                    "candidate_actual_kernel_entries": candidate.actual_kernel_entries,
                    "reference_actual_kernel_entries": reference.actual_kernel_entries,
                    "seed_rows": case.rows,
                    "change_rows": case.rows,
                    "source_columns": case.shape.source_columns(),
                    "source_payload_bytes_seed": spec.source_payload_bytes,
                    "view_changes": candidate.pushes.iter().map(|push| push.changelog_rows > 0).collect::<Vec<_>>(),
                    "group_float_push_count": candidate.pushes.iter().filter(|push| !push.group_float_bits.is_empty()).count(),
                    "live_restore_continuation_equal": candidate.pushes.last() == candidate.pushes.get(candidate.pushes.len() - 2),
                }));
            }
            ContractRun::Rejected(reason) => {
                panic!("intended source case {} rejected: {reason}", case.name)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ContractResult {
    pushes: Vec<PushContract>,
    checkpoint: Vec<u8>,
    actual_kernel_entries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PushContract {
    changelog: Vec<u8>,
    snapshot: Vec<u8>,
    changelog_rows: usize,
    snapshot_rows: usize,
    group_float_bits: Vec<(u64, u64)>,
    kernel_delta: u64,
}

enum ContractRun {
    Accepted(ContractResult),
    Rejected(String),
}

fn run_contract(fixture: &CaseSpec, mode: EngineMode) -> ContractRun {
    let _mode = mode.guard();
    crate::ops::filter_project::reset_fused_filter_project_kernel_count();
    let mut engine = EngineCore::new();
    if let Err(error) = engine.register_input(InputId(0)) {
        return ContractRun::Rejected(error.to_string());
    }
    if let Err(error) = engine.build_view(ViewId(0), &fixture.plan) {
        return ContractRun::Rejected(error.to_string());
    }
    if let Err(error) = engine.tap_view(ViewId(0)) {
        return ContractRun::Rejected(error.to_string());
    }

    let mut pushes = Vec::new();
    let expected_entry = mode == EngineMode::Candidate && fixture.case.expected_fused;
    let inputs = generated_inputs(fixture);
    for input in [&inputs.seed, &inputs.change] {
        let kernel_before = crate::ops::filter_project::fused_filter_project_kernel_count();
        if let Err(error) = push_checked(&mut engine, input, expected_entry) {
            return ContractRun::Rejected(error.to_string());
        }
        match capture_push(fixture, &mut engine) {
            Ok(push) => pushes.push(push),
            Err(error) => return ContractRun::Rejected(error),
        }
        let actual_delta =
            crate::ops::filter_project::fused_filter_project_kernel_count() - kernel_before;
        assert_eq!(actual_delta, if expected_entry { 1 } else { 0 });
        if expected_entry {
            assert_eq!(actual_delta, 1);
        }
        if !input.is_empty() && fixture.case.selection != fixtures::Selection::AllFalse {
            assert!(
                pushes.last().unwrap().changelog_rows > 0,
                "selected input yielded empty changelog"
            );
        }
    }
    let before_restore = match engine.checkpoint() {
        Ok(checkpoint) => checkpoint,
        Err(error) => return ContractRun::Rejected(error.to_string()),
    };
    let mut restored = EngineCore::new();
    if let Err(error) = restored.restore(&before_restore) {
        return ContractRun::Rejected(error.to_string());
    }
    if let Err(error) = push_checked(&mut engine, &inputs.continuation, expected_entry) {
        return ContractRun::Rejected(error.to_string());
    }
    if let Err(error) = push_checked(&mut restored, &inputs.continuation, expected_entry) {
        return ContractRun::Rejected(error.to_string());
    }
    let live_continuation = match capture_push(fixture, &mut engine) {
        Ok(push) => push,
        Err(error) => return ContractRun::Rejected(error),
    };
    let restored_continuation = match capture_push(fixture, &mut restored) {
        Ok(push) => push,
        Err(error) => return ContractRun::Rejected(error),
    };
    if live_continuation != restored_continuation {
        return ContractRun::Rejected("live and restored continuation differ".into());
    }
    assert!(
        live_continuation.changelog_rows > 0,
        "{} continuation must change the view",
        fixture.case.name
    );
    pushes.push(live_continuation);
    pushes.push(restored_continuation);
    let checkpoint = match engine.checkpoint() {
        Ok(checkpoint) => fixture.encode_checkpoint(&checkpoint),
        Err(error) => return ContractRun::Rejected(error.to_string()),
    };
    if checkpoint
        != match restored.checkpoint() {
            Ok(checkpoint) => fixture.encode_checkpoint(&checkpoint),
            Err(error) => return ContractRun::Rejected(error.to_string()),
        }
    {
        return ContractRun::Rejected("live and restored final checkpoints differ".into());
    }
    if matches!(fixture.case.shape, fixtures::Shape::GroupFloat) {
        let histories = pushes
            .iter()
            .take(3)
            .map(|push| push.group_float_bits[0])
            .collect::<Vec<_>>();
        for (sum, avg) in &histories {
            assert_eq!(sum, avg, "SUM/AVG float history bits differ: {histories:?}");
        }
        assert_eq!(
            histories.iter().map(|(sum, _)| *sum).collect::<Vec<_>>(),
            [
                0.0_f64.to_bits(),
                0.0_f64.to_bits(),
                (9_007_199_254_740_992.0_f64).to_bits()
            ],
            "ordered M,1,-M rows must retain the expected float history"
        );
    }
    ContractRun::Accepted(ContractResult {
        pushes,
        checkpoint,
        actual_kernel_entries: crate::ops::filter_project::fused_filter_project_kernel_count(),
    })
}

fn push_checked(
    engine: &mut EngineCore,
    input: &hotlap_core::ZSetBatch,
    expect_kernel_entry: bool,
) -> Result<(), String> {
    let before = crate::ops::filter_project::fused_filter_project_kernel_count();
    engine
        .push(InputId(0), input)
        .map_err(|error| error.to_string())?;
    let after = crate::ops::filter_project::fused_filter_project_kernel_count();
    if expect_kernel_entry {
        assert_eq!(
            after,
            before + 1,
            "candidate did not enter fused kernel exactly once"
        );
    } else {
        assert_eq!(after, before, "eager/fallback path entered fused kernel");
    }
    Ok(())
}

fn capture_push(fixture: &CaseSpec, engine: &mut EngineCore) -> Result<PushContract, String> {
    let before = crate::ops::filter_project::fused_filter_project_kernel_count();
    let changelog = engine
        .take_changes(ViewId(0))
        .map_err(|error| error.to_string())?;
    let snapshot = engine
        .snapshot(ViewId(0))
        .map_err(|error| error.to_string())?;
    let checkpoint = engine.checkpoint().map_err(|error| error.to_string())?;
    let after = crate::ops::filter_project::fused_filter_project_kernel_count();
    Ok(PushContract {
        changelog: fixture.encode_zset(&changelog),
        snapshot: fixture.encode_zset(&snapshot),
        changelog_rows: changelog.len(),
        snapshot_rows: snapshot.len(),
        group_float_bits: fixture.group_float_bits(&checkpoint),
        kernel_delta: after - before,
    })
}

#[test]
#[ignore = "single-case paired measurement; run after independent harness review"]
fn filter_project_measurement_sample() {
    let name = std::env::var("FILTER_PROJECT_CASE").expect("FILTER_PROJECT_CASE is required");
    let mode = EngineMode::from_env();
    let case = CASES
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("unknown FILTER_PROJECT_CASE={name}"));
    let spec = case_spec(*case);
    measurement_sample(spec, mode);
}

fn measurement_sample(fixture: CaseSpec, mode: EngineMode) {
    let case = fixture.case;
    let plan = fixture.plan.clone();
    let _mode = mode.guard();
    crate::ops::filter_project::reset_fused_filter_project_kernel_count();
    let mut engine = EngineCore::new();
    engine.register_input(InputId(0)).unwrap();
    engine.build_view(ViewId(0), &plan).unwrap();
    engine.tap_view(ViewId(0)).unwrap();
    let inputs = generated_inputs(&fixture);
    let seed = inputs.seed;
    let seed_selected_rows = {
        let predicate = hotlap_core::Predicate::Cmp {
            op: hotlap_core::CmpOp::Eq,
            col: fixture.predicate_column,
            scalar: hotlap_core::Scalar::I64(1),
        };
        predicate
            .eval(&seed.batch)
            .unwrap()
            .iter()
            .filter(|value| *value == Some(true))
            .count()
    };
    engine.push(InputId(0), &seed).unwrap();
    let seed_changelog = engine.take_changes(ViewId(0)).unwrap();
    drop(seed_changelog);
    let seed_kernel_entries = crate::ops::filter_project::fused_filter_project_kernel_count();
    match mode {
        EngineMode::Candidate if case.expected_fused => assert!(seed_kernel_entries > 0),
        _ => assert_eq!(seed_kernel_entries, 0),
    }
    crate::ops::filter_project::reset_fused_filter_project_kernel_count();
    let seed_rows = seed.len();
    let change = inputs.change;
    let rows_change = change.len();
    let change_selected_rows = {
        let predicate = hotlap_core::Predicate::Cmp {
            op: hotlap_core::CmpOp::Eq,
            col: fixture.predicate_column,
            scalar: hotlap_core::Scalar::I64(1),
        };
        predicate
            .eval(&change.batch)
            .unwrap()
            .iter()
            .filter(|value| *value == Some(true))
            .count()
    };
    let change_diffs = change
        .diff
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let (negative_diffs, zero_diffs, positive_diffs) = change_diffs.values().iter().fold(
        (0_usize, 0_usize, 0_usize),
        |(negative, zero, positive), diff| match diff.cmp(&0) {
            std::cmp::Ordering::Less => (negative + 1, zero, positive),
            std::cmp::Ordering::Equal => (negative, zero + 1, positive),
            std::cmp::Ordering::Greater => (negative, zero, positive + 1),
        },
    );
    let actual_kernel_before = crate::ops::filter_project::fused_filter_project_kernel_count();
    let elapsed_ns = timed_public_push(&mut engine, InputId(0), &change);
    let change_changelog = engine.take_changes(ViewId(0)).unwrap();
    let changelog_rows = change_changelog.len();
    let view_noop = change_changelog.is_empty();
    drop(change_changelog);
    drop(change);
    let actual_kernel_after = crate::ops::filter_project::fused_filter_project_kernel_count();
    assert_eq!(actual_kernel_before, 0);
    match mode {
        EngineMode::Candidate if case.expected_fused => assert!(actual_kernel_after > 0),
        EngineMode::Candidate => assert_eq!(actual_kernel_after, 0),
        EngineMode::ForceEager => assert_eq!(actual_kernel_after, 0),
    }
    let continuation = inputs.continuation;
    engine.push(InputId(0), &continuation).unwrap();
    let continuation_kernel_entries =
        crate::ops::filter_project::fused_filter_project_kernel_count();
    match mode {
        EngineMode::Candidate if case.expected_fused => {
            assert_eq!(continuation_kernel_entries, actual_kernel_after + 1)
        }
        _ => assert_eq!(continuation_kernel_entries, actual_kernel_after),
    }
    let continuation_changelog = engine.take_changes(ViewId(0)).unwrap();
    let continuation_changelog_rows = continuation_changelog.len();
    assert!(
        continuation_changelog_rows > 0,
        "{} continuation must change the view",
        case.name
    );
    drop(continuation_changelog);
    drop(continuation);
    drop(seed);
    let final_checkpoint = engine.checkpoint().unwrap();
    let float_group_push_history_bits = if matches!(case.shape, fixtures::Shape::GroupFloat) {
        reference_group_float_history(&fixture)
    } else {
        Vec::new()
    };
    emit(serde_json::json!({
        "record": "filter_project_single_sample",
        "case": case.name,
        "mode": mode.as_str(),
        "rows_seed": seed_rows,
        "source_input_rows_seed": seed_rows,
        "rows_change": rows_change,
        "source_columns": case.shape.source_columns(),
        "selected_rows_seed": seed_selected_rows,
        "selected_rows_change": change_selected_rows,
        "change_diff_negative_rows": negative_diffs,
        "change_diff_zero_rows": zero_diffs,
        "change_diff_positive_rows": positive_diffs,
        "actual_fused_kernel_entries_seed": seed_kernel_entries,
        "source_payload_bytes": case.rows * case.payload_bytes,
        "elapsed_push_ns": elapsed_ns,
        "actual_fused_kernel_entries_before": actual_kernel_before,
        "actual_fused_kernel_entries_after": actual_kernel_after,
        "actual_fused_kernel_entries_after_continuation": continuation_kernel_entries,
        "actual_fused_kernel_entries_continuation_before": actual_kernel_after,
        "changelog_rows": changelog_rows,
        "snapshot_rows": 0,
        "view_noop": view_noop,
        "continuation_changelog_rows": continuation_changelog_rows,
        "continuation_snapshot_rows": 0,
        "checkpoint_bytes": fixture.encode_checkpoint(&final_checkpoint).len(),
        "float_group_history_bits": fixture.group_float_bits(&final_checkpoint),
        "float_group_reference_sequence_bits": float_group_push_history_bits
            .iter()
            .map(|history| history.first().map(|(sum, _)| *sum).unwrap_or_default())
            .collect::<Vec<_>>(),
        "float_group_retained_operand_bits": fixture.group_float_contract,
        "float_group_push_history_bits": float_group_push_history_bits,
        "allocation_observer_feature": false,
    }));
}

fn reference_group_float_history(fixture: &CaseSpec) -> Vec<Vec<(u64, u64)>> {
    let _mode = EngineMode::ForceEager.guard();
    let inputs = generated_inputs(fixture);
    let mut reference = EngineCore::new();
    reference.register_input(InputId(0)).unwrap();
    reference.build_view(ViewId(0), &fixture.plan).unwrap();
    reference.tap_view(ViewId(0)).unwrap();
    let mut histories = Vec::new();
    for input in [&inputs.seed, &inputs.change, &inputs.continuation] {
        reference.push(InputId(0), input).unwrap();
        drop(reference.take_changes(ViewId(0)).unwrap());
        histories.push(fixture.group_float_bits(&reference.checkpoint().unwrap()));
    }
    histories
}

fn timed_public_push(
    engine: &mut EngineCore,
    input_id: InputId,
    input: &hotlap_core::ZSetBatch,
) -> u128 {
    let started = Instant::now();
    engine.push(input_id, input).unwrap();
    started.elapsed().as_nanos()
}

#[test]
#[ignore = "separate fresh-process RSS phases; not part of timed push sample"]
fn filter_project_memory_sample() {
    let name = std::env::var("FILTER_PROJECT_CASE").expect("FILTER_PROJECT_CASE is required");
    let mode = EngineMode::from_env();
    let case = CASES.iter().find(|case| case.name == name).unwrap();
    let fixture = case_spec(*case);
    let inputs = generated_inputs(&fixture);
    let case = fixture.case;
    let plan = fixture.plan.clone();
    let predicate_column = fixture.predicate_column;
    let source_payload_bytes = fixture.source_payload_bytes;
    drop(fixture);
    let _mode = mode.guard();
    let mut engine = EngineCore::new();
    engine.register_input(InputId(0)).unwrap();
    engine.build_view(ViewId(0), &plan).unwrap();
    engine.tap_view(ViewId(0)).unwrap();
    let seed = inputs.seed;
    let source_input_rows_seed = seed.len();
    let source_columns = seed.batch.num_columns();
    let source_schema_seed = seed
        .batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            serde_json::json!({
                "name": field.name(),
                "data_type": format!("{:?}", field.data_type())
            })
        })
        .collect::<Vec<_>>();
    let selected_rows_seed = hotlap_core::Predicate::Cmp {
        op: hotlap_core::CmpOp::Eq,
        col: predicate_column,
        scalar: hotlap_core::Scalar::I64(1),
    }
    .eval(&seed.batch)
    .unwrap()
    .iter()
    .filter(|value| *value == Some(true))
    .count();
    engine.push(InputId(0), &seed).unwrap();
    drop(engine.take_changes(ViewId(0)).unwrap());
    let (rss_after_seed, hwm_after_seed) = current_rss_kib().unwrap();
    drop(seed);
    let before_memory = engine.memory_report().unwrap();
    let change = inputs.change;
    let source_input_rows_change = change.len();
    let selected_rows_change = hotlap_core::Predicate::Cmp {
        op: hotlap_core::CmpOp::Eq,
        col: predicate_column,
        scalar: hotlap_core::Scalar::I64(1),
    }
    .eval(&change.batch)
    .unwrap()
    .iter()
    .filter(|value| *value == Some(true))
    .count();
    engine.push(InputId(0), &change).unwrap();
    drop(engine.take_changes(ViewId(0)).unwrap());
    let (rss_after_push_and_drain, hwm_after_push_and_drain) = current_rss_kib().unwrap();
    let after_memory = engine.memory_report().unwrap();
    drop(change);
    drop(inputs.continuation);
    let (rss_after_input_drop, hwm_after_input_drop) = current_rss_kib().unwrap();
    emit(serde_json::json!({
        "record": "filter_project_memory_sample",
        "case": case.name,
        "mode": mode.as_str(),
        "source_input_rows_seed": source_input_rows_seed,
        "source_input_rows_change": source_input_rows_change,
        "source_columns": source_columns,
        "source_payload_bytes_seed": source_payload_bytes,
        "source_payload_bytes_change": source_payload_bytes,
        "selected_rows_seed": selected_rows_seed,
        "selected_rows_change": selected_rows_change,
        "source_schema_seed": source_schema_seed,
        "process_rss_kib_after_seed_drain": rss_after_seed,
        "process_rss_kib_after_measured_push_and_drain_engine_alive": rss_after_push_and_drain,
        "process_rss_kib_after_dropping_raw_inputs_engine_alive": rss_after_input_drop,
        "post_exec_observed_peak_rss_kib": hwm_after_input_drop,
        "post_exec_observed_peak_rss_scope": "VmHWM sampled after input owners are dropped with engine alive; process exec through seed, push, drain and input construction, excluding pre-exec launcher and final JSON reporting/teardown",
        "post_exec_observed_peak_rss_kib_after_seed_drain": hwm_after_seed,
        "post_exec_observed_peak_rss_kib_after_measured_push_and_drain": hwm_after_push_and_drain,
        "engine_reported_before": before_memory,
        "engine_reported_after": after_memory,
        "engine_state_retained": true,
        "observer_feature": false
    }));
}

#[cfg(feature = "bench-allocation-observer")]
#[test]
#[ignore = "isolated operator allocation sample; not an EngineCore timing result"]
fn filter_project_operator_allocation_sample() {
    use arrow::array::Array;
    use hotlap_core::Predicate;
    use stats_alloc::{INSTRUMENTED_SYSTEM, Region};
    use std::sync::Arc;

    let name = std::env::var("FILTER_PROJECT_CASE").expect("FILTER_PROJECT_CASE is required");
    let case = CASES
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("unknown FILTER_PROJECT_CASE={name}"));
    let fixture = case_spec(*case);
    let inputs = generated_inputs(&fixture);
    let predicate = Predicate::Cmp {
        op: hotlap_core::CmpOp::Eq,
        col: fixture.predicate_column,
        scalar: hotlap_core::Scalar::I64(1),
    };
    let mask = predicate.eval(&inputs.seed.batch).unwrap();
    let cols = fixture.projected_columns();

    let loops = 16;
    let old_region = Region::new(&INSTRUMENTED_SYSTEM);
    let mut old_output = Vec::with_capacity(loops);
    for _ in 0..loops {
        let filtered = crate::ops::filter_project::filter(&inputs.seed, &mask).unwrap();
        old_output.push(crate::ops::filter_project::project(&filtered, &cols).unwrap());
    }
    let old_stats = old_region.change();

    let fused_region = Region::new(&INSTRUMENTED_SYSTEM);
    let mut fused_output = Vec::with_capacity(loops);
    for _ in 0..loops {
        fused_output
            .push(crate::ops::filter_project::filter_project(&inputs.seed, &mask, &cols).unwrap());
    }
    let fused_stats = fused_region.change();

    assert_eq!(old_output[0].schema(), fused_output[0].schema());
    assert_eq!(old_output[0].batch, fused_output[0].batch);
    assert_eq!(old_output[0].diff.to_data(), fused_output[0].diff.to_data());
    for position in 0..cols.len() {
        let first = cols
            .iter()
            .position(|&column| column == cols[position])
            .unwrap();
        assert!(Arc::ptr_eq(
            fused_output[0].batch.column(position),
            fused_output[0].batch.column(first)
        ));
    }
    emit(serde_json::json!({
        "record": "filter_project_operator_allocation",
        "case": case.name,
        "iterations": loops,
        "source_rows": inputs.seed.len(),
        "source_columns": inputs.seed.batch.num_columns(),
        "projection_columns": cols.len(),
        "source_payload_bytes_seed": fixture.source_payload_bytes,
        "old_filter_then_project": allocation_stats(old_stats),
        "fused_filter_project": allocation_stats(fused_stats),
        "allocation_scope": "16 isolated operator calls per mode; outputs retained symmetrically through each mode's counter snapshot; process-global allocator counters; no wall-time claim"
    }));
    drop(old_output);
    drop(fused_output);
}

#[test]
#[ignore = "observer-off operator wall clock; separately archived release binary"]
fn filter_project_operator_wall_sample() {
    use hotlap_core::Predicate;
    use std::hint::black_box;
    use std::time::Instant;

    let name = std::env::var("FILTER_PROJECT_CASE").expect("FILTER_PROJECT_CASE is required");
    let case = CASES.iter().find(|case| case.name == name).unwrap();
    let mode = EngineMode::from_env();
    let _guard = mode.guard();
    let fixture = case_spec(*case);
    let inputs = generated_inputs(&fixture);
    let predicate = Predicate::Cmp {
        op: hotlap_core::CmpOp::Eq,
        col: fixture.predicate_column,
        scalar: hotlap_core::Scalar::I64(1),
    };
    let mask = predicate.eval(&inputs.seed.batch).unwrap();
    let cols = fixture.projected_columns();
    let loops = 500;
    let started = Instant::now();
    let mut rows = 0;
    for _ in 0..loops {
        let value = if mode == EngineMode::Candidate {
            crate::ops::filter_project::filter_project(&inputs.seed, &mask, &cols).unwrap()
        } else {
            let filtered = crate::ops::filter_project::filter(&inputs.seed, &mask).unwrap();
            crate::ops::filter_project::project(&filtered, &cols).unwrap()
        };
        rows += value.len();
        black_box(value);
    }
    emit(serde_json::json!({
        "record": "filter_project_operator_wall_sample",
        "case": case.name,
        "mode": mode.as_str(),
        "iterations": loops,
        "elapsed_loop_ns": started.elapsed().as_nanos(),
        "ns_per_operator_call": started.elapsed().as_nanos() as f64 / loops as f64,
        "result_rows_sum": rows,
        "observer_feature": false,
        "drop_policy": "each result consumed by black_box and dropped inside timed loop"
    }));
}

#[cfg(feature = "bench-allocation-observer")]
fn allocation_stats(stats: stats_alloc::Stats) -> serde_json::Value {
    serde_json::json!({
        "allocations": stats.allocations,
        "deallocations": stats.deallocations,
        "reallocations": stats.reallocations,
        "bytes_allocated": stats.bytes_allocated,
        "bytes_deallocated": stats.bytes_deallocated,
        "bytes_reallocated": stats.bytes_reallocated,
        "signed_alloc_free_delta_bytes": stats.bytes_allocated as i128 - stats.bytes_deallocated as i128
    })
}
