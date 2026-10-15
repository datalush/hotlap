use super::*;
use std::time::Instant;

pub(super) const TRIALS: usize = 6; // trial zero is the explicit warmup pair

pub(super) fn cases() -> Vec<AvgCase> {
    vec![
        AvgCase {
            name: "avg-left-small-replace",
            fanout: 1,
            project_subset: false,
            avg_left: true,
            repeated: false,
            with_mixed_flat_view: false,
            update: Update::ReplaceLeft,
            all_null: false,
        },
        AvgCase {
            name: "avg-right-fanout16-append",
            fanout: 16,
            project_subset: false,
            avg_left: false,
            repeated: false,
            with_mixed_flat_view: false,
            update: Update::AppendRight,
            all_null: false,
        },
        AvgCase {
            name: "avg-left-fanout256-replace",
            fanout: 256,
            project_subset: false,
            avg_left: true,
            repeated: false,
            with_mixed_flat_view: false,
            update: Update::ReplaceLeft,
            all_null: false,
        },
        AvgCase {
            name: "avg-projected-repeated-mixed-flat",
            fanout: 4,
            project_subset: true,
            avg_left: false,
            repeated: true,
            with_mixed_flat_view: true,
            update: Update::ReplaceRight,
            all_null: false,
        },
        AvgCase {
            name: "avg-all-null-right-to-value",
            fanout: 1,
            project_subset: false,
            avg_left: false,
            repeated: false,
            with_mixed_flat_view: false,
            update: Update::ReplaceRight,
            all_null: true,
        },
        AvgCase {
            name: "avg-inexact-outside-2^53-control",
            fanout: 1,
            project_subset: false,
            avg_left: true,
            repeated: false,
            with_mixed_flat_view: false,
            update: Update::AppendRight,
            all_null: false,
        },
        AvgCase {
            name: "avg-inexact-old-history-control",
            fanout: 1,
            project_subset: false,
            avg_left: true,
            repeated: false,
            with_mixed_flat_view: false,
            update: Update::AppendRight,
            all_null: false,
        },
    ]
}

pub(super) fn source_rows(case: AvgCase) -> (ZSetBatch, ZSetBatch) {
    if case.name == "avg-inexact-outside-2^53-control" {
        return (
            nullable_rows(&[0], &[Some((1_i64 << 53) + 1)], &[1]),
            nullable_rows(&[0], &[Some(0)], &[1]),
        );
    }
    if case.name == "avg-inexact-old-history-control" {
        return (
            nullable_rows(&[0], &[Some(1)], &[1]),
            nullable_rows(&[0], &[Some(0)], &[1]),
        );
    }
    let mut keys = Vec::with_capacity(case.fanout * 4);
    let mut left_values = Vec::with_capacity(case.fanout * 4);
    let mut right_values = Vec::with_capacity(case.fanout * 4);
    for key in 0..4_i64 {
        for row in 0..case.fanout {
            keys.push(key);
            left_values.push(Some((row % 9 + 2) as i64));
            right_values.push(if case.all_null || row == 1 || (row != 0 && row % 7 == 0) {
                None
            } else {
                Some((row % 11 + 1) as i64)
            });
        }
    }
    (
        nullable_rows(&keys, &left_values, &vec![1; keys.len()]),
        nullable_rows(&keys, &right_values, &vec![1; keys.len()]),
    )
}

pub(super) fn update(case: AvgCase, left: &ZSetBatch, right: &ZSetBatch) -> (InputId, ZSetBatch) {
    match case.name {
        "avg-inexact-outside-2^53-control" => (InputId(1), nullable_rows(&[0], &[Some(1)], &[1])),
        "avg-inexact-old-history-control" => (InputId(0), nullable_rows(&[0], &[Some(2)], &[1])),
        _ => match case.update {
            Update::ReplaceLeft => {
                let old = left
                    .batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let old_value = old.value(0);
                (
                    InputId(0),
                    nullable_rows(&[0, 0], &[Some(old_value), Some(12)], &[-1, 1]),
                )
            }
            Update::ReplaceRight => {
                let old = right
                    .batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let value = (!old.is_null(0)).then(|| old.value(0));
                (
                    InputId(1),
                    nullable_rows(&[0, 0], &[value, Some(10)], &[-1, 1]),
                )
            }
            Update::AppendRight => (InputId(1), nullable_rows(&[0], &[Some(10)], &[1])),
            _ => panic!("unsupported AVG measurement update"),
        },
    }
}

pub(super) fn reset_counters() {
    crate::core::graph::FACTORIZED_CUTS.with(|c| c.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|c| c.set(0));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|c| c.set(0));
    crate::core::graph::DENSE_JOIN_ROWS.with(|c| c.set(0));
    crate::core::graph::DENSE_JOIN_BYTES.with(|c| c.set(0));
}

pub(super) fn counters() -> serde_json::Value {
    serde_json::json!({
        "factorized_commits": crate::core::graph::FACTORIZED_CUTS.with(Cell::get),
        "flat_rows_cuts": crate::core::graph::FLAT_ROWS_CUTS.with(Cell::get),
        "dense_join_calls": crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(Cell::get),
        "dense_join_rows": crate::core::graph::DENSE_JOIN_ROWS.with(Cell::get),
        "dense_join_array_bytes": crate::core::graph::DENSE_JOIN_BYTES.with(Cell::get),
    })
}

pub(super) fn drain_views(core: &mut EngineCore, case: AvgCase) -> Vec<ZSetBatch> {
    let mut views = vec![core.take_changes(ViewId(0)).unwrap()];
    if case.with_mixed_flat_view {
        views.push(core.take_changes(ViewId(1)).unwrap());
    }
    views
}

pub(super) fn snapshots(core: &mut EngineCore, case: AvgCase) -> Vec<ZSetBatch> {
    let mut views = vec![core.snapshot(ViewId(0)).unwrap()];
    if case.with_mixed_flat_view {
        views.push(core.snapshot(ViewId(1)).unwrap());
    }
    views
}

fn seed_ops(case: AvgCase, left: &ZSetBatch, right: &ZSetBatch) -> Vec<(InputId, ZSetBatch)> {
    if case.name == "avg-inexact-old-history-control" {
        vec![
            (InputId(0), nullable_rows(&[0], &[Some(1_i64 << 53)], &[1])),
            (InputId(1), right.clone()),
            (InputId(0), nullable_rows(&[0], &[Some(1)], &[1])),
            (InputId(0), nullable_rows(&[0], &[Some(1_i64 << 53)], &[-1])),
        ]
    } else {
        vec![(InputId(0), left.clone()), (InputId(1), right.clone())]
    }
}

pub(super) fn seed(case: AvgCase, dense: bool) -> (EngineCore, u128, ZSetBatch, ZSetBatch) {
    let (left, right) = source_rows(case);
    let operations = seed_ops(case, &left, &right);
    let started = Instant::now();
    let mut core = avg_core(case);
    for (input, batch) in operations {
        let mode = ModeGuard::new(dense);
        core.push(input, &batch).unwrap();
        drop(mode);
        let _ = drain_views(&mut core, case);
    }
    let elapsed = started.elapsed().as_nanos();
    (core, elapsed, left, right)
}

fn seed_pair(case: AvgCase) -> (EngineCore, EngineCore, u128, ZSetBatch, ZSetBatch) {
    let (left, right) = source_rows(case);
    let operations = seed_ops(case, &left, &right);
    let started = Instant::now();
    let mut auto = avg_core(case);
    let mut dense = avg_core(case);
    for (input, batch) in operations {
        auto.push(input, &batch).unwrap();
        let auto_changes = drain_views(&mut auto, case);
        let auto_snapshots = snapshots(&mut auto, case);
        let auto_history = history_signature(&auto);
        let mode = ModeGuard::new(true);
        dense.push(input, &batch).unwrap();
        drop(mode);
        let dense_changes = drain_views(&mut dense, case);
        assert_batches_same(&auto_changes, &dense_changes);
        assert_batches_same(&auto_snapshots, &snapshots(&mut dense, case));
        assert_eq!(auto_history, history_signature(&dense));
    }
    (auto, dense, started.elapsed().as_nanos(), left, right)
}

pub(super) fn avg_output_signature(views: &[ZSetBatch]) -> Vec<Vec<(bool, u64)>> {
    views
        .iter()
        .take(1)
        .map(|view| {
            view.batch
                .columns()
                .iter()
                .filter_map(|column| column.as_any().downcast_ref::<arrow::array::Float64Array>())
                .flat_map(|values| {
                    (0..values.len()).map(|row| {
                        (
                            values.is_null(row),
                            if values.is_null(row) {
                                0
                            } else {
                                values.value(row).to_bits()
                            },
                        )
                    })
                })
                .collect()
        })
        .collect()
}

pub(super) struct PushResult {
    pub(super) elapsed_ns: u128,
    pub(super) path: serde_json::Value,
    pub(super) changes: Vec<ZSetBatch>,
    pub(super) snapshots: Vec<ZSetBatch>,
    pub(super) history: super::AvgHistory,
}

pub(super) fn timed_push(
    core: &mut EngineCore,
    case: AvgCase,
    input: InputId,
    delta: &ZSetBatch,
    dense: bool,
) -> PushResult {
    reset_counters();
    let mode = ModeGuard::new(dense);
    let started = Instant::now();
    core.push(input, delta).unwrap();
    let elapsed_ns = started.elapsed().as_nanos();
    drop(mode);
    let path = counters();
    let changes = drain_views(core, case);
    let snapshots = snapshots(core, case);
    let history = history_signature(core);
    PushResult {
        elapsed_ns,
        path,
        changes,
        snapshots,
        history,
    }
}

pub(super) fn count_contract(result: &PushResult, case: AvgCase, dense: bool) {
    let factorized = result.path["factorized_commits"].as_u64().unwrap();
    let row_cuts = result.path["flat_rows_cuts"].as_u64().unwrap();
    if dense {
        assert_eq!(factorized, 0);
        assert!(result.path["dense_join_calls"].as_u64().unwrap() > 0);
    } else if case.name.contains("control") {
        assert_eq!(
            factorized, 0,
            "{} fallback reason must remain Dense",
            case.name
        );
        assert_eq!(row_cuts, 1, "{} must consume actual joined rows", case.name);
    } else {
        assert!(factorized > 0, "{} must publish certified AVG", case.name);
        assert_eq!(row_cuts, u64::from(case.with_mixed_flat_view));
    }
}

pub(super) fn actual_counts(
    case: AvgCase,
    left: &ZSetBatch,
    right: &ZSetBatch,
) -> serde_json::Value {
    let weight = |batch: &ZSetBatch| {
        batch
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .iter()
            .sum::<i64>()
    };
    let payload_count = |batch: &ZSetBatch| {
        let values = batch
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..values.len())
            .map(|row| (!values.is_null(row)).then(|| values.value(row)))
            .collect::<std::collections::HashSet<_>>()
            .len()
    };
    let key_counts = |batch: &ZSetBatch| {
        let hi = batch
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lo = batch
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut counts = HashMap::new();
        for row in 0..batch.len() {
            *counts
                .entry((hi.value(row), lo.value(row)))
                .or_insert(0usize) += 1;
        }
        counts
    };
    let left_keys = key_counts(left);
    let right_keys = key_counts(right);
    let matching_pairs = left_keys
        .iter()
        .map(|(key, count)| count.saturating_mul(*right_keys.get(key).unwrap_or(&0)))
        .sum::<usize>();
    serde_json::json!({
        "left_physical_rows": left.len(),
        "right_physical_rows": right.len(),
        "left_weight": weight(left),
        "right_weight": weight(right),
        "left_distinct_nullable_payload_values": payload_count(left),
        "right_distinct_nullable_payload_values": payload_count(right),
        "matching_join_products_before_consolidation": matching_pairs,
        "fixture_fanout_parameter": case.fanout,
        "mixed_flat_view": case.with_mixed_flat_view,
        "seed_history_sequence": if case.name == "avg-inexact-old-history-control" {
            serde_json::json!(["left +2^53", "left +1", "left -2^53"])
        } else {
            serde_json::Value::Null
        },
    })
}

fn emit_timing_case(case: AvgCase) {
    let run_id = std::env::var("F7ZV_RUN_ID").unwrap_or_else(|_| "unset".into());
    for trial in 0..TRIALS {
        let (mut auto, mut dense, paired_setup_ns, left, right) = seed_pair(case);
        let (input, delta) = update(case, &left, &right);
        let before_auto = avg_output_signature(&snapshots(&mut auto, case));
        let before_dense = avg_output_signature(&snapshots(&mut dense, case));
        assert_eq!(before_auto, before_dense);
        let before_history = history_signature(&auto);
        let order = if trial % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        let mut auto_result = None;
        let mut dense_result = None;
        for dense_mode in order {
            if dense_mode {
                dense_result = Some(timed_push(&mut dense, case, input, &delta, true));
            } else {
                auto_result = Some(timed_push(&mut auto, case, input, &delta, false));
            }
        }
        let auto_result = auto_result.unwrap();
        let dense_result = dense_result.unwrap();
        if !case.name.contains("control") {
            assert_ne!(
                before_auto,
                avg_output_signature(&auto_result.snapshots),
                "{} must change AVG output",
                case.name
            );
        }
        assert_eq!(auto_result.changes.len(), dense_result.changes.len());
        assert_batches_same(&auto_result.changes, &dense_result.changes);
        assert_batches_same(&auto_result.snapshots, &dense_result.snapshots);
        assert_eq!(auto_result.history, dense_result.history);
        assert_ne!(
            before_history, auto_result.history,
            "{} update must change stored AVG history",
            case.name
        );
        count_contract(&auto_result, case, false);
        count_contract(&dense_result, case, true);
        for (mode, result) in [("auto", auto_result), ("forced_dense", dense_result)] {
            println!(
                "{}",
                serde_json::json!({
                    "record":"f7zv_timing_sample","run_id":run_id,"fixture":case.name,
                    "mode":mode,"trial":trial,"warmup":trial==0,
                    "public_push_wall_ns":result.elapsed_ns,"paired_seed_setup_wall_ns_separate":paired_setup_ns,
                    "timed_scope":"one EngineCore::push only; setup/input construction/drain/snapshot/history/parity excluded",
                    "active_features":active_features(),"allocator_observer_enabled":cfg!(feature="bench-allocation-observer"),
                    "left_input_rows":left.len(),"right_input_rows":right.len(),
                    "source_shape":actual_counts(case,&left,&right),
                    "output_group_rows":result.snapshots[0].len(),"path":result.path,
                    "snapshot_rows":result.snapshots.iter().map(ZSetBatch::len).collect::<Vec<_>>(),
                    "avg_history_groups":result.history.len(),
                    "avg_history_before":history_record(&before_history),
                    "avg_history":history_record(&result.history),
                    "avg_history_checkpoint_fingerprint64_noncrypto":history_fingerprint64(&result.history),
                })
            );
        }
    }
}

pub(super) fn history_fingerprint64(history: &super::AvgHistory) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    history.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub(super) fn history_record(history: &super::AvgHistory) -> serde_json::Value {
    serde_json::Value::Array(
        history
            .iter()
            .map(|(key, values)| {
                serde_json::json!({
                    "group_key_hex":key.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
                    "avg_slots":values.iter().map(|(sum_bits,count,nan,pos_inf,neg_inf)| serde_json::json!({
                        "sum_f64_bits":format!("{sum_bits:016x}"),"count":count,
                        "nan":nan,"pos_inf":pos_inf,"neg_inf":neg_inf,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

#[test]
#[ignore = "f7zv release-only paired performance/recovery harness; run by recorded measurement wrapper"]
fn avg_paired_measurement_driver() {
    assert!(!std::hint::black_box(cfg!(
        feature = "bench-allocation-observer"
    )));
    if let Ok(fixture_name) = std::env::var("F7ZV_PROCESS_FIXTURE") {
        super::memory::process_memory_case(&fixture_name);
        return;
    }
    if std::env::var_os("F7ZV_RECOVERY").is_some() {
        super::recovery::recovery_matrix();
        return;
    }
    for case in cases() {
        emit_timing_case(case);
    }
}

#[test]
fn measurement_case_contracts_validate_all_actual_fixture_builders() {
    for case in cases() {
        let (mut auto, mut dense, _, left, right) = seed_pair(case);
        let (input, delta) = update(case, &left, &right);
        let before = history_signature(&auto);
        let before_snapshot = snapshots(&mut auto, case);
        assert_batches_same(&before_snapshot, &snapshots(&mut dense, case));
        let before_avg = avg_output_signature(&before_snapshot);
        reset_counters();
        auto.push(input, &delta).unwrap();
        let auto_path = counters();
        let auto_changes = drain_views(&mut auto, case);
        let auto_snapshots = snapshots(&mut auto, case);
        let auto_history = history_signature(&auto);
        reset_counters();
        let mode = ModeGuard::new(true);
        dense.push(input, &delta).unwrap();
        drop(mode);
        let dense_path = counters();
        let dense_changes = drain_views(&mut dense, case);
        let dense_snapshots = snapshots(&mut dense, case);
        let dense_history = history_signature(&dense);
        assert_batches_same(&auto_changes, &dense_changes);
        assert_batches_same(&auto_snapshots, &dense_snapshots);
        assert_eq!(auto_history, dense_history, "{} AVG history", case.name);
        assert_ne!(before, auto_history, "{} history must change", case.name);
        assert!(
            before_avg != avg_output_signature(&auto_snapshots) || case.name.contains("control"),
            "{} eligible fixture must change a visible AVG",
            case.name
        );
        let expected_fallback = case.name.contains("control");
        if expected_fallback {
            assert_eq!(
                auto_path["factorized_commits"].as_u64(),
                Some(0),
                "{}",
                case.name
            );
            assert_eq!(
                auto_path["flat_rows_cuts"].as_u64(),
                Some(1),
                "{}",
                case.name
            );
        } else {
            assert!(
                auto_path["factorized_commits"].as_u64().unwrap() > 0,
                "{}",
                case.name
            );
            assert_eq!(
                auto_path["flat_rows_cuts"].as_u64(),
                Some(u64::from(case.with_mixed_flat_view)),
                "{}",
                case.name
            );
        }
        assert_eq!(dense_path["factorized_commits"].as_u64(), Some(0));
        assert!(dense_path["dense_join_calls"].as_u64().unwrap() > 0);
        assert_eq!(before_snapshot[0].schema(), auto_snapshots[0].schema());
        assert_eq!(auto_changes[0].schema(), dense_changes[0].schema());
        assert!(!actual_counts(case, &left, &right).is_null());
    }
}

#[test]
fn history_fingerprint_is_explicitly_noncryptographic() {
    let history = vec![(vec![1, 2], vec![(7, 3, 0, 0, 0)])];
    assert_eq!(history_fingerprint64(&history).len(), 16);
}
