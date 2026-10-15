use super::measurement::*;
use super::*;
use std::time::Instant;

pub(super) fn recovery_matrix() {
    for case in cases() {
        recovery_case(case);
    }
}

fn recovery_case(case: AvgCase) {
    let (mut original, _, left, right) = seed(case, false);
    let checkpoint = original.checkpoint().unwrap();
    let checkpoint_fingerprint64_noncrypto = format!(
        "v1-{:016x}",
        crate::core::tests::paired_bench::checkpoint_hash(&checkpoint)
    );
    let (input, delta) = update(case, &left, &right);
    let reference_before = snapshots(&mut original, case);
    let reference_history = history_signature(&original);
    original.push(input, &delta).unwrap();
    let reference_changes = drain_views(&mut original, case);
    let reference_after = snapshots(&mut original, case);
    let reference_history_after = history_signature(&original);
    drop(original);
    let run_id = std::env::var("F7ZV_RUN_ID").unwrap_or_else(|_| "unset".into());
    for trial in 0..TRIALS {
        for dense in if trial % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let mut restored = avg_core(case);
            let mode = ModeGuard::new(dense);
            let started = Instant::now();
            restored.restore(&checkpoint).unwrap();
            let restore_ns = started.elapsed().as_nanos();
            drop(mode);
            assert_batches_same(&reference_before, &snapshots(&mut restored, case));
            assert_eq!(history_signature(&restored), reference_history);
            let (input, delta) = update(case, &left, &right);
            reset_counters();
            let mode = ModeGuard::new(dense);
            restored.push(input, &delta).unwrap();
            drop(mode);
            let continuation_path = counters();
            if dense || case.name.contains("control") {
                assert_eq!(continuation_path["factorized_commits"].as_u64(), Some(0));
                if dense {
                    assert!(continuation_path["dense_join_calls"].as_u64().unwrap() > 0);
                } else {
                    assert_eq!(continuation_path["flat_rows_cuts"].as_u64(), Some(1));
                }
            } else {
                assert!(continuation_path["factorized_commits"].as_u64().unwrap() > 0);
            }
            let after_changes = drain_views(&mut restored, case);
            assert_batches_same(&reference_changes, &after_changes);
            assert_batches_same(&reference_after, &snapshots(&mut restored, case));
            assert_eq!(history_signature(&restored), reference_history_after);
            println!(
                "{}",
                serde_json::json!({
                    "record":"f7zv_recovery_sample","run_id":run_id,"fixture":case.name,
                    "mode":if dense{"forced_dense"}else{"auto"},"trial":trial,"warmup":trial==0,
                    "restore_wall_ns":restore_ns,"checkpoint_fingerprint64_noncrypto":checkpoint_fingerprint64_noncrypto,
                    "same_immutable_checkpoint_object_reused_for_all_modes_and_trials":true,
                    "restore_timed_scope":"public EngineCore::restore only; construction and parity/continuation excluded",
                    "continuation_counters":counters(),"continuation_parity":"original live EngineCore output/history vs restored instance",
                    "avg_history_before":history_record(&reference_history),
                    "avg_history_after":history_record(&reference_history_after),
                })
            );
            drop(restored);
        }
    }
}
