//! Window, checkpoint, and recovery benchmark workloads.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

use arrow::array::Int64Array;
use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, WatermarkSpec};
use hotlap_engine::{EngineCore, decode_snapshot, encode_snapshot};

use crate::baseline_workloads::{batch, int_rows, metric, schema};

pub(super) fn window_workload(n: usize) {
    let start = Instant::now();
    let input_schema = schema(&["k", "v", "ts"]);
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        WatermarkSpec {
            time_col: 2,
            lag: 0,
        },
    )
    .unwrap();
    core.build_view(
        ViewId(0),
        &Plan::TumbleCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            time_col: 2,
            size: 10,
        },
    )
    .unwrap();
    let setup_ns = start.elapsed().as_nanos();
    let fixture_start = Instant::now();
    let seed = batch(
        input_schema.clone(),
        vec![vec![1; n], (0..n as i64).collect(), vec![0; n]],
        vec![1; n],
    );
    let advance = batch(input_schema, vec![vec![2], vec![0], vec![10]], vec![1]);
    let fixture_setup_ns = fixture_start.elapsed().as_nanos();
    metric(
        "tumble_count",
        "open_window_seed",
        n,
        n,
        setup_ns,
        fixture_setup_ns,
        || {
            core.push(InputId(0), &seed).unwrap();
        },
    );
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());
    metric(
        "tumble_count",
        "close_window",
        n + 1,
        1,
        setup_ns,
        fixture_setup_ns,
        || {
            core.push(InputId(0), &advance).unwrap();
        },
    );
    let output = core.snapshot(ViewId(0)).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        output
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        n as i64
    );
}

pub(super) fn recovery_multiview_workload(n: usize) {
    let start = Instant::now();
    let input_schema = schema(&["k", "v", "ts"]);
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    for (view, aggs) in [
        (ViewId(0), vec![AggSpec::count()]),
        (ViewId(1), vec![AggSpec::count(), AggSpec::sum(1)]),
    ] {
        core.build_view(
            view,
            &Plan::GroupAggregate {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                aggs,
            },
        )
        .unwrap();
    }
    let setup_ns = start.elapsed().as_nanos();
    let fixture_start = Instant::now();
    let seed = batch(
        input_schema.clone(),
        vec![vec![1; n], vec![2; n], vec![0; n]],
        vec![1; n],
    );
    let continuation = batch(input_schema, vec![vec![1], vec![3], vec![0]], vec![1]);
    let fixture_setup_ns = fixture_start.elapsed().as_nanos();
    core.push(InputId(0), &seed).unwrap();
    assert_eq!(
        int_rows(&core.snapshot(ViewId(0)).unwrap()),
        BTreeMap::from([(vec![1, n as i64], 1)])
    );
    assert_eq!(
        int_rows(&core.snapshot(ViewId(1)).unwrap()),
        BTreeMap::from([(vec![1, n as i64, (2 * n) as i64], 1)])
    );
    let checkpoint = metric(
        "multiview_recovery",
        "snapshot_capture",
        n,
        0,
        setup_ns,
        fixture_setup_ns,
        || core.checkpoint().unwrap(),
    );
    let mut restored_core = metric(
        "multiview_recovery",
        "encode_decode_restore",
        n,
        0,
        setup_ns,
        fixture_setup_ns,
        || {
            let bytes = encode_snapshot(&checkpoint).unwrap();
            let decoded = decode_snapshot(&bytes).unwrap();
            let mut restored = EngineCore::new();
            restored.restore(&decoded).unwrap();
            restored
        },
    );
    for view in [ViewId(0), ViewId(1)] {
        assert_eq!(
            int_rows(&restored_core.snapshot(view).unwrap()),
            int_rows(&core.snapshot(view).unwrap())
        );
    }
    core.push(InputId(0), &continuation).unwrap();
    metric(
        "multiview_recovery",
        "continuation_after_restore",
        n + 1,
        1,
        setup_ns,
        fixture_setup_ns,
        || restored_core.push(InputId(0), &continuation).unwrap(),
    );
    for view in [ViewId(0), ViewId(1)] {
        assert_eq!(
            int_rows(&core.snapshot(view).unwrap()),
            int_rows(&restored_core.snapshot(view).unwrap())
        );
    }
    assert_eq!(
        int_rows(&core.snapshot(ViewId(0)).unwrap()),
        BTreeMap::from([(vec![1, n as i64 + 1], 1)])
    );
    assert_eq!(
        int_rows(&core.snapshot(ViewId(1)).unwrap()),
        BTreeMap::from([(vec![1, n as i64 + 1, (2 * n + 3) as i64], 1)])
    );
    black_box(&restored_core);
}
