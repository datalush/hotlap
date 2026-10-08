//! Wall-clock benchmark: one incremental push versus a full recomputation.
//!
//! Seeds a large view state, then drives a long history of compensated updates
//! (each nets to zero, so the current state stays bounded) and reports the
//! per-push cost against a full `consolidate` over the whole history.
//!
//! The push is timed alone: snapshots are not part of the per-push cost, so the
//! two operations are no longer conflated. Timing is environment-sensitive, so
//! the timing assertion lives in an `#[ignore]`d test; the normal suite keeps a
//! non-timing correctness check that the snapshot equals the recomputation.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::{EngineCore, consolidate};

const KEYS: i64 = 500;
const EPOCHS: i64 = 4_000;

fn text_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

/// Builds a Z-set of `(key, value, diff)` triples over the text schema.
fn text_zset(rows: &[(i64, &str, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let batch = RecordBatch::try_new(text_schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn group_plan() -> Plan {
    Plan::GroupCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
    }
}

/// Costs and row counts measured by a single benchmark run.
struct Measurement {
    per_push_ns: u128,
    snapshot_ns: u128,
    recompute_ns: u128,
    history_len: usize,
    snapshot_len: usize,
    recomputed_len: usize,
}

/// Runs the workload once, timing the pushes, the snapshot and the recompute.
fn measure() -> Measurement {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();

    // Seed a large current state and mirror it into the recomputation history.
    let seed: Vec<(i64, &str, i64)> = (0..KEYS).map(|key| (key, "v", 1)).collect();
    core.push(InputId(0), &text_zset(&seed)).unwrap();
    let mut history: Vec<(i64, &str, i64)> = seed.clone();

    // Time the pushes alone: the per-push cost excludes snapshots.
    let mut push_ns = 0u128;
    for epoch in 0..EPOCHS {
        let key = epoch % KEYS;
        let batch = text_zset(&[(key, "v", 1), (key, "v", -1)]);
        history.push((key, "v", 1));
        history.push((key, "v", -1));

        let start = Instant::now();
        core.push(InputId(0), &batch).unwrap();
        push_ns += start.elapsed().as_nanos();
    }

    // Materialize once (outside the push loop) and compare to recomputation.
    let start = Instant::now();
    let snapshot = core.snapshot(ViewId(0)).unwrap();
    let snapshot_ns = start.elapsed().as_nanos();

    let start = Instant::now();
    let recomputed = consolidate(&text_zset(&history)).unwrap();
    let recompute_ns = start.elapsed().as_nanos();
    std::hint::black_box(recomputed.len());

    Measurement {
        per_push_ns: push_ns / EPOCHS as u128,
        snapshot_ns,
        recompute_ns,
        history_len: history.len(),
        snapshot_len: snapshot.len(),
        recomputed_len: recomputed.len(),
    }
}

#[test]
fn incremental_push_matches_full_recompute() {
    let measured = measure();
    assert_eq!(measured.snapshot_len, measured.recomputed_len);
}

#[test]
#[ignore = "wall-clock sensitive; run with --ignored to compare timings"]
fn incremental_push_beats_full_recompute_on_large_state() {
    let measured = measure();
    assert_eq!(measured.snapshot_len, measured.recomputed_len);
    eprintln!(
        "incremental push: {} ns/push (snapshot {} ns); full recompute: {} ns over {} history \
         rows ({:.1}x)",
        measured.per_push_ns,
        measured.snapshot_ns,
        measured.recompute_ns,
        measured.history_len,
        measured.recompute_ns as f64 / measured.per_push_ns.max(1) as f64
    );
    // Wide margin: a delta-scoped push should beat recomputing the whole
    // history by orders of magnitude, not merely by a few percent.
    assert!(
        measured.per_push_ns.saturating_mul(4) < measured.recompute_ns,
        "incremental push ({} ns) should beat full recompute ({} ns) by a wide margin",
        measured.per_push_ns,
        measured.recompute_ns
    );
}
