//! Wall-clock benchmark: one incremental push versus a full recomputation.
//!
//! Seeds a large view state, then drives a long history of compensated updates
//! (each nets to zero, so the current state stays bounded) and reports the
//! per-push cost against a full `consolidate` over the whole history.

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

#[test]
fn incremental_push_beats_full_recompute_on_large_state() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();

    // Seed a large current state and mirror it into the recomputation history.
    let seed: Vec<(i64, &str, i64)> = (0..KEYS).map(|key| (key, "v", 1)).collect();
    core.push(InputId(0), &text_zset(&seed)).unwrap();
    let mut history: Vec<(i64, &str, i64)> = seed.clone();

    // Long history of compensated updates: state size stays `KEYS`, history
    // grows by two rows per epoch.
    let mut incremental_ns = 0u128;
    for epoch in 0..EPOCHS {
        let key = epoch % KEYS;
        let batch = text_zset(&[(key, "v", 1), (key, "v", -1)]);
        history.push((key, "v", 1));
        history.push((key, "v", -1));

        let start = Instant::now();
        core.push(InputId(0), &batch).unwrap();
        let snapshot = core.snapshot(ViewId(0)).unwrap();
        incremental_ns += start.elapsed().as_nanos();
        std::hint::black_box(snapshot.len());
    }
    let per_push = incremental_ns / EPOCHS as u128;

    // Full recomputation over the entire input history.
    let historic = text_zset(&history);
    let start = Instant::now();
    let recomputed = consolidate(&historic).unwrap();
    let recompute_ns = start.elapsed().as_nanos();
    std::hint::black_box(recomputed.len());

    eprintln!(
        "incremental push: {per_push} ns/push; full recompute: {recompute_ns} ns \
         over {} history rows ({:.1}x)",
        history.len(),
        recompute_ns as f64 / per_push.max(1) as f64
    );
    assert!(
        per_push < recompute_ns,
        "incremental push ({per_push} ns) should beat full recompute ({recompute_ns} ns)"
    );
}
