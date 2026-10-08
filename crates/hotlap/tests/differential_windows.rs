//! Public-API differential coverage for event-time tumbling windows: the engine's
//! append-only snapshot must equal a full recomputation using the same watermark
//! calendar. See `docs/hotlap-event-time-windows.md` for the rule being replicated.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap::{Hotlap, InputId, Plan, ZSetBatch};
use hotlap_engine::EngineCore;

fn zset(columns: &[Vec<i64>], diffs: &[i64]) -> ZSetBatch {
    let fields: Vec<Field> = columns
        .iter()
        .enumerate()
        .map(|(i, _)| Field::new(format!("c{i}"), DataType::Int64, true))
        .collect();
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|c| Arc::new(Int64Array::from(c.clone())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

fn rows(z: &ZSetBatch) -> Vec<Vec<i64>> {
    let columns: Vec<&Int64Array> = z
        .batch
        .columns()
        .iter()
        .map(|c| c.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    (0..z.len())
        .map(|row| columns.iter().map(|c| c.value(row)).collect())
        .collect()
}

fn open() -> Hotlap {
    Hotlap::open_with(Box::new(EngineCore::new()))
}

fn tumble(key: usize, time_col: usize, size: i64) -> Plan {
    Plan::TumbleCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![key],
        time_col,
        size,
    }
}

/// Full-recomputation oracle for `tumble(key, size)` with the given lag.
/// `batches` are the batches in the same order/grouping the engine receives.
fn recompute_tumble(batches: &[Vec<(i64, i64)>], size: i64, lag: i64) -> Vec<Vec<i64>> {
    use std::collections::BTreeMap;
    let mut counts: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut wm: i64 = 0;
    for batch in batches {
        let mut max_ts = i64::MIN;
        for (ts, key) in batch {
            max_ts = max_ts.max(*ts);
            if *ts < wm {
                continue; // late
            }
            let ws = (ts / size) * size;
            *counts.entry((*key, ws)).or_default() += 1;
        }
        if max_ts != i64::MIN {
            wm = wm.max((max_ts - lag).max(0));
        }
    }
    counts
        .into_iter()
        .filter(|((_, ws), c)| *c != 0 && ws + size <= wm)
        .map(|((key, ws), c)| vec![key, ws, c])
        .collect()
}

#[test]
fn tumble_incremental_equals_recompute_across_batches() {
    let mut h = open();
    h.register_input("events").unwrap();
    h.declare_watermark("events", 0, 2).unwrap();
    h.create_view("w", tumble(1, 0, 10)).unwrap();

    let batches: Vec<Vec<(i64, i64)>> = vec![
        vec![(1, 5), (3, 5)],
        vec![(7, 5)],
        vec![(4, 7), (15, 7)], // 4 < wm(5) -> late
        vec![(25, 7)],         // wm 23 -> closes [10,20)
    ];
    for batch in &batches {
        let times: Vec<i64> = batch.iter().map(|(ts, _)| *ts).collect();
        let keys: Vec<i64> = batch.iter().map(|(_, k)| *k).collect();
        let ones = vec![1i64; batch.len()];
        h.push("events", &zset(&[times, keys], &ones)).unwrap();
    }

    let mut got = rows(&h.snapshot("w").unwrap());
    got.sort();
    let mut want = recompute_tumble(&batches, 10, 2);
    want.sort();
    assert_eq!(got, want);
    assert!(h.late_dropped("events").unwrap() >= 1);
    h.shutdown().unwrap();
}
