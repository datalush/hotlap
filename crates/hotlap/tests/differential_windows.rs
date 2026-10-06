//! Public-API differential coverage for event-time tumbling windows: the engine's
//! append-only snapshot must equal a full recomputation using the same watermark
//! calendar. See `docs/hotlap-event-time-windows.md` for the rule being replicated.

use hotlap::{ChangeBatch, Hotlap, InputId, Plan, Row, Scalar};

fn tumble(key: usize, time_col: usize, size: i64) -> Plan {
    Plan::TumbleCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![key],
        time_col,
        size,
    }
}

/// Oráculo de recomputación completa para `tumble(key, size)` con lag dado.
/// `batches` son los lotes en el mismo orden/agrupación que el motor recibe.
fn recompute_tumble(batches: &[Vec<(i64, i64)>], size: i64, lag: i64) -> Vec<Row> {
    use std::collections::BTreeMap;
    let mut counts: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut wm: i64 = 0;
    for batch in batches {
        let mut max_ts = i64::MIN;
        for (ts, key) in batch {
            max_ts = max_ts.max(*ts);
            if *ts < wm {
                continue; // tardío
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
        .map(|((key, ws), c)| Row(vec![Scalar::I64(key), Scalar::I64(ws), Scalar::I64(c)]))
        .collect()
}

#[test]
fn tumble_incremental_equals_recompute_across_batches() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("events").unwrap();
    h.declare_watermark("events", 0, 2).unwrap();
    h.create_view("w", tumble(1, 0, 10)).unwrap();

    let batches: Vec<Vec<(i64, i64)>> = vec![
        vec![(1, 5), (3, 5)],
        vec![(7, 5)],
        vec![(4, 7), (15, 7)], // 4 < wm(5) -> tardío
        vec![(25, 7)],         // wm 23 -> cierra [10,20)
    ];
    for batch in &batches {
        let mut b = ChangeBatch::default();
        for (ts, k) in batch {
            b.push(Row(vec![Scalar::I64(*ts), Scalar::I64(*k)]), 1);
        }
        h.push("events", &b).unwrap();
    }

    let mut got = h.snapshot("w").unwrap();
    got.sort();
    let mut want = recompute_tumble(&batches, 10, 2);
    want.sort();
    assert_eq!(got, want);
    assert!(h.late_dropped("events").unwrap() >= 1);
    h.shutdown().unwrap();
}
