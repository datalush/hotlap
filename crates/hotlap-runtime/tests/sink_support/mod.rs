//! Assertion helpers for the `CREATE SINK` tests.

mod fakes;

pub use fakes::{session, session_with, source_batch};

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use hotlap::ZSetBatch;
use hotlap_connectors::source::SourceBatch;
use hotlap_runtime::{QueryResult, SqlSession};

fn col(batch: &RecordBatch, index: usize) -> &Int64Array {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}

fn rows(result: QueryResult) -> Vec<(i64, i64, i64)> {
    let QueryResult::Rows(batches) = result else {
        panic!("expected a result set");
    };
    let mut out = Vec::new();
    for batch in batches {
        let (k, w, c) = (col(&batch, 0), col(&batch, 1), col(&batch, 2));
        for i in 0..batch.num_rows() {
            out.push((k.value(i), w.value(i), c.value(i)));
        }
    }
    out.sort_unstable();
    out
}

/// Full recomputation of the closed tumbling-window counts from raw events.
pub fn recompute(batches: &[SourceBatch], size: i64, lag: i64) -> Vec<(i64, i64, i64)> {
    let mut events = Vec::new();
    for b in batches {
        for i in 0..b.batch.num_rows() {
            events.push((col(&b.batch, 0).value(i), col(&b.batch, 1).value(i)));
        }
    }
    let max_ts = events.iter().map(|(_, t)| *t).max().unwrap_or(0);
    let watermark = (max_ts - lag).max(0);
    let mut counts: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for (key, ts) in &events {
        let start = (ts / size) * size;
        if watermark >= start + size {
            *counts.entry((*key, start)).or_insert(0) += 1;
        }
    }
    counts.into_iter().map(|((k, w), c)| (k, w, c)).collect()
}

/// Consolidate the sink's Z-set stream into a sorted multiset of view rows.
///
/// Each row's multiplicity is summed across batches; a net-zero row cancels
/// out, matching the engine's consolidated snapshot.
pub fn consolidate(batches: &[ZSetBatch]) -> Vec<(i64, i64, i64)> {
    let mut counts: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
    for zset in batches {
        let (k, w, c) = (
            col(&zset.batch, 0),
            col(&zset.batch, 1),
            col(&zset.batch, 2),
        );
        let diff = zset
            .diff
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 diff");
        for i in 0..zset.len() {
            let key = (k.value(i), w.value(i), c.value(i));
            *counts.entry(key).or_insert(0) += diff.value(i);
        }
    }
    let mut out = Vec::new();
    for (row, multiplicity) in counts {
        for _ in 0..multiplicity.max(0) {
            out.push(row);
        }
    }
    out
}

/// Poll the MV until it returns `want` (or the deadline elapses).
pub async fn wait_for_rows(
    session: &mut SqlSession,
    want: &[(i64, i64, i64)],
) -> Vec<(i64, i64, i64)> {
    let query = "SELECT k, window_start, count FROM mv ORDER BY k, window_start";
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = rows(session.sql(query).await.expect("mv query failed"));
        if got == want || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Poll the sink accumulator until it matches `want` (or the deadline elapses).
pub async fn wait_for_sink(
    batches: &Arc<Mutex<Vec<ZSetBatch>>>,
    want: &[(i64, i64, i64)],
) -> Vec<(i64, i64, i64)> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = consolidate(&batches.lock().unwrap());
        if got == want || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
