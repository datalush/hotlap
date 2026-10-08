//! End-to-end test for the embedded [`Session`] API.
//!
//! Oraculo: the `SELECT` over the materialized view equals a full
//! recomputation over the raw events.

mod session_support;

use std::collections::BTreeMap;
use std::time::Duration;

use hotlap_connectors::source::SourceBatch;
use hotlap_runtime::{Session, SessionConfig};
use session_support::{MemBackend, batch, col, factory, wait_for_rows};

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src \
     GROUP BY k, tumble(_event_time, INTERVAL '10 s');";

/// Full recomputation of the closed tumbling-window counts from the raw events.
fn recompute(batches: &[SourceBatch], size: i64, lag: i64) -> Vec<(i64, i64, i64)> {
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

#[test]
fn session_end_to_end_matches_recomputation() {
    let data = vec![
        batch(&[1, 1, 1, 2], &[1000, 2000, 3000, 1000]),
        batch(&[1, 2], &[12000, 12000]),
        batch(&[1], &[21000]),
    ];
    let config = SessionConfig::new()
        .with_source_factory(factory(data.clone()))
        .with_checkpoint(
            Duration::from_secs(3600),
            1,
            Box::new(MemBackend::default()),
        );
    let mut session = Session::open(config).expect("open session");

    session.sql(SOURCE).expect("create source");
    session.sql(VIEW).expect("create view");
    session.sql("START;").expect("start engine");

    let want = recompute(&data, 10_000, 1_000);
    assert_eq!(wait_for_rows(&mut session, "mv", &want), want);

    let metrics = session.metrics();
    assert!(!metrics.entries.is_empty(), "metrics must not be empty");
    assert!(metrics.entries.get("rows_ingested").copied().unwrap_or(0) > 0);

    let id = session.checkpoint().expect("checkpoint");
    assert!(id >= 1, "checkpoint id must be positive");
    let metrics = session.metrics();
    assert!(
        metrics
            .entries
            .get("checkpoints_taken")
            .copied()
            .unwrap_or(0)
            >= 1,
        "checkpoint must be counted"
    );

    session.shutdown().expect("shutdown");
}

#[test]
fn mixed_case_source_name_binds_the_view() {
    // `Src` is unquoted, so DataFusion canonicalizes it to lowercase. The
    // DDL-to-binding map must use the same canonical name, otherwise the view
    // cannot resolve the relation even for a single source.
    let config = SessionConfig::new().with_source_factory(factory(vec![]));
    let mut session = Session::open(config).expect("open session");
    session
        .sql(
            "CREATE SOURCE Src WITH (connector='inmem') WATERMARK FOR \
             _event_time AS _event_time - INTERVAL '1 s';",
        )
        .expect("create mixed-case source");
    session
        .sql("CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM Src GROUP BY k;")
        .expect("mixed-case relation must resolve to its source binding");
}
