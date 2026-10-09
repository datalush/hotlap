//! End-to-end test for `CREATE SINK` wiring through `SqlSession`.

mod sink_support;

use std::sync::{Arc, Mutex};

use hotlap_sql::SqlError;

use sink_support::{
    consolidate, recompute, session, session_with, source_batch, wait_for_rows, wait_for_sink,
};

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src \
     GROUP BY k, tumble(_event_time, INTERVAL '10 s');";
const SINK: &str = "CREATE SINK out WITH (connector='inmem') AS SELECT * FROM mv;";

#[tokio::test]
async fn sink_state_matches_snapshot() {
    let data = vec![
        source_batch(&[1, 1, 1, 2], &[1000, 2000, 3000, 1000]),
        source_batch(&[1, 2], &[12000, 12000]),
        source_batch(&[1], &[21000]),
    ];
    let sink_batches = Arc::new(Mutex::new(Vec::new()));
    let mut session = session(data.clone(), &sink_batches);
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql(SINK).await.unwrap();
    session.sql("START;").await.unwrap();

    // Read the engine snapshot through the MV table provider.
    let snap = wait_for_rows(&mut session, &recompute(&data, 10_000, 1_000)).await;
    assert_eq!(snap, recompute(&data, 10_000, 1_000));
    // The sink must consolidate to exactly the engine snapshot.
    assert_eq!(wait_for_sink(&sink_batches, &snap).await, snap);
    assert_eq!(
        consolidate(&sink_batches.lock().unwrap()),
        snap,
        "sink Z-set must consolidate to the engine snapshot"
    );
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn create_sink_after_start_rejected() {
    let sink_batches = Arc::new(Mutex::new(Vec::new()));
    let mut session = session(vec![source_batch(&[1], &[1000])], &sink_batches);
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql("START;").await.unwrap();
    assert!(session.sql(SINK).await.is_err(), "CREATE SINK after START");
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn create_sink_unknown_view_rejected() {
    let sink_batches = Arc::new(Mutex::new(Vec::new()));
    let mut session = session(vec![source_batch(&[1], &[1000])], &sink_batches);
    session.sql(SOURCE).await.unwrap();
    let bad = "CREATE SINK out WITH (connector='inmem') AS SELECT * FROM nope;";
    assert!(matches!(session.sql(bad).await, Err(SqlError::Catalog(_))));
}

#[tokio::test]
async fn retracting_view_with_append_only_sink_rejected_at_start() {
    let sink_batches = Arc::new(Mutex::new(Vec::new()));
    // A sink that does not accept retractions cannot target the grouped,
    // tumbling view; START must refuse before any write.
    let mut session = session_with(vec![source_batch(&[1], &[1000])], &sink_batches, false);
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql(SINK).await.unwrap();
    let error = match session.sql("START;").await {
        Err(error) => error,
        Ok(_) => panic!("a retracting plan must be rejected before writes"),
    };
    assert!(matches!(error, SqlError::Engine(_)), "{error:?}");
}

#[tokio::test]
async fn second_sink_on_same_view_rejected() {
    let sink_batches = Arc::new(Mutex::new(Vec::new()));
    let mut session = session(vec![source_batch(&[1], &[1000])], &sink_batches);
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql(SINK).await.unwrap();
    let dup = "CREATE SINK out2 WITH (connector='inmem') AS SELECT * FROM mv;";
    assert!(matches!(
        session.sql(dup).await,
        Err(SqlError::Unsupported(_))
    ));
}
