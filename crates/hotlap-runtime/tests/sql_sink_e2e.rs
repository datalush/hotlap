//! End-to-end test for `CREATE SINK` wiring through `SqlSession`.

mod sink_support;

use std::sync::{Arc, Mutex};

use hotlap_sql::SqlError;

use sink_support::{
    consolidate, recompute, session, session_with, session_with_effects, source_batch,
    wait_for_rows, wait_for_sink,
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
    // A changing grouped aggregate still retracts and must be refused before writes.
    let (mut session, effects) =
        session_with_effects(vec![source_batch(&[1], &[1000])], &sink_batches, false);
    session.sql(SOURCE).await.unwrap();
    session
        .sql("CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src GROUP BY k;")
        .await
        .unwrap();
    session.sql(SINK).await.unwrap();
    let error = match session.sql("START;").await {
        Err(error) => error,
        Ok(_) => panic!("a retracting plan must be rejected before writes"),
    };
    assert!(
        matches!(
            error,
            SqlError::Unsupported(ref reason) if reason.contains("cannot apply retractions")
        ),
        "{error:?}"
    );
    assert_eq!(
        effects
            .sink_creates
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        effects
            .sink_writes
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        effects
            .source_reads
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn closed_window_writes_one_append_to_append_only_sink() {
    let sink_batches = Arc::new(Mutex::new(Vec::new()));
    let data = vec![source_batch(&[1, 1, 1], &[1_000, 2_000, 12_000])];
    let mut session = session_with(data, &sink_batches, false);
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql(SINK).await.unwrap();

    if let Err(error) = session.sql("START;").await {
        panic!("final window output is append-only: {error}");
    }
    let rows = wait_for_sink(&sink_batches, &[(1, 0, 2)]).await;
    assert_eq!(rows, vec![(1, 0, 2)]);

    {
        let batches = sink_batches.lock().unwrap();
        let diffs: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .diff
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(diffs, vec![1]);
    }
    session.shutdown().await.unwrap();
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
