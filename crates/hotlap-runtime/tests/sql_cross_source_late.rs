//! `SqlSession` post-`START` views over two sources and input retention.

#[path = "cross_source_support/factory.rs"]
mod factory;
#[path = "cross_source_support/session.rs"]
mod session;
#[path = "cross_source_support/sql_source.rs"]
mod sql_source;

use std::sync::Arc;

use hotlap_runtime::{FlussSinkFactory, SqlSession};

use factory::{CrossSourceFactory, SourceSpec};
use session::{batch, ints, schema_a, schema_b, send, wait_acks, wait_pairs};

const CREATE_A: &str = "CREATE SOURCE a WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const CREATE_B: &str = "CREATE SOURCE b WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const LATE_JOIN: &str = "CREATE MATERIALIZED VIEW latej AS SELECT a.k, b.value \
     FROM a JOIN b ON a.k = b.k;";

async fn started(factory: &Arc<CrossSourceFactory>, retention: bool) -> SqlSession {
    let factory = factory.clone();
    let mut session = SqlSession::open_with_factories(factory.clone(), Arc::new(FlussSinkFactory));
    if retention {
        session = session.with_input_retention(16);
    }
    session.sql(CREATE_A).await.unwrap();
    session.sql(CREATE_B).await.unwrap();
    session.sql("START;").await.unwrap();
    send(
        &factory,
        "a",
        batch(schema_a(), vec![ints(&[1]), ints(&[1000])]),
    );
    send(
        &factory,
        "b",
        batch(schema_b(), vec![ints(&[1]), ints(&[20]), ints(&[1000])]),
    );
    assert!(
        wait_acks(&factory, &["a", "b"]).await,
        "both sources must ack"
    );
    session
}

#[tokio::test]
async fn late_view_replays_retained_inputs_from_both_sources() {
    let factory = CrossSourceFactory::new();
    factory.declare("a", SourceSpec::new(schema_a(), 1));
    factory.declare("b", SourceSpec::new(schema_b(), 2));
    let mut session = started(&factory, true).await;

    session
        .sql(LATE_JOIN)
        .await
        .expect("post-start view with retention");
    assert_eq!(
        wait_pairs(&mut session, "SELECT k, value FROM latej", &[(1, 20)]).await,
        vec![(1, 20)]
    );
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn late_view_without_retention_is_rejected() {
    let factory = CrossSourceFactory::new();
    factory.declare("a", SourceSpec::new(schema_a(), 1));
    factory.declare("b", SourceSpec::new(schema_b(), 2));
    let mut session = started(&factory, false).await;

    let late = session.sql(LATE_JOIN).await;
    assert!(
        matches!(late, Err(hotlap_sql::SqlError::Unsupported(_))),
        "a post-start view without retention must be rejected, not partial"
    );
    session.shutdown().await.unwrap();
}
