//! End-to-end `SqlSession` tests joining two independent sources.
//!
//! Each test drives real DataFusion planning, the real kernel engine and
//! channel-fed controlled sources; no result is asserted before both sources
//! have been acked.

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
const CREATE_Z: &str = "CREATE SOURCE z WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";

#[tokio::test]
async fn join_across_two_sources_with_distinct_schemas() {
    let factory = CrossSourceFactory::new();
    factory.declare("a", SourceSpec::new(schema_a(), 1));
    factory.declare("b", SourceSpec::new(schema_b(), 2));
    let mut session = SqlSession::open_with_factories(factory.clone(), Arc::new(FlussSinkFactory));

    session.sql(CREATE_A).await.unwrap();
    session.sql(CREATE_B).await.unwrap();
    session
        .sql(
            "CREATE MATERIALIZED VIEW j AS SELECT a.k, b.value \
             FROM a JOIN b ON a.k = b.k;",
        )
        .await
        .unwrap();
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
    assert_eq!(
        wait_pairs(&mut session, "SELECT k, value FROM j", &[(1, 20)]).await,
        vec![(1, 20)]
    );
    session.shutdown().await.unwrap();
}

/// Declares `z`, a view over it, then `a`: the early view must still read `z`
/// even though the final assignment gives `a` the lower id.
#[tokio::test]
async fn early_view_still_reads_its_declared_source() {
    let factory = CrossSourceFactory::new();
    factory.declare("z", SourceSpec::new(schema_a(), 1));
    factory.declare("a", SourceSpec::new(schema_a(), 1));
    let mut session = SqlSession::open_with_factories(factory.clone(), Arc::new(FlussSinkFactory));

    session.sql(CREATE_Z).await.unwrap();
    session
        .sql("CREATE MATERIALIZED VIEW zview AS SELECT k, _event_time FROM z;")
        .await
        .unwrap();
    session.sql(CREATE_A).await.unwrap();
    session
        .sql("CREATE MATERIALIZED VIEW j AS SELECT a.k, z._event_time FROM a JOIN z ON a.k = z.k;")
        .await
        .unwrap();
    session.sql("START;").await.unwrap();

    send(
        &factory,
        "a",
        batch(schema_a(), vec![ints(&[1]), ints(&[1000])]),
    );
    send(
        &factory,
        "z",
        batch(schema_a(), vec![ints(&[1]), ints(&[2000])]),
    );
    assert!(
        wait_acks(&factory, &["a", "z"]).await,
        "both sources must ack"
    );

    // `z` carries event time 2000; reading `a` instead would produce 1000.
    assert_eq!(
        wait_pairs(
            &mut session,
            "SELECT k, _event_time FROM zview",
            &[(1, 2000)]
        )
        .await,
        vec![(1, 2000)]
    );
    assert_eq!(
        wait_pairs(&mut session, "SELECT * FROM j", &[(1, 2000)]).await,
        vec![(1, 2000)]
    );
    session.shutdown().await.unwrap();
}

/// The inverted declaration order must produce the same assignment and result.
#[tokio::test]
async fn inverted_declaration_order_keeps_the_same_assignment() {
    let factory = CrossSourceFactory::new();
    factory.declare("a", SourceSpec::new(schema_a(), 1));
    factory.declare("z", SourceSpec::new(schema_a(), 1));
    let mut session = SqlSession::open_with_factories(factory.clone(), Arc::new(FlussSinkFactory));

    session.sql(CREATE_A).await.unwrap();
    session.sql(CREATE_Z).await.unwrap();
    session
        .sql("CREATE MATERIALIZED VIEW zview AS SELECT k, _event_time FROM z;")
        .await
        .unwrap();
    session
        .sql("CREATE MATERIALIZED VIEW j AS SELECT a.k, z._event_time FROM a JOIN z ON a.k = z.k;")
        .await
        .unwrap();
    session.sql("START;").await.unwrap();

    send(
        &factory,
        "a",
        batch(schema_a(), vec![ints(&[1]), ints(&[1000])]),
    );
    send(
        &factory,
        "z",
        batch(schema_a(), vec![ints(&[1]), ints(&[2000])]),
    );
    assert!(
        wait_acks(&factory, &["a", "z"]).await,
        "both sources must ack"
    );

    assert_eq!(
        wait_pairs(
            &mut session,
            "SELECT k, _event_time FROM zview",
            &[(1, 2000)]
        )
        .await,
        vec![(1, 2000)]
    );
    assert_eq!(
        wait_pairs(&mut session, "SELECT * FROM j", &[(1, 2000)]).await,
        vec![(1, 2000)]
    );
    session.shutdown().await.unwrap();
}
