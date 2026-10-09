//! SQL bag semantics: a consolidated snapshot weight expands into that many
//! rows, cross-checked against an independent DataFusion `VALUES` oracle.

#[path = "cross_source_support/factory.rs"]
mod factory;
#[path = "cross_source_support/sql_source.rs"]
mod sql_source;
#[path = "cross_source_support/mod.rs"]
mod support;

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::SchemaRef;
use hotlap_connectors::source::SourceBatch;
use hotlap_runtime::{Session, SessionConfig};

use factory::{CrossSourceFactory, SourceSpec};
use support::rows::{
    append_batch, ints, query_rows, recompute, schema_left, schema_right, send, wait_commits,
    zset_tuples,
};

const CREATE_A: &str = "CREATE SOURCE a WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const CREATE_B: &str = "CREATE SOURCE b WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW j AS SELECT a.k, a.lv, b.rv \
     FROM a JOIN b ON a.k = b.k;";

/// An append batch over `schema` from `(key, value, event_time)` rows.
fn rows(schema: SchemaRef, triples: &[(i64, i64, i64)]) -> SourceBatch {
    let keys: Vec<i64> = triples.iter().map(|row| row.0).collect();
    let values: Vec<i64> = triples.iter().map(|row| row.1).collect();
    let times: Vec<i64> = triples.iter().map(|row| row.2).collect();
    append_batch(schema, vec![ints(&keys), ints(&values), ints(&times)])
}

fn open(factory: &Arc<CrossSourceFactory>) -> Session {
    let mut session = Session::open(SessionConfig::new().with_source_factory(factory.clone()))
        .expect("open session");
    session.sql(CREATE_A).expect("create a");
    session.sql(CREATE_B).expect("create b");
    session.sql(VIEW).expect("create view");
    session.sql("START;").expect("start");
    session
}

/// A `2 x 3` join match must surface six rows and aggregate over all six. The
/// expected aggregates come from a fresh DataFusion context over literal
/// `VALUES`, independent of the view's own conversion.
#[test]
fn join_multiset_expands_to_a_bag_and_matches_values_oracle() {
    let factory = CrossSourceFactory::new();
    factory.declare("a", SourceSpec::new(schema_left(), 2));
    factory.declare("b", SourceSpec::new(schema_right(), 2));
    let mut session = open(&factory);

    send(
        &factory,
        "a",
        rows(schema_left(), &[(1, 10, 0), (1, 10, 0)]),
    );
    send(
        &factory,
        "b",
        rows(schema_right(), &[(1, 20, 0), (1, 20, 0), (1, 20, 0)]),
    );
    assert!(wait_commits(&factory, "a", 1), "a must ack");
    assert!(wait_commits(&factory, "b", 1), "b must ack");

    // The consolidated snapshot keeps one row with weight 2 * 3 = 6, matching
    // an independent recompute of the inserted inputs.
    let snapshot = session.snapshot("j").expect("MV snapshot");
    assert_eq!(
        zset_tuples(&snapshot),
        recompute(&[(1, 10, 2)], &[(1, 20, 3)])
    );

    // SELECT expands the weight into six identical rows; an independent
    // DataFusion `VALUES` query returns the same six rows.
    let select = query_rows(session.sql("SELECT k, lv, rv FROM j").expect("select"));
    assert_eq!(select, vec![vec![1, 10, 20]; 6]);
    let select_oracle = values_oracle(
        "SELECT k, lv, rv FROM (VALUES (1, 10, 20), (1, 10, 20), (1, 10, 20), \
         (1, 10, 20), (1, 10, 20), (1, 10, 20)) AS t(k, lv, rv)",
    );
    assert_eq!(select, select_oracle);

    // Aggregates see the six rows, not one distinct row; the oracle folds the
    // same six literals through DataFusion.
    let aggregate = session
        .sql("SELECT count(*), sum(lv), CAST(avg(rv) AS BIGINT), min(lv), max(rv) FROM j")
        .expect("aggregate");
    let aggregate = query_rows(aggregate);
    assert_eq!(aggregate, vec![vec![6, 60, 20, 10, 20]]);
    let oracle = values_oracle(
        "SELECT count(*), sum(lv), CAST(avg(rv) AS BIGINT), min(lv), max(rv) FROM \
         (VALUES (1, 10, 20), (1, 10, 20), (1, 10, 20), (1, 10, 20), (1, 10, 20), (1, 10, 20)) \
         AS t(k, lv, rv)",
    );
    assert_eq!(aggregate, oracle);
    session.shutdown().expect("shutdown");
}

/// Run `sql` in a fresh DataFusion context and read the sorted integer rows.
fn values_oracle(sql: &str) -> Vec<Vec<i64>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("oracle runtime");
    runtime.block_on(async {
        let ctx = datafusion::prelude::SessionContext::new();
        let batches = ctx
            .sql(sql)
            .await
            .expect("oracle plan")
            .collect()
            .await
            .expect("oracle run");
        let mut rows = Vec::new();
        for batch in &batches {
            let columns: Vec<&Int64Array> = batch
                .columns()
                .iter()
                .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
                .collect();
            for row in 0..batch.num_rows() {
                rows.push(columns.iter().map(|column| column.value(row)).collect());
            }
        }
        rows.sort_unstable();
        rows
    })
}
