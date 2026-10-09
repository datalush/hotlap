//! SQL session oracle: two append-only sources compared to full recomputation.
//!
//! After every ack the raw MV snapshot (which keeps multiplicities) is compared
//! against an independent recompute of the appended rows. Retractions are not
//! simulated here: an append-only `SourceBatch` carries positive diffs only.

#[path = "cross_source_support/factory.rs"]
mod factory;
#[path = "cross_source_support/sql_source.rs"]
mod sql_source;
#[path = "cross_source_support/mod.rs"]
mod support;

use std::sync::Arc;

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

/// Accumulates the oracle inputs and checks the snapshot after every append.
#[derive(Default)]
struct Oracle {
    left: Vec<(i64, i64, i64)>,
    right: Vec<(i64, i64, i64)>,
    a_acks: usize,
    b_acks: usize,
}

impl Oracle {
    fn push_left(
        &mut self,
        session: &Session,
        factory: &CrossSourceFactory,
        triples: &[(i64, i64, i64)],
        diff: i64,
    ) {
        send(factory, "a", rows(schema_left(), triples));
        self.a_acks += 1;
        assert!(wait_commits(factory, "a", self.a_acks), "a must ack");
        self.left.push((triples[0].0, triples[0].1, diff));
        self.snapshot(session);
    }

    fn push_right(
        &mut self,
        session: &Session,
        factory: &CrossSourceFactory,
        triples: &[(i64, i64, i64)],
        diff: i64,
    ) {
        send(factory, "b", rows(schema_right(), triples));
        self.b_acks += 1;
        assert!(wait_commits(factory, "b", self.b_acks), "b must ack");
        self.right.push((triples[0].0, triples[0].1, diff));
        self.snapshot(session);
    }

    fn snapshot(&self, session: &Session) {
        let snapshot = session.snapshot("j").expect("MV snapshot");
        assert_eq!(zset_tuples(&snapshot), recompute(&self.left, &self.right));
    }
}

#[test]
fn sql_join_matches_recomputation_after_every_append() {
    let factory = CrossSourceFactory::new();
    factory.declare("a", SourceSpec::new(schema_left(), 2));
    factory.declare("b", SourceSpec::new(schema_right(), 2));
    let mut session = open(&factory);
    let mut oracle = Oracle::default();

    // Duplicates on both sides: the join multiplies 2 * 3.
    oracle.push_left(&session, &factory, &[(1, 10, 0), (1, 10, 0)], 2);
    oracle.push_right(&session, &factory, &[(1, 20, 0), (1, 20, 0), (1, 20, 0)], 3);
    // A new key on one side only must not match until the other side arrives.
    oracle.push_left(&session, &factory, &[(2, 40, 0)], 1);
    oracle.push_right(&session, &factory, &[(2, 30, 0)], 1);
    // Keys with no match on the other source contribute nothing.
    oracle.push_left(&session, &factory, &[(3, 50, 0)], 1);
    oracle.push_right(&session, &factory, &[(4, 60, 0)], 1);

    // The SQL surface expands multiplicities: key 1 occurs 2 * 3 = 6 times and
    // key 2 once, so `SELECT` returns seven bag rows (no implicit DISTINCT).
    let select = session.sql("SELECT k, lv, rv FROM j").expect("select");
    let mut expected = vec![vec![1, 10, 20]; 6];
    expected.push(vec![2, 40, 30]);
    assert_eq!(query_rows(select), expected);
    session.shutdown().expect("shutdown");
}

#[test]
fn no_match_keys_leave_the_join_empty() {
    let factory = CrossSourceFactory::new();
    factory.declare("a", SourceSpec::new(schema_left(), 2));
    factory.declare("b", SourceSpec::new(schema_right(), 2));
    let session = open(&factory);

    send(&factory, "a", rows(schema_left(), &[(1, 10, 0)]));
    send(&factory, "b", rows(schema_right(), &[(2, 20, 0)]));
    assert!(wait_commits(&factory, "a", 1), "a must ack");
    assert!(wait_commits(&factory, "b", 1), "b must ack");

    let snapshot = session.snapshot("j").expect("MV snapshot");
    assert!(zset_tuples(&snapshot).is_empty(), "no equal keys");
    session.shutdown().expect("shutdown");
}
