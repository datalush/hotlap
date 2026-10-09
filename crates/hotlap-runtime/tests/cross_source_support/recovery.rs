//! Checkpoint/restart helpers and key oracle for the T6 recovery test.
//!
//! It takes the caller's source factory and backend so the test reuses the
//! existing resumable source fixture instead of duplicating one, and folds the
//! key rows itself so the expected join is independent of the engine.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use hotlap::ZSetBatch;
use hotlap::state::StateBackend;
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::{Session, SessionConfig, SourceFactory};

const CREATE_A: &str = "CREATE SOURCE a WITH (connector='inmem') WATERMARK FOR \
     k AS k - INTERVAL '1 s';";
const CREATE_B: &str = "CREATE SOURCE b WITH (connector='inmem') WATERMARK FOR \
     k AS k - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW j AS SELECT a.k FROM a JOIN b ON a.k = b.k;";

/// The order the session issues `CREATE SOURCE`, independent of the factory's
/// declaration order.
#[derive(Clone, Copy)]
pub enum Order {
    /// `CREATE SOURCE a` then `b`.
    Ab,
    /// `CREATE SOURCE b` then `a`.
    Ba,
}

/// Open a checkpointed session and declare `a`, `b` and the join view.
pub fn session(
    factory: Arc<dyn SourceFactory>,
    backend: Box<dyn StateBackend + Send>,
    order: Order,
) -> Session {
    let config = SessionConfig::new()
        .with_source_factory(factory)
        .with_checkpoint(Duration::from_secs(3600), DEFAULT_RETAIN, backend);
    let mut session = Session::open(config).expect("open session");
    match order {
        Order::Ab => {
            session.sql(CREATE_A).expect("create a");
            session.sql(CREATE_B).expect("create b");
        }
        Order::Ba => {
            session.sql(CREATE_B).expect("create b");
            session.sql(CREATE_A).expect("create a");
        }
    }
    session.sql(VIEW).expect("create view");
    session
}

/// Full recompute of a key-only join; rows are `(k, diff)`.
pub fn recompute_keys(left: &[(i64, i64)], right: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut rows: BTreeMap<i64, i64> = BTreeMap::new();
    for &(lk, ld) in left {
        for &(rk, rd) in right {
            if lk == rk {
                *rows.entry(lk).or_insert(0) += ld * rd;
            }
        }
    }
    rows.into_iter().filter(|(_, diff)| *diff != 0).collect()
}

/// A key-only MV snapshot as sorted `(k, diff)` tuples.
pub fn zset_keys(zset: &ZSetBatch) -> Vec<(i64, i64)> {
    if zset.is_empty() {
        return Vec::new();
    }
    let keys = zset
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = zset.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..zset.len())
        .map(|row| (keys.value(row), diffs.value(row)))
        .collect()
}
