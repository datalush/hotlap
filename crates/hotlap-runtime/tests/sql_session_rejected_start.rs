//! A sink rejected before any writer opens must not consume the checkpoint
//! config, so a retry still starts durably (or is refused for the right reason).

#[path = "sql_session_rejected_start/harness.rs"]
mod harness;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use harness::{CountedFactory, ToggleFactory, mismatched_backend};
use hotlap_runtime::SqlSession;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_sql::SqlError;

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src \
     GROUP BY k, tumble(_event_time, INTERVAL '10 s');";
const SINK: &str = "CREATE SINK out WITH (connector='inmem') AS SELECT * FROM mv;";

fn session(reads: Arc<AtomicU32>, creates: Arc<AtomicU32>, accepts: Arc<AtomicBool>) -> SqlSession {
    SqlSession::open_with_factories(
        Arc::new(CountedFactory { reads }),
        Arc::new(ToggleFactory { creates, accepts }),
    )
}

#[tokio::test]
async fn a_rejected_sink_does_not_consume_the_checkpoint_config() {
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let accepts = Arc::new(AtomicBool::new(false));
    let mut session = session(
        Arc::clone(&reads),
        Arc::clone(&creates),
        Arc::clone(&accepts),
    )
    .with_checkpoint(CheckpointConfig {
        interval: Duration::from_secs(3600),
        backend: Box::new(mismatched_backend().await),
        retain: DEFAULT_RETAIN,
    });
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql(SINK).await.unwrap();

    assert!(
        session.sql("START;").await.is_err(),
        "a retracting plan with a non-retracting sink must be refused"
    );
    assert_eq!(creates.load(Ordering::SeqCst), 0, "no writer may open");
    assert_eq!(reads.load(Ordering::SeqCst), 0, "no source may open");

    // Make the sink capable and retry: the checkpoint config must have survived
    // the rejection, so this attempt reaches recovery and fails durably.
    accepts.store(true, Ordering::SeqCst);
    assert!(
        session.sql("START;").await.is_err(),
        "the retry must still start against its checkpoint"
    );
    let error = match session.sql("START;").await {
        Err(error) => error,
        Ok(_) => panic!("a consumed durable start must not be retried cleanly"),
    };
    assert!(
        matches!(error, SqlError::Unsupported(ref message) if message.contains("checkpoint")),
        "the latch must name the consumed checkpoint: {error:?}"
    );
}
