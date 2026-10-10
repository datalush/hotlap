//! A sink rejected before any writer opens must not consume the checkpoint
//! config, so a retry still starts durably (or is refused for the right reason).

#[path = "sql_session_rejected_start/harness.rs"]
mod harness;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use harness::{CountedFactory, ToggleFactory, compatible_backend};
use hotlap::{AggSpec, InputId, Plan};
use hotlap_runtime::SqlSession;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_sql::SqlError;

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";
const GROUP_VIEW: &str = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src GROUP BY k;";
const WINDOW_VIEW: &str = "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src \
     GROUP BY k, tumble(_event_time, INTERVAL '10 s');";
const SINK: &str = "CREATE SINK out WITH (connector='inmem') AS SELECT * FROM mv;";

fn grouped_plan() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    }
}

fn window_plan() -> Plan {
    Plan::TumbleCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        time_col: 1,
        size: 10_000,
    }
}

fn session(
    reads: Arc<AtomicU32>,
    starts: Arc<Mutex<Vec<i64>>>,
    creates: Arc<AtomicU32>,
    accepts: Arc<AtomicBool>,
    writes: Arc<AtomicU32>,
) -> SqlSession {
    SqlSession::open_with_factories(
        Arc::new(CountedFactory { reads, starts }),
        Arc::new(ToggleFactory {
            creates,
            accepts,
            writes,
        }),
    )
}

#[tokio::test]
async fn a_rejected_sink_does_not_consume_the_checkpoint_config() {
    let reads = Arc::new(AtomicU32::new(0));
    let starts = Arc::new(Mutex::new(Vec::new()));
    let creates = Arc::new(AtomicU32::new(0));
    let accepts = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU32::new(0));
    let mut session = session(
        Arc::clone(&reads),
        Arc::clone(&starts),
        Arc::clone(&creates),
        Arc::clone(&accepts),
        Arc::clone(&writes),
    )
    .with_checkpoint(CheckpointConfig {
        interval: Duration::from_secs(3600),
        backend: Box::new(compatible_backend(grouped_plan()).await),
        retain: DEFAULT_RETAIN,
    });
    session.sql(SOURCE).await.unwrap();
    session.sql(GROUP_VIEW).await.unwrap();
    session.sql(SINK).await.unwrap();

    let error = match session.sql("START;").await {
        Err(error) => error,
        Ok(_) => panic!("append-only sink must reject a retracting aggregate"),
    };
    assert!(
        matches!(
            error,
            SqlError::Unsupported(ref reason) if reason.contains("cannot apply retractions")
        ),
        "{error:?}"
    );
    assert_eq!(
        creates.load(Ordering::SeqCst),
        0,
        "factory must not create a sink"
    );
    assert_eq!(writes.load(Ordering::SeqCst), 0, "sink must not write");
    assert_eq!(reads.load(Ordering::SeqCst), 0, "source must not be read");
    assert!(starts.lock().unwrap().is_empty());

    // The valid checkpoint must survive preflight rejection and recover on retry.
    accepts.store(true, Ordering::SeqCst);
    session.sql("START;").await.unwrap();
    session.shutdown().await.unwrap();
    assert_eq!(creates.load(Ordering::SeqCst), 1);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(*starts.lock().unwrap(), vec![7]);
}

#[tokio::test]
async fn append_only_window_recovers_its_matching_checkpoint() {
    let reads = Arc::new(AtomicU32::new(0));
    let starts = Arc::new(Mutex::new(Vec::new()));
    let creates = Arc::new(AtomicU32::new(0));
    let accepts = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU32::new(0));
    let backend = compatible_backend(window_plan()).await;
    let mut session = session(
        reads.clone(),
        starts.clone(),
        Arc::clone(&creates),
        accepts,
        writes,
    )
    .with_checkpoint(CheckpointConfig {
        interval: Duration::from_secs(3600),
        backend: Box::new(backend),
        retain: DEFAULT_RETAIN,
    });
    session.sql(SOURCE).await.unwrap();
    session.sql(WINDOW_VIEW).await.unwrap();
    session.sql(SINK).await.unwrap();

    session.sql("START;").await.unwrap();
    session.shutdown().await.unwrap();
    assert_eq!(creates.load(Ordering::SeqCst), 1);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(*starts.lock().unwrap(), vec![7]);
}
