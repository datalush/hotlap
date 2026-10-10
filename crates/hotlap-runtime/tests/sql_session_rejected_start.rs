//! A sink rejected before any writer opens must not consume the checkpoint
//! config, so a retry still starts durably (or is refused for the right reason).

#[path = "sql_session_rejected_start/harness.rs"]
mod harness;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use harness::{CountedFactory, IdentityFactory, ToggleFactory, compatible_backend};
use hotlap::state::StateBackend;
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
const TARGET: &str = "test/sql-session-rejected-start/output-store";

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

fn identity_session(
    backend: &harness::SharedBackend,
    reads: Arc<AtomicU32>,
    starts: Arc<Mutex<Vec<i64>>>,
    creates: Arc<AtomicU32>,
    writes: Arc<AtomicU32>,
    described: Arc<Mutex<Option<String>>>,
    actual: Arc<Mutex<String>>,
) -> SqlSession {
    SqlSession::open_with_factories(
        Arc::new(CountedFactory { reads, starts }),
        Arc::new(IdentityFactory {
            creates,
            writes,
            described,
            actual,
        }),
    )
    .with_checkpoint(CheckpointConfig {
        interval: Duration::from_secs(3600),
        backend: Box::new(backend.clone()),
        retain: DEFAULT_RETAIN,
    })
}

async fn declare_window(session: &mut SqlSession) {
    session.sql(SOURCE).await.unwrap();
    session.sql(WINDOW_VIEW).await.unwrap();
    session.sql(SINK).await.unwrap();
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

    // Changing the factory's declared sink behavior is a participant change,
    // so the same checkpoint cannot be retried under the new sink contract.
    accepts.store(true, Ordering::SeqCst);
    let retry = match session.sql("START;").await {
        Err(error) => error,
        Ok(_) => panic!("changed sink contract must be rejected"),
    };
    assert!(retry.to_string().contains("sink descriptions"), "{retry}");
    session.shutdown().await.unwrap();
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert!(starts.lock().unwrap().is_empty());
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

#[tokio::test]
async fn durable_start_without_metadata_description_never_creates_a_writer() {
    let backend = compatible_backend(window_plan()).await;
    let reads = Arc::new(AtomicU32::new(0));
    let starts = Arc::new(Mutex::new(Vec::new()));
    let creates = Arc::new(AtomicU32::new(0));
    let writes = Arc::new(AtomicU32::new(0));
    let described = Arc::new(Mutex::new(None));
    let actual = Arc::new(Mutex::new(TARGET.to_owned()));
    let mut session = identity_session(
        &backend,
        Arc::clone(&reads),
        Arc::clone(&starts),
        Arc::clone(&creates),
        Arc::clone(&writes),
        Arc::clone(&described),
        Arc::clone(&actual),
    );
    declare_window(&mut session).await;

    let error = match session.sql("START;").await {
        Err(error) => error,
        Ok(_) => panic!("description required"),
    };
    assert!(matches!(error, SqlError::Unsupported(_)));
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert!(starts.lock().unwrap().is_empty());

    *described.lock().unwrap() = Some(TARGET.to_owned());
    session
        .sql("START;")
        .await
        .expect("durable config retained");
    let (session, checkpoint) = tokio::task::spawn_blocking(move || {
        let checkpoint = session.checkpoint();
        (session, checkpoint)
    })
    .await
    .unwrap();
    assert_eq!(checkpoint.unwrap(), 2);
    assert_eq!(*starts.lock().unwrap(), vec![7]);
    assert_eq!(creates.load(Ordering::SeqCst), 1);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_declared_target_different_from_hlpm_is_rejected_before_create() {
    let backend = compatible_backend(window_plan()).await;
    let reads = Arc::new(AtomicU32::new(0));
    let starts = Arc::new(Mutex::new(Vec::new()));
    let creates = Arc::new(AtomicU32::new(0));
    let writes = Arc::new(AtomicU32::new(0));
    let described = Arc::new(Mutex::new(Some("test/other/target".to_owned())));
    let actual = Arc::new(Mutex::new("test/other/target".to_owned()));
    let mut session = identity_session(
        &backend,
        Arc::clone(&reads),
        Arc::clone(&starts),
        Arc::clone(&creates),
        Arc::clone(&writes),
        described,
        actual,
    );
    declare_window(&mut session).await;

    let error = match session.sql("START;").await {
        Err(error) => error,
        Ok(_) => panic!("old target mismatch"),
    };

    assert!(matches!(error, SqlError::Unsupported(_)));
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert!(starts.lock().unwrap().is_empty());
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn actual_target_mismatch_rejects_before_reads_and_same_session_retry_recovers() {
    let backend = compatible_backend(window_plan()).await;
    let reads = Arc::new(AtomicU32::new(0));
    let starts = Arc::new(Mutex::new(Vec::new()));
    let creates = Arc::new(AtomicU32::new(0));
    let writes = Arc::new(AtomicU32::new(0));
    let described = Arc::new(Mutex::new(Some(TARGET.to_owned())));
    let actual = Arc::new(Mutex::new("test/other/actual-target".to_owned()));
    let mut session = identity_session(
        &backend,
        Arc::clone(&reads),
        Arc::clone(&starts),
        Arc::clone(&creates),
        Arc::clone(&writes),
        Arc::clone(&described),
        Arc::clone(&actual),
    );
    declare_window(&mut session).await;

    let error = match session.sql("START;").await {
        Err(error) => error,
        Ok(_) => panic!("actual target mismatch"),
    };

    assert!(matches!(error, SqlError::Unsupported(_)));
    assert_eq!(creates.load(Ordering::SeqCst), 1);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert!(starts.lock().unwrap().is_empty());

    *actual.lock().unwrap() = TARGET.to_owned();
    session
        .sql("START;")
        .await
        .expect("same-session retry is durable");
    let (session, immediate) = tokio::task::spawn_blocking(move || {
        let snapshot = session.snapshot("mv");
        (session, snapshot)
    })
    .await
    .unwrap();
    let immediate = immediate.unwrap();
    assert!(
        immediate.is_empty(),
        "restored checkpoint output is preserved"
    );
    assert_eq!(*starts.lock().unwrap(), vec![7]);
    let (session, checkpoint) = tokio::task::spawn_blocking(move || {
        let checkpoint = session.checkpoint();
        (session, checkpoint)
    })
    .await
    .unwrap();
    assert_eq!(checkpoint.unwrap(), 2);
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
    assert_eq!(creates.load(Ordering::SeqCst), 2);
    session.shutdown().await.unwrap();
}
