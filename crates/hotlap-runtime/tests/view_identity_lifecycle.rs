//! Public lifecycle: a view created after `START` is captured in the checkpoint
//! registry, so a restart must redeclare it identically or be rejected before
//! any writer, source read or commit.

#[path = "common/backend.rs"]
mod backend;
#[path = "view_identity_lifecycle/factories.rs"]
mod factories;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::{FlussSinkFactory, QueryResult, SqlSession};
use hotlap_sql::SqlError;

use backend::SharedBackend;
use factories::{CountingFactory, CountingSinkFactory};

const SOURCE: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     k AS k - INTERVAL '1 s';";
const VIEW: &str = "CREATE MATERIALIZED VIEW mv AS SELECT k FROM src WHERE k = 1;";
const LATE: &str = "CREATE MATERIALIZED VIEW late AS SELECT k FROM src WHERE k = 2;";
const SINK: &str = "CREATE SINK out WITH (connector='inmem') AS SELECT * FROM mv;";

fn spec(backend: &SharedBackend) -> CheckpointConfig {
    CheckpointConfig {
        interval: Duration::from_millis(25),
        backend: Box::new(backend.clone()),
        retain: DEFAULT_RETAIN,
    }
}

fn ints(result: QueryResult) -> Vec<i64> {
    let QueryResult::Rows(batches) = result else {
        panic!("expected a result set");
    };
    let mut out = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            out.push(column.value(row));
        }
    }
    out.sort_unstable();
    out
}

async fn wait_ints(session: &mut SqlSession, view: &str, want: &[i64]) -> Vec<i64> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let query = format!("SELECT k FROM {view} ORDER BY k");
    loop {
        let got = ints(session.sql(&query).await.expect("query"));
        if got == want || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until a durable checkpoint's registry names `view`.
async fn wait_registered(backend: &SharedBackend, view: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(checkpoint) = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
            .newest_valid()
            .unwrap()
            && checkpoint
                .sources
                .views
                .iter()
                .any(|saved| saved.name == view)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "checkpoint never registered `{view}`"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Run 1: declare `mv`, start, add `late` after `START`, then checkpoint.
async fn run_with_late(backend: &SharedBackend) {
    let mut session = SqlSession::open_with_factories(
        Arc::new(CountingFactory {
            reads: Arc::new(AtomicU32::new(0)),
        }),
        Arc::new(FlussSinkFactory),
    )
    .with_input_retention(16)
    .with_checkpoint(spec(backend));
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql("START;").await.unwrap();
    assert_eq!(wait_ints(&mut session, "mv", &[1]).await, vec![1]);
    session.sql(LATE).await.unwrap();
    assert_eq!(wait_ints(&mut session, "late", &[2]).await, vec![2]);
    wait_registered(backend, "late").await;
    session.shutdown().await.unwrap();
}

/// Run 2: omit the late view and declare a sink; the registry must reject it
/// before the factory or source is touched.
async fn run_without_late(backend: &SharedBackend) -> (SqlError, u32, u32) {
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let mut session = SqlSession::open_with_factories(
        Arc::new(CountingFactory {
            reads: Arc::clone(&reads),
        }),
        Arc::new(CountingSinkFactory {
            creates: Arc::clone(&creates),
        }),
    )
    .with_checkpoint(spec(backend));
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql(SINK).await.unwrap();
    let error = match session.sql("START;").await {
        Ok(_) => panic!("an omitted late view must be rejected"),
        Err(error) => error,
    };
    (
        error,
        creates.load(Ordering::SeqCst),
        reads.load(Ordering::SeqCst),
    )
}

/// Run 3: redeclare the complete namespace identically and read both views.
async fn run_with_complete(backend: &SharedBackend) {
    let mut session = SqlSession::open_with_factories(
        Arc::new(CountingFactory {
            reads: Arc::new(AtomicU32::new(0)),
        }),
        Arc::new(FlussSinkFactory),
    )
    .with_checkpoint(spec(backend));
    session.sql(SOURCE).await.unwrap();
    session.sql(VIEW).await.unwrap();
    session.sql(LATE).await.unwrap();
    session.sql("START;").await.unwrap();
    assert_eq!(wait_ints(&mut session, "mv", &[1]).await, vec![1]);
    assert_eq!(wait_ints(&mut session, "late", &[2]).await, vec![2]);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_late_view_must_be_redeclared_identical_on_restart() {
    let backend = SharedBackend::default();
    tokio::time::timeout(Duration::from_secs(30), async {
        run_with_late(&backend).await;
        let (error, creates, reads) = run_without_late(&backend).await;
        assert!(
            matches!(error, SqlError::Unsupported(_)),
            "omitting `late` must reject: {error}"
        );
        assert_eq!(creates, 0, "no sink writer may open");
        assert_eq!(reads, 0, "no source may be read");
        run_with_complete(&backend).await;
    })
    .await
    .expect("the late-view lifecycle must finish within the timeout");
}
