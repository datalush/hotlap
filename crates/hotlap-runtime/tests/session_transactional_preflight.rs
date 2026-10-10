#![allow(clippy::duplicate_mod)]

#[allow(dead_code)]
#[path = "common/backend.rs"]
mod backend;
#[path = "common/session_transactional_fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "checkpoint_abort_staged/support.rs"]
mod staged;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use fixture::{
    SessionSignals, TestBackend, config, corrupt_pending, declare, remove_pending, rows, seed,
    wait_for_source_read,
};
use hotlap::state::StateBackend;
use hotlap_connectors::sink::Sink;
use hotlap_runtime::{Session, SessionError, SinkFactory};
use hotlap_sql::SqlError as FactorySqlError;
use hotlap_sql::SqlError;

#[test]
fn transactional_pending_retry_restores_and_publishes_a_new_checkpoint() {
    let backend = TestBackend::default();
    seed(&backend);
    corrupt_pending(&backend);
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let spies = Arc::new(Mutex::new(Vec::new()));
    let (source_read, output) = std::sync::mpsc::channel();
    let mut session = Session::open(config(
        &backend,
        reads.clone(),
        creates.clone(),
        true,
        Arc::clone(&spies),
        SessionSignals {
            source_read_ack: Some(source_read),
            ..SessionSignals::default()
        },
    ))
    .unwrap();
    declare(&mut session);

    let error = match session.sql("START;") {
        Ok(_) => panic!("unsafe corrupt pending state must be rejected"),
        Err(error) => error,
    };
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert!(matches!(error, SessionError::Sql(SqlError::Unsupported(_))));
    assert!(backend.get(b"checkpoint/1/valid").unwrap().is_some());

    remove_pending(&backend);
    session
        .sql("START;")
        .expect("same-session retry retains config");
    assert_eq!(rows(&session), vec![vec![1]]);
    wait_for_source_read(output);
    let probes = spies.lock().unwrap();
    assert_eq!(probes[0].resumed(), 1);
    assert_eq!(probes[0].offset(), Some(1));
    assert_eq!(session.checkpoint().unwrap(), 2);
    assert_eq!(
        backend.get(b"checkpoint/2/valid").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(
        backend.get(b"checkpoint/reserved").unwrap(),
        Some(2_u64.to_le_bytes().to_vec())
    );
    drop(probes);
    session.shutdown().expect("shutdown retry");
}

#[test]
fn an_actual_transactional_sink_cannot_contradict_preflight() {
    let backend = TestBackend::default();
    seed(&backend);
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let mut session = Session::open(config(
        &backend,
        reads.clone(),
        creates.clone(),
        false,
        Arc::new(Mutex::new(Vec::new())),
        SessionSignals::default(),
    ))
    .unwrap();
    declare(&mut session);

    let error = match session.sql("START;") {
        Ok(_) => panic!("a transactional sink contradicting preflight must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, SessionError::Sql(SqlError::Unsupported(_))));
    assert_eq!(creates.load(Ordering::SeqCst), 2);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
}

#[test]
fn invalid_prepare_rejects_session_before_sink_factory_or_source_read() {
    let backend = TestBackend::default();
    seed(&backend);
    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let body = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), body)
            .unwrap();
    }
    writer
        .put(b"checkpoint/2/prepare", b"unknown-phase".to_vec())
        .unwrap();
    let body_engine = backend.get(b"checkpoint/2/engine").unwrap();
    let body_sources = backend.get(b"checkpoint/2/sources").unwrap();
    let reads = Arc::new(AtomicU32::new(0));
    let creates = Arc::new(AtomicU32::new(0));
    let mut session = Session::open(config(
        &backend,
        reads.clone(),
        creates.clone(),
        true,
        Arc::new(Mutex::new(Vec::new())),
        SessionSignals::default(),
    ))
    .unwrap();
    declare(&mut session);

    let error = match session.sql("START;") {
        Ok(_) => panic!("an unknown durable phase must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, SessionError::Sql(SqlError::Unsupported(_))));
    assert_eq!(
        creates.load(Ordering::SeqCst),
        0,
        "sink factories must not run"
    );
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "source reads must not start"
    );
    assert_eq!(
        backend.get(b"checkpoint/2/prepare").unwrap(),
        Some(b"unknown-phase".to_vec())
    );
    assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), None);
    assert_eq!(backend.get(b"checkpoint/2/valid").unwrap(), None);
    assert_eq!(backend.get(b"checkpoint/2/engine").unwrap(), body_engine);
    assert_eq!(backend.get(b"checkpoint/2/sources").unwrap(), body_sources);
    session.shutdown().unwrap();
}

struct StagedSinkFactory {
    remote: Arc<staged::Remote>,
    creates: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl SinkFactory for StagedSinkFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &std::collections::BTreeMap<String, String>,
        _schema: arrow::datatypes::SchemaRef,
    ) -> Result<Arc<dyn Sink>, FactorySqlError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        Ok(staged::PersistentTxn::new(self.remote.clone(), false, true))
    }

    fn may_create_transactional(
        &self,
        _options: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        true
    }
}

#[test]
fn invalid_commit_marker_preserves_real_staged_payload_before_session_factory_effects() {
    let backend = TestBackend::default();
    seed(&backend);
    let remote = Arc::new(staged::Remote::default());
    let (engine, changes) = staged::engine_with_changes();
    let staged_writer = staged::PersistentTxn::new(remote.clone(), false, true);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            staged::write_changes(&staged_writer, changes).await;
            staged_writer.prepare().await.unwrap();
        });
    drop(engine);
    let staged_before = remote.staged();
    assert_eq!(
        staged_before,
        vec![(vec![7], 2), (vec![8], 1), (vec![8], -1)]
    );

    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let body = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), body)
            .unwrap();
    }
    writer.delete(b"checkpoint/2/prepare").unwrap();
    writer.put(b"checkpoint/2/commit", b"0".to_vec()).unwrap();
    let body_engine = backend.get(b"checkpoint/2/engine").unwrap();
    let body_sources = backend.get(b"checkpoint/2/sources").unwrap();

    let reads = Arc::new(AtomicU32::new(0));
    let factory_creates = Arc::new(AtomicU32::new(0));
    let mut session = Session::open(
        config(
            &backend,
            reads.clone(),
            Arc::new(AtomicU32::new(0)),
            true,
            Arc::new(Mutex::new(Vec::new())),
            SessionSignals::default(),
        )
        .with_sink_factory(Arc::new(StagedSinkFactory {
            remote: remote.clone(),
            creates: factory_creates.clone(),
        })),
    )
    .unwrap();
    declare(&mut session);

    let error = match session.sql("START;") {
        Ok(_) => panic!("noncanonical commit marker must reject before effects"),
        Err(error) => error,
    };
    assert!(matches!(error, SessionError::Sql(SqlError::Unsupported(_))));
    assert_eq!(factory_creates.load(Ordering::SeqCst), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(remote.commit_calls(), 0);
    assert_eq!(remote.staged(), staged_before);
    assert!(remote.committed().is_empty());
    assert_eq!(
        backend.get(b"checkpoint/2/commit").unwrap(),
        Some(b"0".to_vec())
    );
    assert_eq!(backend.get(b"checkpoint/2/valid").unwrap(), None);
    assert_eq!(backend.get(b"checkpoint/2/engine").unwrap(), body_engine);
    assert_eq!(backend.get(b"checkpoint/2/sources").unwrap(), body_sources);
    session.shutdown().unwrap();
}
