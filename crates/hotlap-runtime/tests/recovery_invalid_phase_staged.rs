//! Invalid phases cannot publish real staged sink state during recovery.

#[path = "common/backend.rs"]
mod backend;
#[allow(dead_code)]
#[path = "checkpoint_abort_staged/support.rs"]
mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap::state::StateBackend;
use hotlap::{InputId, Plan};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;
use support::{PersistentTxn, Remote};

struct ObservedSource(Arc<AtomicUsize>);

impl Source for ObservedSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/recovery-invalid-phase/source".into())
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

fn sources(reads: Arc<AtomicUsize>) -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(ObservedSource(reads)),
        watermark: None,
    }])
    .unwrap()
}

async fn seed_staged_attempt() -> (SharedBackend, Arc<Remote>, Vec<u8>, Vec<u8>) {
    let backend = SharedBackend::default();
    let remote = Arc::new(Remote::default());
    let (hotlap, changes) = support::engine_with_changes();
    let pipeline = Pipeline {
        sources: sources(Arc::new(AtomicUsize::new(0))),
        views: vec![("v".into(), Plan::Source(InputId(0)))],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let seed_sink = PersistentTxn::new(remote.clone(), false, true);
    let mut seed = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
        SinkSync::sink_only_named(SharedSink::new(seed_sink), "txn".into(), "v".into()),
    ]);
    seed.take(&hotlap, &pipeline.sources).await.unwrap();

    let interrupted_sink = PersistentTxn::failing_prepare(remote.clone(), true, true);
    support::write_changes(&interrupted_sink, changes).await;
    let mut interrupted =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
            SinkSync::sink_only_named(SharedSink::new(interrupted_sink), "txn".into(), "v".into()),
        ]);
    assert!(
        interrupted.take(&hotlap, &pipeline.sources).await.is_err(),
        "injected prepare/abort failure leaves a real staged payload"
    );
    assert_eq!(
        remote.staged(),
        vec![(vec![7], 2), (vec![8], 1), (vec![8], -1)]
    );
    assert!(remote.committed().is_empty());

    let mut writer = backend.clone();
    for part in ["engine", "sources", "participants"] {
        let body = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), body)
            .unwrap();
    }
    let engine = backend.get(b"checkpoint/2/engine").unwrap().unwrap();
    let sources = backend.get(b"checkpoint/2/sources").unwrap().unwrap();
    (backend, remote, engine, sources)
}

fn staged_attempt() -> (SharedBackend, Arc<Remote>, Vec<u8>, Vec<u8>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(seed_staged_attempt())
}

fn recovery_pipeline(
    backend: SharedBackend,
    remote: Arc<Remote>,
    reads: Arc<AtomicUsize>,
) -> Pipeline {
    Pipeline {
        sources: sources(reads),
        views: vec![("v".into(), Plan::Source(InputId(0)))],
        sinks: vec![SinkSpec::named(
            "txn",
            "v",
            PersistentTxn::new(remote, false, true),
        )],
        checkpoint: Some(CheckpointConfig {
            interval: std::time::Duration::from_secs(3600),
            backend: Box::new(backend),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    }
}

#[test]
fn invalid_and_prepare_phases_preserve_staged_payload_before_pipeline_effects() {
    for (prepare, commit) in [
        (Some(Vec::new()), None),
        (Some(b"prepare".to_vec()), None),
        (Some(b"unknown-phase".to_vec()), None),
        (Some(b"prepare-v1".to_vec()), None),
        (None, Some(b"0".to_vec())),
    ] {
        let (backend, remote, body_engine, body_sources) = staged_attempt();
        let commits_before_recovery = remote.commit_calls();
        let mut writer = backend.clone();
        match &prepare {
            Some(marker) => writer.put(b"checkpoint/2/prepare", marker.clone()).unwrap(),
            None => writer.delete(b"checkpoint/2/prepare").unwrap(),
        }
        if let Some(marker) = &commit {
            writer.put(b"checkpoint/2/commit", marker.clone()).unwrap();
        }
        let reads = Arc::new(AtomicUsize::new(0));

        let error = match EngineHandle::start(recovery_pipeline(
            backend.clone(),
            remote.clone(),
            reads.clone(),
        )) {
            Ok(_) => panic!("unknown phase and transactional prepare must reject preflight"),
            Err(error) => error,
        };

        assert!(
            matches!(error, ConnectorError::Unsupported(_)),
            "got {error:?}"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "source read must not start"
        );
        assert_eq!(
            remote.staged(),
            vec![(vec![7], 2), (vec![8], 1), (vec![8], -1)]
        );
        assert!(remote.committed().is_empty(), "no new writer may commit");
        assert_eq!(remote.commit_calls(), commits_before_recovery);
        assert_eq!(backend.get(b"checkpoint/2/valid").unwrap(), None);
        assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), commit);
        assert_eq!(backend.get(b"checkpoint/2/prepare").unwrap(), prepare);
        assert_eq!(
            backend.get(b"checkpoint/2/engine").unwrap(),
            Some(body_engine)
        );
        assert_eq!(
            backend.get(b"checkpoint/2/sources").unwrap(),
            Some(body_sources)
        );
    }
}

#[test]
fn exact_commit_marker_redrives_and_publishes_staged_payload_once() {
    let (backend, remote, _, _) = staged_attempt();
    let commits_before_recovery = remote.commit_calls();
    let mut writer = backend.clone();
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
    let handle = EngineHandle::start(recovery_pipeline(
        backend.clone(),
        remote.clone(),
        Arc::new(AtomicUsize::new(0)),
    ))
    .unwrap();

    assert!(remote.staged().is_empty());
    assert_eq!(remote.committed(), vec![(vec![7], 2)]);
    assert_eq!(
        remote.commit_calls(),
        commits_before_recovery + 1,
        "recovery must redrive once"
    );
    assert_eq!(
        backend.get(b"checkpoint/2/valid").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), None);
    handle.shutdown().unwrap();
    assert_eq!(
        remote.committed(),
        vec![(vec![7], 2)],
        "EOF must not republish staged data"
    );
    assert_eq!(
        remote.commit_calls(),
        commits_before_recovery + 2,
        "only recovery and healthy EOF commit ran"
    );
}
