//! A recovery warning must not hide a later fatal checkpoint cause.

#[path = "common/recovery.rs"]
mod recovery;

use std::sync::mpsc;
use std::time::Duration;

use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{Sink, SinkCapabilities};
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::SinkSpec;
use recovery::{Dataset, ResumableSource, drain, engine_with, pipeline, rows, take};

struct CommitSink {
    fail_commit: bool,
    committed: mpsc::Sender<()>,
    gate: Option<std::sync::Arc<tokio::sync::Notify>>,
}

#[async_trait::async_trait]
impl Sink for CommitSink {
    async fn write(
        &self,
        mut changes: hotlap_connectors::ChangeStream,
    ) -> Result<(), ConnectorError> {
        while let Some(change) = futures::StreamExt::next(&mut changes).await {
            change?;
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
    }

    fn accepts_retractions(&self) -> bool {
        true
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        let _ = self.committed.send(());
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        if self.fail_commit {
            return Err(ConnectorError::Infrastructure("commit failed real".into()));
        }
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn start(
    backend: recovery::SharedBackend,
    fail_commit: bool,
    interval: Duration,
    gate: Option<std::sync::Arc<tokio::sync::Notify>>,
) -> (EngineHandle, mpsc::Receiver<()>) {
    let dataset =
        Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3], vec![3, 4]]).with_retention(0);
    let mut seed = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut hotlap, seed_pipeline) = engine_with(ResumableSource::new(dataset.clone()));
    let mut seed_stream = seed_pipeline.sources.stream().unwrap();
    drain(&mut hotlap, &seed_pipeline.sources, &mut seed_stream, 3);
    assert!(!rows(&hotlap.snapshot("c").unwrap()).is_empty());
    take(&mut seed, &hotlap, &seed_pipeline.sources);

    let mut writer = backend.clone();
    let sources = writer.get(b"checkpoint/1/sources").unwrap().unwrap();
    writer
        .put(b"checkpoint/2/engine", b"not-a-snapshot".to_vec())
        .unwrap();
    writer.put(b"checkpoint/2/sources", sources).unwrap();
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();

    let config = CheckpointConfig {
        interval,
        backend: Box::new(backend),
        retain: DEFAULT_RETAIN,
    };
    let mut pipeline = pipeline(ResumableSource::new(dataset), Some(config));
    let (committed, received) = mpsc::channel();
    pipeline.sinks.push(SinkSpec {
        view: "c".into(),
        sink: std::sync::Arc::new(CommitSink {
            fail_commit,
            committed,
            gate,
        }),
    });
    (EngineHandle::start(pipeline).unwrap(), received)
}

#[test]
fn fatal_checkpoint_cause_replaces_prior_recovery_warning_on_shutdown() {
    let (handle, committed) = start(
        recovery::SharedBackend::default(),
        true,
        Duration::from_secs(3600),
        None,
    );
    assert!(
        handle
            .checkpoint_error()
            .unwrap()
            .is_some_and(|warning| warning.contains("discarded interrupted checkpoint")),
        "startup must first record the corrupt-body replay warning"
    );

    let checkpoint = handle.checkpoint().expect_err("sink commit must fail");
    assert!(checkpoint.to_string().contains("commit failed real"));
    committed
        .recv_timeout(Duration::from_secs(5))
        .expect("failed checkpoint commit was not attempted");
    let shutdown = handle
        .shutdown()
        .expect_err("fatal checkpoint cause must survive");
    assert!(
        shutdown.to_string().contains("commit failed real"),
        "got {shutdown}"
    );
    assert!(
        !shutdown
            .to_string()
            .contains("discarded interrupted checkpoint"),
        "a recovery warning must not hide the fatal cause"
    );
}

#[test]
fn recovery_warning_alone_does_not_fail_healthy_shutdown() {
    let (handle, _committed) = start(
        recovery::SharedBackend::default(),
        false,
        Duration::from_secs(3600),
        None,
    );
    assert!(
        handle
            .checkpoint_error()
            .unwrap()
            .is_some_and(|warning| warning.contains("discarded interrupted checkpoint"))
    );
    handle.shutdown().expect("warning alone is not fatal");
}

#[test]
fn fatal_periodic_checkpoint_replaces_prior_recovery_warning() {
    let gate = std::sync::Arc::new(tokio::sync::Notify::new());
    let (handle, committed) = start(
        recovery::SharedBackend::default(),
        true,
        Duration::from_millis(50),
        Some(gate.clone()),
    );
    assert!(
        handle
            .checkpoint_error()
            .unwrap()
            .is_some_and(|warning| warning.contains("discarded interrupted checkpoint")),
        "startup must first record the recovery warning"
    );
    committed
        .recv_timeout(Duration::from_secs(5))
        .expect("periodic checkpoint did not reach sink commit");
    gate.notify_one();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if handle
            .checkpoint_error()
            .unwrap()
            .is_some_and(|error| error.contains("commit failed real"))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let shutdown = handle
        .shutdown()
        .expect_err("periodic checkpoint failure must survive shutdown");
    assert!(
        shutdown.to_string().contains("commit failed real"),
        "got {shutdown}"
    );
}
