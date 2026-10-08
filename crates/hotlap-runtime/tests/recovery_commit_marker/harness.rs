//! Fixtures for the commit-marker recovery tests.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::sink::SharedSink;
use hotlap_runtime::runtime::sources::Sources;

use crate::recovery::{
    Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, sources, take,
};

/// The log shared by the commit-marker tests.
pub fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

/// A sink that counts commits and, when probing, reports whether the commit
/// marker was visible (and `valid` absent) while `commit` ran.
pub struct FakeSink {
    pub capabilities: SinkCapabilities,
    pub commits: Arc<AtomicU32>,
    pub probe: Option<(SharedBackend, u64, Arc<Mutex<bool>>)>,
}

#[async_trait::async_trait]
impl Sink for FakeSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        self.capabilities
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        if let Some((backend, id, observed)) = &self.probe {
            let marker = backend
                .get(format!("checkpoint/{id}/commit").as_bytes())
                .unwrap()
                .is_some();
            let valid = backend
                .get(format!("checkpoint/{id}/valid").as_bytes())
                .unwrap()
                .is_some();
            *observed.lock().unwrap() = marker && !valid;
        }
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A shared fake sink with the given capability plus its commit counter.
pub fn sink(capabilities: SinkCapabilities) -> (Arc<SharedSink>, Arc<AtomicU32>) {
    let commits = Arc::new(AtomicU32::new(0));
    let sink = Arc::new(FakeSink {
        capabilities,
        commits: Arc::clone(&commits),
        probe: None,
    });
    (SharedSink::new(sink), commits)
}

/// Persist a valid checkpoint 1 over the shared backend.
pub fn seed_valid_one() -> SharedBackend {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    take(&mut checkpointer, &engine, &pipe.sources);
    assert_eq!(
        rows(&engine.snapshot("c").unwrap()),
        vec![vec![1, 2], vec![2, 2]]
    );
    backend
}

/// Simulate a crash mid-commit: copy the valid body to `pending` and add the
/// durable commit marker, leaving `valid` absent.
pub fn seed_pending(backend: &SharedBackend, valid: u64, pending: u64) {
    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let value = writer
            .get(format!("checkpoint/{valid}/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/{pending}/{part}").as_bytes(), value)
            .unwrap();
    }
    writer
        .put(
            format!("checkpoint/{pending}/commit").as_bytes(),
            b"1".to_vec(),
        )
        .unwrap();
}

/// Sources matching the seeded checkpoint's declaration.
pub fn matching() -> Sources {
    sources(ResumableSource::new(log()))
}
