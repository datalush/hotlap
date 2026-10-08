//! Shared fixtures for the pending-commit recovery tests.

use std::sync::Arc;

use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline as runtime;
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sink::SharedSink;

use crate::recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, take};

pub(super) fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]]).with_retention(0)
}

/// A sink that only declares its capability, exercising the built-in default.
struct CapabilitySink(SinkCapabilities);

#[async_trait::async_trait]
impl Sink for CapabilitySink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        self.0
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

pub(super) fn capability(capabilities: SinkCapabilities) -> Arc<SharedSink> {
    SharedSink::new(Arc::new(CapabilitySink(capabilities)))
}

/// Persist a valid checkpoint 1 over a fresh shared backend.
pub(super) fn seed_valid_one() -> SharedBackend {
    let backend = SharedBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut stream = runtime::merged_stream(pipe.source.as_ref()).unwrap();
    drain(&mut engine, pipe.source.as_ref(), &mut stream, 3);
    take(&mut checkpointer, &engine, pipe.source.as_ref());
    backend
}

/// Copy the valid body to `pending` and add the durable commit marker.
pub(super) fn seed_pending(backend: &SharedBackend, valid: u64, pending: u64) {
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

/// Delete every key under `checkpoint/<id>/` and the `latest` pointer.
pub(super) fn delete_checkpoint(backend: &SharedBackend, id: u64) {
    let mut writer = backend.clone();
    for key in writer.list(format!("checkpoint/{id}/").as_bytes()).unwrap() {
        writer.delete(&key).unwrap();
    }
}

pub(super) fn fallback_id(backend: &SharedBackend) -> u64 {
    let checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    Recovery::load(&checkpointer)
        .unwrap()
        .expect("checkpoint")
        .id
}
