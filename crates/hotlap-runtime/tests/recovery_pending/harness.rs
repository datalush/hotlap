//! Shared fixtures for the pending-commit recovery tests.

use std::sync::Arc;

use hotlap::state::StateBackend;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

#[path = "../common/participants.rs"]
mod participants;

use crate::recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, sources, take};

pub(super) fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]])
        .with_retention(0)
        .with_physical_identity("test/recovery-pending/log")
}

/// A sink that only declares its capability, exercising the built-in default.
struct CapabilitySink {
    capabilities: SinkCapabilities,
    identity: String,
}

#[async_trait::async_trait]
impl Sink for CapabilitySink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        self.capabilities
    }
    fn physical_identity(&self) -> Option<String> {
        Some(self.identity.clone())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

pub(super) fn capability(
    capabilities: SinkCapabilities,
    identity: &str,
    binding_name: &str,
) -> SinkSync {
    let sink = Arc::new(CapabilitySink {
        capabilities,
        identity: identity.to_owned(),
    });
    SinkSync::sink_only_named(
        SharedSink::new(sink),
        binding_name.to_owned(),
        "c".to_owned(),
    )
}

/// Persist a valid checkpoint 1 over a fresh shared backend.
pub(super) fn seed_valid_one(sinks: Vec<SinkSync>) -> SharedBackend {
    let backend = SharedBackend::default();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(sinks);
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    engine.tap_view("c").unwrap();
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    take(&mut checkpointer, &engine, &pipe.sources);
    backend
}

/// Copy the valid body to `pending` and add the durable commit marker.
pub(super) fn seed_pending(backend: &SharedBackend, valid: u64, pending: u64) {
    let mut writer = backend.clone();
    for part in ["engine", "sources", "participants"] {
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
    let declared = sources(ResumableSource::new(log()));
    Recovery::load(&checkpointer, &declared)
        .unwrap()
        .expect("checkpoint")
        .id
}
