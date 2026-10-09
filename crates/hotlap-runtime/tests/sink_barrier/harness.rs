//! Fixtures for the sink-barrier (2PC) tests.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{Field, Schema, SchemaRef};
use hotlap::state::{StateBackend, StateEntry, StateError};
use hotlap::{Hotlap, InputId};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError, Sink, SinkCapabilities};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

/// Call order recorded by a fake sink.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    Prepare,
    Commit,
    Abort,
}

/// Records the 2PC calls it receives; can fail its prepare or commit.
pub struct FakeSink {
    capabilities: SinkCapabilities,
    events: Arc<Mutex<Vec<Event>>>,
    fail_prepare: bool,
    fail_commit: bool,
}

impl FakeSink {
    pub fn new(capabilities: SinkCapabilities, events: Arc<Mutex<Vec<Event>>>) -> Self {
        Self {
            capabilities,
            events,
            fail_prepare: false,
            fail_commit: false,
        }
    }

    pub fn failing_prepare(events: Arc<Mutex<Vec<Event>>>) -> Self {
        Self {
            capabilities: SinkCapabilities::Transactional,
            events,
            fail_prepare: true,
            fail_commit: false,
        }
    }

    pub fn failing_commit(capabilities: SinkCapabilities, events: Arc<Mutex<Vec<Event>>>) -> Self {
        Self {
            capabilities,
            events,
            fail_prepare: false,
            fail_commit: true,
        }
    }

    fn record(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }
}

#[async_trait::async_trait]
impl Sink for FakeSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        self.capabilities
    }

    async fn prepare(&self) -> Result<(), ConnectorError> {
        self.record(Event::Prepare);
        if self.fail_prepare {
            return Err(ConnectorError::Infrastructure("prepare failed".into()));
        }
        Ok(())
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.record(Event::Commit);
        if self.fail_commit {
            return Err(ConnectorError::Infrastructure("commit failed".into()));
        }
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.record(Event::Abort);
        Ok(())
    }
}

/// In-memory backend that can be toggled to fail every `put` except the durable
/// id reservation, so a body write fails after the sinks were prepared.
#[derive(Clone, Default)]
pub struct MemBackend {
    map: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    failing: Arc<Mutex<bool>>,
}

impl MemBackend {
    pub fn failing_writes() -> Self {
        let backend = Self::default();
        *backend.failing.lock().unwrap() = true;
        backend
    }
}

impl StateBackend for MemBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        // The reservation must succeed so the failure lands on the checkpoint
        // body, after the two-phase-commit sinks were prepared.
        if *self.failing.lock().unwrap() && key != b"checkpoint/reserved" {
            return Err(StateError::Io(io::Error::other("backend write failed")));
        }
        self.map.lock().unwrap().insert(key.to_vec(), value);
        Ok(())
    }

    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        self.map.lock().unwrap().remove(key);
        Ok(())
    }
}

/// A source with no splits and empty state, enough for the barrier.
pub struct EmptySource;

impl Source for EmptySource {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(Vec::<Field>::new()))
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(Vec::new())
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(futures::stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

/// An empty engine for the barrier.
pub fn engine() -> Hotlap {
    Hotlap::open_with(Box::new(hotlap_engine::EngineCore::new()))
}

/// A single empty source set, enough for the barrier.
pub fn sources() -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(EmptySource),
        watermark: None,
    }])
    .unwrap()
}

/// A fresh event log.
pub fn events() -> Arc<Mutex<Vec<Event>>> {
    Arc::new(Mutex::new(Vec::new()))
}
