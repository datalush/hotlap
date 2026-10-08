//! Two-phase-commit sink coordination at the checkpoint barrier.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{Field, Schema, SchemaRef};
use hotlap::Hotlap;
use hotlap::state::{StateBackend, StateEntry, StateError};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError, Sink, SinkCapabilities};
use hotlap_runtime::runtime::checkpoint::Checkpointer;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

/// Call order recorded by a fake sink.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    Prepare,
    Commit,
    Abort,
}

/// Records the 2PC calls it receives; can fail its prepare.
struct FakeSink {
    capabilities: SinkCapabilities,
    events: Arc<Mutex<Vec<Event>>>,
    fail_prepare: bool,
    fail_commit: bool,
}

impl FakeSink {
    fn new(capabilities: SinkCapabilities, events: Arc<Mutex<Vec<Event>>>) -> Self {
        Self {
            capabilities,
            events,
            fail_prepare: false,
            fail_commit: false,
        }
    }

    fn failing_prepare(events: Arc<Mutex<Vec<Event>>>) -> Self {
        Self {
            capabilities: SinkCapabilities::Transactional,
            events,
            fail_prepare: true,
            fail_commit: false,
        }
    }

    fn failing_commit(capabilities: SinkCapabilities, events: Arc<Mutex<Vec<Event>>>) -> Self {
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

/// In-memory backend that can be toggled to fail every `put`.
#[derive(Clone, Default)]
struct MemBackend {
    map: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    failing: Arc<Mutex<bool>>,
}

impl MemBackend {
    fn failing() -> Self {
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
        if *self.failing.lock().unwrap() {
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
struct EmptySource;

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

fn engine() -> Hotlap {
    Hotlap::open_with(Box::new(hotlap_engine::EngineCore::new()))
}

fn events() -> Arc<Mutex<Vec<Event>>> {
    Arc::new(Mutex::new(Vec::new()))
}

#[tokio::test]
async fn transactional_sink_prepares_then_commits_before_valid() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    let id = checkpointer
        .take(&engine(), &EmptySource)
        .await
        .expect("checkpoint");
    assert_eq!(id, 1);
    assert_eq!(
        *log.lock().unwrap(),
        vec![Event::Prepare, Event::Commit],
        "commit must follow prepare and precede validity"
    );

    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(reader.latest().unwrap(), Some(1));
    assert!(
        reader.read(1).is_ok(),
        "checkpoint must be valid after commit"
    );
}

#[tokio::test]
async fn capture_failure_aborts_and_discards_the_checkpoint() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        log.clone(),
    )));
    let backend = MemBackend::failing();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    let result = checkpointer.take(&engine(), &EmptySource).await;
    assert!(result.is_err(), "a failing write must surface the error");
    assert_eq!(
        *log.lock().unwrap(),
        vec![Event::Prepare, Event::Abort],
        "a prepared sink must be aborted"
    );
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(
        reader.latest().unwrap(),
        None,
        "no checkpoint may become valid"
    );
}

#[tokio::test]
async fn prepare_failure_aborts_the_already_prepared_sinks() {
    let log = events();
    let first = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        log.clone(),
    )));
    // The second sink fails prepare; the first, already prepared, is aborted.
    let second_log = Arc::new(Mutex::new(Vec::new()));
    let second = SharedSink::new(Arc::new(FakeSink::failing_prepare(second_log.clone())));
    let backend = MemBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![
        SinkSync::sink_only(first),
        SinkSync::sink_only(second),
    ]);

    let result = checkpointer.take(&engine(), &EmptySource).await;
    assert!(result.is_err());
    assert_eq!(*log.lock().unwrap(), vec![Event::Prepare, Event::Abort]);
    assert_eq!(*second_log.lock().unwrap(), vec![Event::Prepare]);
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(reader.latest().unwrap(), None);
}

#[tokio::test]
async fn idempotent_sink_is_flushed_but_not_prepared() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Idempotent,
        log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    checkpointer.take(&engine(), &EmptySource).await.unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        vec![Event::Commit],
        "idempotent sinks flush on commit without prepare"
    );
}

#[tokio::test]
async fn at_least_once_sink_is_not_coordinated() {
    let log = events();
    let sink = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::AtLeastOnce,
        log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer =
        Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![SinkSync::sink_only(sink)]);

    checkpointer.take(&engine(), &EmptySource).await.unwrap();
    assert!(
        log.lock().unwrap().is_empty(),
        "at-least-once sinks are already visible and must not be coordinated"
    );
}

#[tokio::test]
async fn commit_failure_aborts_the_failed_and_remaining_prepared_sinks() {
    let log = events();
    let first = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        log.clone(),
    )));
    // The second fails its commit; it must be aborted along with the first.
    let second_log = events();
    let second = SharedSink::new(Arc::new(FakeSink::failing_commit(
        SinkCapabilities::Transactional,
        second_log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![
        SinkSync::sink_only(first),
        SinkSync::sink_only(second),
    ]);

    let result = checkpointer.take(&engine(), &EmptySource).await;
    assert!(result.is_err());
    assert_eq!(*log.lock().unwrap(), vec![Event::Prepare, Event::Commit]);
    assert_eq!(
        *second_log.lock().unwrap(),
        vec![Event::Prepare, Event::Commit, Event::Abort],
        "the sink whose commit failed must still be aborted"
    );
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(reader.latest().unwrap(), None);
}

#[tokio::test]
async fn idempotent_flush_failure_aborts_prepared_transactional_sinks() {
    let transactional_log = events();
    let transactional = SharedSink::new(Arc::new(FakeSink::new(
        SinkCapabilities::Transactional,
        transactional_log.clone(),
    )));
    let idempotent_log = events();
    let idempotent = SharedSink::new(Arc::new(FakeSink::failing_commit(
        SinkCapabilities::Idempotent,
        idempotent_log.clone(),
    )));
    let backend = MemBackend::default();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), 3).with_sinks(vec![
        SinkSync::sink_only(transactional),
        SinkSync::sink_only(idempotent),
    ]);

    let result = checkpointer.take(&engine(), &EmptySource).await;
    assert!(result.is_err());
    assert_eq!(
        *transactional_log.lock().unwrap(),
        vec![Event::Prepare, Event::Abort],
        "an idempotent flush failure must abort the prepared transactional sink"
    );
    assert_eq!(*idempotent_log.lock().unwrap(), vec![Event::Commit]);
    let reader = Checkpointer::new(Box::new(backend), 3);
    assert_eq!(reader.latest().unwrap(), None);
}
