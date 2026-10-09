//! Fixtures for the commit-marker and abort-ordering tests.
//!
//! Local to `checkpoint_abort`, so no shared fixture is included and unused.

use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap::{Hotlap, InputId};
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

/// Control calls a transactional sink recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    Prepare,
    Commit,
    Abort,
}

/// A transactional sink that records control calls and can fail its commit.
struct Probe {
    events: Arc<Mutex<Vec<Event>>>,
    fail_commit: bool,
}

#[async_trait::async_trait]
impl Sink for Probe {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }
    async fn prepare(&self) -> Result<(), ConnectorError> {
        self.events.lock().unwrap().push(Event::Prepare);
        Ok(())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.events.lock().unwrap().push(Event::Commit);
        if self.fail_commit {
            return Err(ConnectorError::Infrastructure("commit failed".into()));
        }
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        self.events.lock().unwrap().push(Event::Abort);
        Ok(())
    }
}

/// A barrier handle over a probe sink.
pub fn sink(events: &Arc<Mutex<Vec<Event>>>, fail_commit: bool) -> SinkSync {
    let probe = Arc::new(Probe {
        events: Arc::clone(events),
        fail_commit,
    });
    SinkSync::sink_only(SharedSink::new(probe))
}

/// An empty, healthy engine whose capture succeeds.
pub fn healthy_engine() -> Hotlap {
    Hotlap::open_with(Box::new(EngineCore::new()))
}

/// An engine poisoned by a partially-applied push, so `checkpoint` fails with a
/// non-storage error while the sources were already prepared.
pub fn poisoned_engine() -> Hotlap {
    let mut engine = Hotlap::open_with(Box::new(EngineCore::new()));
    engine.register_input("in").unwrap();
    engine
        .create_view("a", hotlap::Plan::Source(InputId(0)))
        .unwrap();
    engine
        .create_view("b", hotlap::Plan::Source(InputId(0)))
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let saturated =
        hotlap::ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![i64::MAX]))).unwrap();
    engine.push("in", &saturated).unwrap();
    assert!(
        engine.push("in", &saturated).is_err(),
        "the second push must poison the engine"
    );
    engine
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
