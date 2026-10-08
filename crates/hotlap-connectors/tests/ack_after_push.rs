//! Ack-after-push: the runtime advances a split's offset only once the engine
//! has ingested the batch, so `state()` never runs ahead of applied records.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream;
use hotlap::{InputId, Plan};

use hotlap_connectors::runtime::handle::EngineHandle;
use hotlap_connectors::runtime::pipeline::Pipeline;
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use hotlap_connectors::ConnectorError;

/// Shared view of what the source has produced and what the runtime has acked.
#[derive(Clone, Default)]
struct Ledger {
    produced: Arc<Mutex<Vec<(SplitId, Offset)>>>,
    applied: Arc<Mutex<Vec<(SplitId, Offset)>>>,
}

impl Ledger {
    fn produced(&self) -> Vec<(SplitId, Offset)> {
        self.produced.lock().unwrap().clone()
    }

    fn applied(&self) -> Vec<(SplitId, Offset)> {
        self.applied.lock().unwrap().clone()
    }
}

/// Yields one batch, recording production; only `commit` advances the state.
struct AckSource {
    schema: SchemaRef,
    batch: SourceBatch,
    ledger: Ledger,
}

impl Source for AckSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }

    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        let batch = self.batch.clone();
        // Production advances the read position but must not touch `state()`.
        self.ledger
            .produced
            .lock()
            .unwrap()
            .push((batch.split, batch.next_offset));
        let items: Vec<Result<SourceBatch, ConnectorError>> = vec![Ok(batch)];
        Ok(Box::pin(stream::iter(items)))
    }

    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        self.ledger.applied.lock().unwrap().push((split, offset));
        Ok(())
    }

    fn state(&self) -> SourceState {
        // The checkpoint reads the applied offsets, never the produced ones.
        let mut state = SourceState::default();
        for (split, offset) in self.ledger.applied() {
            state.offsets.insert(split, offset);
        }
        state
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// A two-row batch whose resume offset is `next_offset`.
fn batch(next_offset: Offset) -> SourceBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(Int64Array::from(vec![10, 10])),
    ];
    SourceBatch {
        batch: RecordBatch::try_new(schema(), cols).unwrap(),
        base_offset: 0,
        next_offset,
        split: 0,
    }
}

/// `advance` picks a view the push accepts; otherwise it rejects the push.
fn pipeline(ledger: Ledger, advance: bool) -> Pipeline {
    let input = Box::new(Plan::Source(InputId(0)));
    let view = if advance {
        Plan::GroupCount {
            input,
            key: vec![0],
        }
    } else {
        // A window with no declared watermark fails on the first push.
        Plan::TumbleCount {
            input,
            key: vec![0],
            time_col: 1,
            size: 10,
        }
    };
    Pipeline {
        input: "in".into(),
        source: Box::new(AckSource {
            schema: schema(),
            batch: batch(7),
            ledger,
        }),
        watermark: None,
        views: vec![("c".into(), view)],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    }
}

fn wait_for(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if done() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("condition not met before the deadline");
}

#[test]
fn a_successful_push_advances_to_the_next_offset() {
    let ledger = Ledger::default();
    let handle = EngineHandle::start(pipeline(ledger.clone(), true)).unwrap();
    wait_for(|| !ledger.applied().is_empty());
    assert_eq!(ledger.applied(), vec![(0, 7)]);
    assert_eq!(ledger.produced(), vec![(0, 7)]);
    handle.shutdown().unwrap();
}

#[test]
fn a_failed_push_never_advances_the_offset() {
    let ledger = Ledger::default();
    let handle = EngineHandle::start(pipeline(ledger.clone(), false)).unwrap();
    wait_for(|| handle.last_error().unwrap().is_some());
    assert!(
        !ledger.produced().is_empty(),
        "the batch must have been produced before the push failed"
    );
    assert!(
        ledger.applied().is_empty(),
        "a failed push must not be acked; state() must stay behind applied"
    );
    handle.shutdown().unwrap();
}
