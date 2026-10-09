//! Deterministic scheduler test for the serve loop's fail-stop guard.
//!
//! The source stream yields a read error first and a ready, joinable event
//! second. The loop must go pending after the failure and never poll that second
//! event, so removing the `!source_done` guard makes this test fail.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::Stream;
use hotlap::{Hotlap, InputId};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{
    Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId,
};
use hotlap_engine::EngineCore;
use tokio::sync::mpsc;

use super::{Engine, EngineShared, serve};
use crate::runtime::cancel::Cancel;
use crate::runtime::command::Command;
use crate::runtime::pipeline::Pipeline;
use crate::runtime::sink::SinkPump;
use crate::runtime::sources::{InputSource, InputStream, SourceEvent, Sources};

/// A source that only counts commits; the serve loop never reads it here.
struct DummySource {
    schema: SchemaRef,
    commits: Arc<AtomicUsize>,
}

impl Source for DummySource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }

    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(futures::stream::empty()))
    }

    fn commit(&self, _split: SplitId, _offset: Offset) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn state(&self) -> SourceState {
        SourceState::default()
    }

    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

/// Yields a read error once, then a ready event, recording its poll.
struct FailThenReady {
    polls: Arc<AtomicUsize>,
    b_polled: Arc<AtomicBool>,
    b_event: Option<SourceEvent>,
}

impl Stream for FailThenReady {
    type Item = Result<SourceEvent, ConnectorError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.polls.fetch_add(1, Ordering::SeqCst) {
            // A read error stops the loop before the ready event is ever polled.
            0 => Poll::Ready(Some(Err(ConnectorError::Infrastructure(
                "read boom".into(),
            )))),
            // A single ready event; polling it is the regression under test.
            1 => {
                this.b_polled.store(true, Ordering::SeqCst);
                Poll::Ready(this.b_event.take().map(Ok))
            }
            // Then stay pending forever, so a mutation cannot spin the loop.
            _ => Poll::Pending,
        }
    }
}

/// Engine and pipeline over a source that fails, then offers a ready event.
fn fail_then_ready() -> (Engine, Pipeline, Arc<AtomicBool>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let b_polled = Arc::new(AtomicBool::new(false));
    let commits = Arc::new(AtomicUsize::new(0));
    let schema: SchemaRef = Arc::new(Schema::empty());

    let sources = Sources::new(vec![InputSource {
        id: InputId(1),
        name: "b".into(),
        source: Arc::new(DummySource {
            schema: schema.clone(),
            commits: Arc::clone(&commits),
        }),
        watermark: None,
    }])
    .unwrap();
    let pipeline = Pipeline {
        sources,
        views: vec![],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };

    let source: InputStream = Box::pin(FailThenReady {
        polls: Arc::clone(&polls),
        b_polled: Arc::clone(&b_polled),
        b_event: Some(SourceEvent {
            input: InputId(1),
            batch: SourceBatch {
                batch: RecordBatch::new_empty(schema),
                base_offset: 0,
                next_offset: 1,
                split: 0,
            },
        }),
    });
    let engine = Engine {
        hotlap: Hotlap::open_with(Box::new(EngineCore::new())),
        source,
        sinks: SinkPump::start(&[]),
        checkpointer: None,
        ticker: None,
    };
    (engine, pipeline, b_polled, commits)
}

/// Drive `serve` single-threaded until pending and return the recorded error.
fn drive_until_pending(engine: Engine, pipeline: Pipeline) -> Option<String> {
    let last_error = Arc::new(Mutex::new(None));
    let shared = EngineShared {
        last_error: Arc::clone(&last_error),
        checkpoint_error: Arc::new(Mutex::new(None)),
        close_error: Arc::new(Mutex::new(None)),
        built: Arc::new(AtomicBool::new(false)),
        cancel: Cancel::new(),
    };
    // Keep the sender alive so the command channel stays pending.
    let (_tx, mut rx) = mpsc::unbounded_channel::<Command>();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut serving = Box::pin(serve(engine, pipeline, &mut rx, &shared));
    for _ in 0..5 {
        assert!(
            serving.as_mut().poll(&mut cx).is_pending(),
            "serve must stay pending after the failure"
        );
    }
    drop(serving);
    last_error.lock().unwrap().clone()
}

#[test]
fn a_source_failure_never_polls_the_next_ready_event() {
    let (engine, pipeline, b_polled, commits) = fail_then_ready();
    let error = drive_until_pending(engine, pipeline);
    assert!(error.is_some(), "the read error must be recorded");
    assert!(
        !b_polled.load(Ordering::SeqCst),
        "the ready event must never be polled after a failure"
    );
    assert_eq!(
        commits.load(Ordering::SeqCst),
        0,
        "the ready event must never be acked"
    );
}
