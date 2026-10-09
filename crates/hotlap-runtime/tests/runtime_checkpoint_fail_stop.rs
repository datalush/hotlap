//! A failed checkpoint leaves the engine inconsistent, so ingestion stops.
//!
//! Aborting the sinks does not roll the engine or the source offsets back. The
//! serving loop must therefore stop polling sources and keep rejecting both
//! checkpoints and post-start view builds until a restart resolves the state.

#[path = "common/backend.rs"]
mod backend;
#[path = "common/fault.rs"]
mod fault;
#[path = "runtime_checkpoint_fail_stop/harness.rs"]
mod harness;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use hotlap::{InputId, Plan};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{SourceBatch, Split};
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;
use fault::FaultBackend;
use harness::{ControlledB, schema};

fn split() -> Split {
    Split { id: 0, start: 0 }
}

fn batch_on(key: i64) -> SourceBatch {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![key]));
    let batch = RecordBatch::try_new(schema(), vec![array]).unwrap();
    SourceBatch {
        batch,
        base_offset: 0,
        next_offset: 1,
        split: 0,
    }
}

fn sources(b: Arc<ControlledB>) -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: b,
        watermark: None,
    }])
    .unwrap()
}

/// A pipeline over `b` whose first checkpoint body write fails.
fn pipeline(b: Arc<ControlledB>, interval: Duration) -> Pipeline {
    let faulty = FaultBackend::new(SharedBackend::default());
    faulty.fail("put", b"checkpoint/1/engine", false);
    Pipeline {
        sources: sources(b),
        views: vec![],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval,
            backend: Box::new(faulty),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    }
}

fn wait_for(done: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

/// The failure must stop ingestion and keep checkpoints/views rejected.
fn assert_stopped(b: Arc<ControlledB>, b_tx: Vec<harness::BatchSender>, handle: EngineHandle) {
    b_tx[0].send(Ok(batch_on(9))).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        b.commits().is_empty(),
        "no source may continue after a checkpoint failure"
    );
    assert!(
        handle.checkpoint().is_err(),
        "checkpoints must stay rejected"
    );
    let build = handle.build_view(
        "v",
        Plan::Project {
            input: Box::new(Plan::Source(InputId(0))),
            cols: vec![0],
        },
    );
    assert!(build.is_err(), "view builds must stay rejected");
    handle.shutdown().unwrap();
}

#[test]
fn a_manual_checkpoint_failure_stops_ingestion() {
    let (b, b_tx) = ControlledB::new(schema(), vec![split()]);
    let handle = EngineHandle::start(pipeline(b.clone(), Duration::from_secs(3600))).unwrap();

    let error = handle.checkpoint().unwrap_err();
    assert!(matches!(error, ConnectorError::Storage(_)), "got {error:?}");

    assert_stopped(b, b_tx, handle);
}

#[test]
fn a_periodic_checkpoint_failure_stops_ingestion() {
    let (b, b_tx) = ControlledB::new(schema(), vec![split()]);
    let handle = EngineHandle::start(pipeline(b.clone(), Duration::from_millis(20))).unwrap();

    assert!(
        wait_for(|| handle.checkpoint_error().unwrap().is_some()),
        "the periodic checkpoint never failed"
    );

    assert_stopped(b, b_tx, handle);
}
