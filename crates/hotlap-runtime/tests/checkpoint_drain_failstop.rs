//! A sink writer failure at Flush must stop the public runtime.

#[path = "common/backend.rs"]
mod backend;
#[path = "runtime_checkpoint_fail_stop/harness.rs"]
mod harness;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::{CmpOp, InputId, Plan, Predicate, Scalar};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::{SourceBatch, Split};
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;
use harness::{BatchSender, ControlledB, schema};

struct FailingSink {
    aborts: std::sync::mpsc::Sender<()>,
    writes: AtomicU32,
}

#[async_trait::async_trait]
impl Sink for FailingSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        while changes.next().await.is_some() {}
        Err(ConnectorError::Infrastructure(
            "last sink write failed".into(),
        ))
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.aborts.send(()).unwrap();
        Ok(())
    }
}

fn wait_for(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(done(), "gated source state was not reached");
}

fn pipeline(source: Arc<ControlledB>, sink: Arc<FailingSink>) -> Pipeline {
    let plan = Plan::Filter {
        input: Box::new(Plan::Source(InputId(0))),
        pred: Predicate::Cmp {
            op: CmpOp::Eq,
            col: 0,
            scalar: Scalar::I64(1),
        },
    };
    Pipeline {
        sources: Sources::new(vec![InputSource {
            id: InputId(0),
            name: "in".into(),
            source,
            watermark: None,
        }])
        .unwrap(),
        views: vec![("v".into(), plan)],
        sinks: vec![SinkSpec {
            view: "v".into(),
            sink,
        }],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(SharedBackend::default()),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    }
}

fn send(sender: &BatchSender, key: i64) {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![key]));
    let batch = RecordBatch::try_new(schema(), vec![array]).unwrap();
    sender
        .send(Ok(SourceBatch {
            batch,
            base_offset: 0,
            next_offset: 1,
            split: 0,
        }))
        .unwrap();
}

#[test]
fn failed_flush_stops_later_filtered_source_ack_and_view_build() {
    let (source, senders) = ControlledB::new(schema(), vec![Split { id: 0, start: 0 }]);
    let (aborts_tx, aborts_rx) = std::sync::mpsc::channel();
    let sink = Arc::new(FailingSink {
        aborts: aborts_tx,
        writes: AtomicU32::new(0),
    });
    let handle = EngineHandle::start(pipeline(source.clone(), sink.clone())).unwrap();

    send(&senders[0], 1);
    wait_for(|| source.commits().len() == 1);
    aborts_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the writer must abort its failed write");
    assert!(
        handle.checkpoint().is_err(),
        "Flush must report writer failure"
    );

    assert!(
        handle
            .build_view("later", Plan::Source(InputId(0)))
            .is_err()
    );
    send(&senders[0], 9);
    assert!(
        handle.shutdown().is_err(),
        "the failed writer must surface at close"
    );
    assert_eq!(
        source.commits().len(),
        1,
        "the filtered row must not be acked"
    );
    assert_eq!(sink.writes.load(Ordering::SeqCst), 1);
}
