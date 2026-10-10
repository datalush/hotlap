//! A sink writer failure at Flush must stop the public runtime.

#[path = "common/backend.rs"]
mod backend;
#[path = "checkpoint_drain_failstop/source.rs"]
mod source_fixture;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use hotlap::{CmpOp, InputId, Plan, Predicate, Scalar};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::SourceBatch;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;
use source_fixture::{GatedSource, schema};

struct FailingSink {
    physical_identity: String,
    aborts: std::sync::mpsc::Sender<()>,
    writes: AtomicU32,
}

#[async_trait::async_trait]
impl Sink for FailingSink {
    fn physical_identity(&self) -> Option<String> {
        Some(self.physical_identity.clone())
    }

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

fn pipeline(source: Arc<GatedSource>, sink: Arc<FailingSink>) -> Pipeline {
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
        sinks: vec![SinkSpec::named("failure-sink", "v", sink)],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(SharedBackend::default()),
            retain: DEFAULT_RETAIN,
        }),
        retention: Some(16),
    }
}

fn send(sender: &source_fixture::BatchSender, key: i64, base_offset: i64) {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![key]));
    let batch = RecordBatch::try_new(schema(), vec![array]).unwrap();
    sender
        .send(Ok(SourceBatch {
            batch,
            base_offset,
            next_offset: base_offset + 1,
            split: 0,
        }))
        .unwrap();
}

fn assert_failstop_commands(handle: &EngineHandle, late_view: Plan) {
    assert!(matches!(
        handle.checkpoint(),
        Err(ConnectorError::Infrastructure(message)) if message == "engine stopped after a runtime failure"
    ));
    assert!(matches!(
        handle.build_view("after_failure", late_view),
        Err(ConnectorError::Infrastructure(message)) if message == "engine stopped after a runtime failure"
    ));
}

fn assert_no_late_ack(commits: &std::sync::mpsc::Receiver<(i32, i64)>) {
    assert!(
        matches!(
            commits.recv_timeout(Duration::from_millis(250)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "the later filtered row must not be acknowledged after the failure"
    );
}

#[test]
fn failed_flush_stops_later_filtered_source_ack_and_view_build() {
    let (source, sender, commits) =
        GatedSource::new(schema(), "test/checkpoint-drain-failstop/source");
    let (aborts_tx, aborts_rx) = std::sync::mpsc::channel();
    let sink = Arc::new(FailingSink {
        physical_identity: "test/checkpoint-drain-failstop/sink".into(),
        aborts: aborts_tx,
        writes: AtomicU32::new(0),
    });
    let handle = EngineHandle::start(pipeline(source.clone(), sink.clone())).unwrap();
    let late_view = Plan::Source(InputId(0));
    handle
        .build_view("healthy_late_view", late_view.clone())
        .expect("retention must allow a late view before the failure");

    send(&sender, 1, 0);
    assert_eq!(
        commits.recv_timeout(Duration::from_secs(5)).unwrap(),
        (0, 1),
        "the first row must be acknowledged before injecting the failure"
    );
    aborts_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the writer must abort its failed write");
    assert!(
        handle.checkpoint().is_err(),
        "Flush must report writer failure"
    );
    send(&sender, 9, 1);
    assert_failstop_commands(&handle, late_view);
    assert_no_late_ack(&commits);

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
