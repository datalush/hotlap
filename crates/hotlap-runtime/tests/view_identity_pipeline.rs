//! A public `Pipeline` with an incompatible saved view registry must fail
//! before the sink pump starts, so no writer opens and no EOF commit runs.

#[path = "common/recovery.rs"]
mod recovery;
#[path = "common/spy.rs"]
mod spy;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use futures::StreamExt;
use hotlap::{CmpOp, Hotlap, InputId, Plan, Predicate, Scalar};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink};
use hotlap_connectors::source::Source;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{self, Pipeline, SinkSpec};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use recovery::{Dataset, ResumableSource, SharedBackend};
use spy::SpySource;

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![2]]).with_retention(0)
}

fn filter(value: i64) -> Plan {
    Plan::Filter {
        input: Box::new(Plan::Source(InputId(0))),
        pred: Predicate::Cmp {
            op: CmpOp::Eq,
            col: 0,
            scalar: Scalar::I64(value),
        },
    }
}

fn sources(source: Arc<dyn Source>) -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source,
        watermark: None,
    }])
    .unwrap()
}

/// Seed a checkpoint that binds the name `a` to the handle for `filter(1)`.
fn seed(backend: &SharedBackend) {
    let pipe = Pipeline {
        sources: sources(Arc::new(ResumableSource::new(log()))),
        views: vec![("a".into(), filter(1))],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipe).unwrap();
    let mut stream = pipe.sources.stream().unwrap();
    recovery::drain(&mut hotlap, &pipe.sources, &mut stream, 2);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    recovery::take(&mut checkpointer, &hotlap, &pipe.sources);
}

/// Counts writes, commits and aborts so a rejected pipeline can prove no writer
/// was ever driven.
#[derive(Default)]
struct RecordingSink {
    writes: AtomicU32,
    commits: AtomicU32,
}

#[async_trait::async_trait]
impl Sink for RecordingSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        while changes.next().await.is_some() {}
        Ok(())
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[test]
fn an_incompatible_registry_fails_before_the_pump_starts() {
    let backend = SharedBackend::default();
    seed(&backend);

    let inner: Arc<dyn Source> = Arc::new(ResumableSource::new(log()));
    let spy = Arc::new(SpySource::new(inner));
    let as_source: Arc<dyn Source> = spy.clone();
    let sink = Arc::new(RecordingSink::default());
    let as_sink: Arc<dyn Sink> = sink.clone();
    let pipeline = Pipeline {
        sources: sources(as_source),
        // Same plan as the saved `a`, but a different name.
        views: vec![("b".into(), filter(1))],
        sinks: vec![SinkSpec {
            view: "b".into(),
            sink: as_sink,
        }],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend: Box::new(backend.clone()),
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    };

    let result = EngineHandle::start(pipeline);
    assert!(
        matches!(result, Err(ConnectorError::Unsupported(_))),
        "an incompatible registry must be rejected"
    );
    assert_eq!(sink.writes.load(Ordering::SeqCst), 0, "no write may run");
    assert_eq!(
        sink.commits.load(Ordering::SeqCst),
        0,
        "the pump must not run its EOF commit"
    );
    assert_eq!(spy.resumed(), 0, "no source may be reopened");
}
