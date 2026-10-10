//! Shared fixtures for pipeline view-identity recovery tests.

#[path = "backend.rs"]
mod backend;
#[path = "recovery/ops.rs"]
mod ops;
#[path = "recovery/resumable.rs"]
mod resumable;
#[path = "spy.rs"]
mod spy;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use futures::StreamExt;
use hotlap::state::StateBackend;
use hotlap::{CmpOp, Hotlap, InputId, Plan, Predicate, Scalar};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::Source;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

pub use backend::SharedBackend;
pub use ops::rows;
pub use resumable::{Dataset, ResumableSource};
pub use spy::SpySource;

pub fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![2]]).with_retention(0)
}

pub fn filter(value: i64) -> Plan {
    Plan::Filter {
        input: Box::new(Plan::Source(InputId(0))),
        pred: Predicate::Cmp {
            op: CmpOp::Eq,
            col: 0,
            scalar: Scalar::I64(value),
        },
    }
}

pub fn sources(source: Arc<dyn Source>) -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source,
        watermark: None,
    }])
    .unwrap()
}

pub fn seed(backend: &SharedBackend) {
    let pipe = Pipeline {
        sources: sources(Arc::new(ResumableSource::new(log()))),
        views: vec![("a".into(), filter(1))],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipe).unwrap();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    futures::executor::block_on(checkpointer.take(&hotlap, &pipe.sources)).unwrap();
}

pub fn seed_output(backend: &SharedBackend) {
    let pipe = Pipeline {
        sources: sources(Arc::new(ResumableSource::new(log()))),
        views: vec![("a".into(), filter(1))],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipe).unwrap();
    let mut input = pipe.sources.stream().unwrap();
    ops::drain(&mut hotlap, &pipe.sources, &mut input, 1);
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    assert_eq!(ops::take(&mut checkpointer, &hotlap, &pipe.sources), 1);
}

pub fn seed_two_outputs(backend: &SharedBackend) {
    let pipe = Pipeline {
        sources: sources(Arc::new(ResumableSource::new(log()))),
        views: vec![("a".into(), filter(1))],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipe).unwrap();
    let mut input = pipe.sources.stream().unwrap();
    let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    ops::drain(&mut hotlap, &pipe.sources, &mut input, 1);
    assert_eq!(ops::take(&mut checkpointer, &hotlap, &pipe.sources), 1);
    ops::drain(&mut hotlap, &pipe.sources, &mut input, 1);
    assert_eq!(ops::take(&mut checkpointer, &hotlap, &pipe.sources), 2);
}

pub fn corrupt_pending(backend: &SharedBackend) {
    let mut writer = backend.clone();
    for part in ["engine", "sources"] {
        let value = writer
            .get(format!("checkpoint/1/{part}").as_bytes())
            .unwrap()
            .unwrap();
        writer
            .put(format!("checkpoint/2/{part}").as_bytes(), value)
            .unwrap();
    }
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
    writer
        .put(b"checkpoint/2/engine", b"HLSR\x02".to_vec())
        .unwrap();
}

#[derive(Default)]
pub struct RecordingSink {
    pub writes: AtomicU32,
    pub commits: AtomicU32,
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
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }
}
