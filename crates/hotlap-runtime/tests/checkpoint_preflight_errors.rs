//! Preflight must not blur operational or format errors into corruption.

#[path = "common/backend.rs"]
mod backend;
#[path = "common/fault.rs"]
mod fault;
#[path = "common/recovery/resumable.rs"]
mod resumable;

use std::sync::Arc;
use std::time::Duration;

use hotlap::state::StateBackend;
use hotlap::{CmpOp, Hotlap, InputId, Plan, Predicate, Scalar};
use hotlap_connectors::error::ConnectorError;
use hotlap_engine::EngineCore;
use hotlap_runtime::runtime::checkpoint::{CheckpointConfig, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::{self, Pipeline};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

use backend::SharedBackend;
use fault::FaultBackend;
use resumable::{Dataset, ResumableSource};

fn log() -> Dataset {
    Dataset::new(vec![vec![1]]).with_retention(0)
}

fn filter() -> Plan {
    Plan::Filter {
        input: Box::new(Plan::Source(InputId(0))),
        pred: Predicate::Cmp {
            op: CmpOp::Eq,
            col: 0,
            scalar: Scalar::I64(1),
        },
    }
}

fn sources() -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(ResumableSource::new(log())),
        watermark: None,
    }])
    .unwrap()
}

fn seed(backend: &SharedBackend) {
    let pipe = Pipeline {
        sources: sources(),
        views: vec![("a".into(), filter())],
        sinks: vec![],
        checkpoint: None,
        retention: None,
    };
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, &pipe).unwrap();
    let mut checkpointer = hotlap_runtime::runtime::checkpoint::Checkpointer::new(
        Box::new(backend.clone()),
        DEFAULT_RETAIN,
    );
    futures::executor::block_on(checkpointer.take(&hotlap, &pipe.sources)).unwrap();
}

#[test]
fn storage_and_unsupported_errors_remain_fatal_in_pipeline_preflight() {
    let backend = SharedBackend::default();
    seed(&backend);
    let faulty = FaultBackend::new(backend.clone());
    faulty.fail("get", b"checkpoint/1/engine", false);
    let mut pipe = pipeline(Box::new(faulty));
    assert!(matches!(
        pipe.preflight_recovery(),
        Err(ConnectorError::Storage(_))
    ));

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
    let mut engine = writer.get(b"checkpoint/2/engine").unwrap().unwrap();
    engine[4..8].copy_from_slice(&9_u32.to_le_bytes());
    writer.put(b"checkpoint/2/engine", engine).unwrap();
    writer.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
    let mut unsupported = pipeline(Box::new(backend.clone()));
    assert!(matches!(
        unsupported.preflight_recovery(),
        Err(ConnectorError::Unsupported(_))
    ));
    assert!(backend.get(b"checkpoint/2/commit").unwrap().is_some());
}

fn pipeline(backend: Box<dyn hotlap::state::StateBackend + Send>) -> Pipeline {
    Pipeline {
        sources: sources(),
        views: vec![("a".into(), filter())],
        sinks: vec![],
        checkpoint: Some(CheckpointConfig {
            interval: Duration::from_secs(3600),
            backend,
            retain: DEFAULT_RETAIN,
        }),
        retention: None,
    }
}
