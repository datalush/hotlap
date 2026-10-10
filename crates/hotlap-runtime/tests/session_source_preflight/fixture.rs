use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow::datatypes::DataType;
use hotlap::state::StateBackend;
use hotlap_connectors::source::Split;
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::{Session, SessionConfig};

#[path = "../common/backend.rs"]
mod backend;
#[path = "metadata.rs"]
mod metadata;
#[path = "sink.rs"]
mod sink;

use backend::SharedBackend;
use metadata::{Metadata, MetadataFactory, schema};
use sink::{CountSinkFactory, SinkCounters};

pub fn retry_after_metadata_rejection(wrong_schema: DataType, wrong_split: i32) {
    let backend = SharedBackend::default();
    let metadata = Metadata::new(schema(DataType::Int64), vec![Split { id: 0, start: 1 }]);
    seed(&backend, metadata.clone());
    metadata.reads.store(0, Ordering::SeqCst);
    metadata.resumes.store(0, Ordering::SeqCst);
    metadata.set(
        schema(wrong_schema),
        vec![Split {
            id: wrong_split,
            start: 1,
        }],
    );

    let counts = SinkCounters::default();
    let mut session = Session::open(config(&backend, metadata.clone(), counts.clone())).unwrap();
    declare(&mut session);
    assert!(session.sql("START;").is_err());
    assert_eq!(counts.creates.load(Ordering::SeqCst), 0);
    assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
    assert_eq!(counts.commits.load(Ordering::SeqCst), 0);
    assert_eq!(metadata.reads.load(Ordering::SeqCst), 0);
    assert_eq!(metadata.resumes.load(Ordering::SeqCst), 0);

    metadata.set(schema(DataType::Int64), vec![Split { id: 0, start: 1 }]);
    session.sql("START;").unwrap();
    assert_eq!(counts.creates.load(Ordering::SeqCst), 1);
    assert_eq!(session.checkpoint().unwrap(), 2);
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
    session.shutdown().unwrap();
}

fn config(backend: &SharedBackend, metadata: Metadata, counts: SinkCounters) -> SessionConfig {
    SessionConfig::new()
        .with_source_factory(Arc::new(MetadataFactory(metadata)))
        .with_sink_factory(Arc::new(CountSinkFactory(counts)))
        .with_checkpoint(
            Duration::from_secs(3600),
            DEFAULT_RETAIN,
            Box::new(backend.clone()),
        )
}

fn declare(session: &mut Session) {
    session
        .sql(
            "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
             _event_time AS _event_time - INTERVAL '1 s';",
        )
        .unwrap();
    session
        .sql("CREATE MATERIALIZED VIEW a AS SELECT k FROM src;")
        .unwrap();
    session
        .sql("CREATE SINK out WITH (connector='inmem') AS SELECT * FROM a;")
        .unwrap();
}

fn seed(backend: &SharedBackend, metadata: Metadata) {
    let mut session = Session::open(config(backend, metadata, SinkCounters::default())).unwrap();
    declare(&mut session);
    session.sql("START;").unwrap();
    session.checkpoint().unwrap();
    session.shutdown().unwrap();
}
