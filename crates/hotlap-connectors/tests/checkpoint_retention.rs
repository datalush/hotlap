//! Retention: only the newest configured number of checkpoints survives.

mod common;

use std::time::Duration;

use common::{SharedBackend, pipeline, wait_rows};
use hotlap::state::StateBackend;
use hotlap_connectors::runtime::checkpoint::Checkpointer;
use hotlap_connectors::runtime::handle::EngineHandle;

#[test]
fn retention_keeps_only_the_newest_checkpoints() {
    let backend = SharedBackend::default();
    let handle =
        EngineHandle::start(pipeline(backend.clone(), Duration::from_secs(3600), 2)).unwrap();
    assert!(wait_rows(&handle, &[vec![1, 2], vec![2, 2]]));

    for _ in 0..4 {
        handle.checkpoint().unwrap();
    }

    let reader = Checkpointer::new(Box::new(backend.clone()), 2);
    assert_eq!(reader.latest().unwrap(), Some(4));
    assert!(
        reader.read(1).is_err(),
        "pruned checkpoint is still readable"
    );
    assert!(
        reader.read(2).is_err(),
        "pruned checkpoint is still readable"
    );
    assert!(reader.read(3).is_ok());
    assert!(reader.read(4).is_ok());

    assert!(
        backend.list(b"checkpoint/1/").unwrap().is_empty(),
        "pruned checkpoint 1 left state behind"
    );
    assert!(
        backend.list(b"checkpoint/2/").unwrap().is_empty(),
        "pruned checkpoint 2 left state behind"
    );

    handle.shutdown().unwrap();
}
