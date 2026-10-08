//! Periodic checkpoint trigger and the "not configured" on-demand reply.

mod common;

use std::time::{Duration, Instant};

use common::{SharedBackend, pipeline, wait_rows};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;

#[test]
fn periodic_trigger_writes_a_checkpoint() {
    let backend = SharedBackend::default();
    let handle = EngineHandle::start(pipeline(
        backend.clone(),
        Duration::from_millis(25),
        DEFAULT_RETAIN,
    ))
    .unwrap();
    assert!(wait_rows(&handle, &[vec![1, 2], vec![2, 2]]));

    let reader = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut latest = None;
    while Instant::now() < deadline {
        latest = reader.latest().unwrap();
        if latest.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let id = latest.expect("periodic trigger never wrote a checkpoint");
    assert!(reader.read(id).unwrap().engine.epoch >= 1);

    handle.shutdown().unwrap();
}

#[test]
fn checkpoint_without_config_is_rejected() {
    let mut without = pipeline(
        SharedBackend::default(),
        Duration::from_secs(1),
        DEFAULT_RETAIN,
    );
    without.checkpoint = None;
    let handle = EngineHandle::start(without).unwrap();
    assert!(handle.checkpoint().is_err());
    handle.shutdown().unwrap();
}
