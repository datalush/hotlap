//! On-demand checkpoint barrier: coherent persistence and readback.

mod common;

use std::time::Duration;

use common::{SharedBackend, pipeline, wait_rows};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::handle::EngineHandle;

#[test]
fn on_demand_checkpoint_is_coherent_and_readable() {
    let backend = SharedBackend::default();
    let handle = EngineHandle::start(pipeline(
        backend.clone(),
        Duration::from_secs(3600),
        DEFAULT_RETAIN,
    ))
    .unwrap();
    assert!(wait_rows(&handle, &[vec![1, 2], vec![2, 2]]));

    let id = handle.checkpoint().unwrap();
    assert_eq!(id, 1);

    let reader = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
    assert_eq!(reader.latest().unwrap(), Some(1));
    let checkpoint = reader.read(id).unwrap();
    assert_eq!(
        checkpoint.engine.format_version,
        hotlap_engine::ENGINE_SNAPSHOT_FORMAT_VERSION
    );
    assert_eq!(
        checkpoint.engine.epoch, 3,
        "engine snapshot lags the barrier"
    );
    assert_eq!(checkpoint.engine.views.len(), 1);
    assert_eq!(
        checkpoint.sources.offsets.get(&0),
        Some(&3),
        "source offsets must match the ingested batches"
    );

    handle.shutdown().unwrap();
}

#[test]
fn handle_metrics_share_engine_counters() {
    let backend = SharedBackend::default();
    let handle = EngineHandle::start(pipeline(
        backend.clone(),
        Duration::from_secs(3600),
        DEFAULT_RETAIN,
    ))
    .unwrap();
    assert!(wait_rows(&handle, &[vec![1, 2], vec![2, 2]]));

    let _ = handle.checkpoint().unwrap();

    let counts = handle.metrics().snapshot();
    assert_eq!(counts.get("rows_ingested"), Some(&4));
    assert!(counts.get("checkpoints_taken").copied().unwrap_or(0) >= 1);

    handle.shutdown().unwrap();
}
