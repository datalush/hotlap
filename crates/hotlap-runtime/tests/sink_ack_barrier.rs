//! A checkpoint may only become valid after the writer confirms delivery.

#[path = "sink_ack_barrier/harness.rs"]
mod harness;

use std::time::Duration;

use harness::{GatedSink, RemoteStore, SharedBackend, latest, restart, stage_writes, start};
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};

#[tokio::test]
async fn a_checkpoint_waits_for_the_writer_ack() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let remote = RemoteStore::default();
        let (sink, entered, release) = GatedSink::new(remote.clone(), false);
        let (mut hotlap, pipeline, pump) = start(sink);
        stage_writes(&mut hotlap, &pipeline, &pump).await;

        let backend = SharedBackend::default();
        let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
            .with_sinks(pump.coordinated());
        let take = checkpointer.take(&hotlap, &pipeline.sources);
        tokio::pin!(take);
        tokio::select! {
            _ = entered.notified() => {}
            result = &mut take => panic!("checkpoint finished before the writer ACK: {result:?}"),
        }

        // The flush is pending, so the offsets cannot be certified and no output
        // is visible yet.
        assert_eq!(latest(&backend), None, "no valid checkpoint while pending");
        assert_eq!(remote.total(), 0, "output stays invisible until the ACK");

        release.notify_one();
        let id = take.await.unwrap();
        assert_eq!(latest(&backend), Some(id), "valid after the confirmed ACK");
        let delivered = remote.total();
        assert!(delivered > 0, "output visible after the ACK");

        // Restart: a fresh empty writer against the same output store; the
        // checkpoint still restores its offsets and the output is not lost.
        let (_, pipeline, fresh) = restart(remote.clone());
        let reader = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
            .with_sinks(vec![SinkSync::sink_only(SharedSink::new(fresh))]);
        match Recovery::inspect(&reader, &pipeline.sources).unwrap() {
            RecoveryDecision::Resume(checkpoint) => assert_eq!(checkpoint.id, id),
            other => panic!("a fresh writer must resume the valid checkpoint: {other:?}"),
        }
        assert_eq!(remote.total(), delivered, "output survives the restart");
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn a_failed_writer_ack_publishes_no_valid_checkpoint() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let remote = RemoteStore::default();
        let (sink, entered, release) = GatedSink::new(remote.clone(), true);
        let (mut hotlap, pipeline, pump) = start(sink);
        stage_writes(&mut hotlap, &pipeline, &pump).await;

        let backend = SharedBackend::default();
        let mut checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
            .with_sinks(pump.coordinated());
        let take = checkpointer.take(&hotlap, &pipeline.sources);
        tokio::pin!(take);
        tokio::select! {
            _ = entered.notified() => {}
            result = &mut take => panic!("checkpoint finished before the writer ACK: {result:?}"),
        }
        release.notify_one();
        assert!(take.await.is_err(), "a failed flush must surface");
        assert_eq!(
            latest(&backend),
            None,
            "no valid checkpoint after a failed ACK"
        );
        assert_eq!(remote.total(), 0, "nothing may become visible");
    })
    .await
    .expect("test timed out");
}
