//! Engine-owned sink contract with two-phase-commit coordination.

use std::pin::Pin;

use futures::Stream;
use hotlap::ZSetBatch;

use crate::error::ConnectorError;

/// A stream of change batches (Z-sets) to write.
pub type ChangeStream = Pin<Box<dyn Stream<Item = Result<ZSetBatch, ConnectorError>> + Send>>;

/// Strongest delivery guarantee a sink can offer, chosen by the checkpoint
/// barrier to adapt the two-phase-commit protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkCapabilities {
    /// Supports `prepare`/`commit`/`abort`: the barrier commits atomically, so
    /// the run is exactly-once when the whole protocol succeeds.
    Transactional,
    /// Writes are keyed upserts (e.g. a primary-key table), so replaying after a
    /// crash is safe: the barrier skips `prepare` and only flushes on commit.
    Idempotent,
    /// Writes are visible immediately and cannot be rolled back, so the barrier
    /// does not coordinate them and replay may duplicate (at-least-once).
    AtLeastOnce,
}

/// A sink of change batches with a two-phase-commit shape.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    /// Consume the changelog until the stream ends, then return.
    async fn write(&self, changes: ChangeStream) -> Result<(), ConnectorError>;
    /// Delivery guarantee this sink offers; defaults to [`SinkCapabilities::AtLeastOnce`].
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
    }
    /// Whether [`Sink::commit`] may be safely re-driven after a restart.
    ///
    /// Recovery finds a checkpoint whose body is complete but whose `valid`
    /// marker never landed (the process stopped mid-commit). When every sink
    /// declares its commit re-drivable, recovery re-drives `commit` and
    /// publishes the checkpoint without replay.
    ///
    /// Defaults to `false`: replaying idempotent writes is safe, but that does
    /// not make an interrupted commit completable after a restart. A sink that
    /// only queues writes in memory (as Fluss does) loses its queue when the
    /// process dies, so re-driving a new instance's commit cannot deliver the
    /// lost writes. Override to `true` only when the sink holds durable staged
    /// state or its re-driven commit is truly a no-op, and `commit` tolerates
    /// running more than once.
    fn commit_redriable(&self) -> bool {
        false
    }
    /// First phase of two-phase commit.
    ///
    /// Called before the checkpoint body is written and before the engine
    /// resumes. A [`SinkCapabilities::Transactional`] sink must flush pending
    /// writes and block new ones until [`Sink::commit`] or [`Sink::abort`];
    /// other sinks keep the default no-op.
    async fn prepare(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    /// Commit the data written since the last commit.
    ///
    /// Must tolerate running more than once: recovery re-drives it after an
    /// interrupted commit when [`Sink::commit_redriable`] holds.
    async fn commit(&self) -> Result<(), ConnectorError>;
    /// Drop uncommitted data written since the last commit.
    async fn abort(&self) -> Result<(), ConnectorError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct Fake {
        capabilities: SinkCapabilities,
    }

    #[async_trait::async_trait]
    impl Sink for Fake {
        async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn capabilities(&self) -> SinkCapabilities {
            self.capabilities
        }

        async fn commit(&self) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn abort(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    /// An idempotent sink that only queues writes in memory before flushing.
    struct VolatileQueueSink {
        queued: Mutex<Vec<i64>>,
    }

    impl VolatileQueueSink {
        fn new() -> Self {
            Self {
                queued: Mutex::new(Vec::new()),
            }
        }

        fn enqueue(&self, value: i64) {
            self.queued.lock().unwrap().push(value);
        }

        fn queued(&self) -> usize {
            self.queued.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl Sink for VolatileQueueSink {
        async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn capabilities(&self) -> SinkCapabilities {
            SinkCapabilities::Idempotent
        }

        async fn commit(&self) -> Result<(), ConnectorError> {
            self.queued.lock().unwrap().clear();
            Ok(())
        }

        async fn abort(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    /// A sink whose staged writes survive a restart, so its commit is safe to
    /// re-drive after an interrupted commit.
    struct DurableStagedSink;

    #[async_trait::async_trait]
    impl Sink for DurableStagedSink {
        async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn capabilities(&self) -> SinkCapabilities {
            SinkCapabilities::Idempotent
        }

        fn commit_redriable(&self) -> bool {
            true
        }

        async fn commit(&self) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn abort(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[test]
    fn commit_is_not_redrivable_by_default_for_any_capability() {
        let sink = |capabilities| Fake { capabilities };
        assert!(
            !sink(SinkCapabilities::Transactional).commit_redriable(),
            "transactional sinks must opt in explicitly"
        );
        assert!(
            !sink(SinkCapabilities::Idempotent).commit_redriable(),
            "idempotent replay does not make an interrupted commit completable"
        );
        assert!(!sink(SinkCapabilities::AtLeastOnce).commit_redriable());
    }

    #[test]
    fn a_sink_with_durable_staged_state_may_opt_into_redriving() {
        assert!(DurableStagedSink.commit_redriable());
    }

    #[test]
    fn a_new_instance_cannot_complete_a_volatile_queue_commit() {
        let before_crash = VolatileQueueSink::new();
        before_crash.enqueue(1);
        before_crash.enqueue(2);
        assert_eq!(before_crash.queued(), 2);

        // A restart hands out a new sink instance with an empty queue, so
        // re-driving its commit after the crash cannot deliver the lost writes.
        let after_restart = VolatileQueueSink::new();
        assert_eq!(after_restart.queued(), 0, "the old queue is gone");
        assert!(
            !after_restart.commit_redriable(),
            "a volatile queue must not declare its commit re-drivable"
        );
    }
}
