//! Engine-owned sink contract with two-phase-commit coordination (SP4).

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
    /// publishes the checkpoint without replay. Running `commit` twice must
    /// therefore not duplicate output: only [`SinkCapabilities::Idempotent`]
    /// sinks qualify by default, because replaying their keyed upserts is safe.
    /// A [`SinkCapabilities::Transactional`] sink must opt in with an explicit
    /// override: its staged writes may not have survived the crash, so
    /// promoting an uncommitted checkpoint would silently lose them. An
    /// [`SinkCapabilities::AtLeastOnce`] sink may already have exposed its
    /// writes and is discarded and replayed instead.
    ///
    /// Override this when a capability understates or overstates the guarantee,
    /// e.g. a transactional sink with durable commit recovery, or an
    /// idempotent-looking sink whose external side effects cannot be repeated.
    fn commit_redriable(&self) -> bool {
        matches!(self.capabilities(), SinkCapabilities::Idempotent)
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

    #[test]
    fn default_redrivability_follows_the_delivery_capability() {
        let sink = |capabilities| Fake { capabilities };
        assert!(
            !sink(SinkCapabilities::Transactional).commit_redriable(),
            "transactional sinks must opt in explicitly"
        );
        assert!(sink(SinkCapabilities::Idempotent).commit_redriable());
        assert!(!sink(SinkCapabilities::AtLeastOnce).commit_redriable());
    }
}
