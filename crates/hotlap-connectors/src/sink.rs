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
    /// Explicit stable binding name for a pipeline sink, when supplied by the caller.
    fn binding_name(&self) -> Option<&str> {
        None
    }

    /// Stable opaque identity of the physical output target.
    ///
    /// Durable recovery rejects sinks without a non-empty target identity;
    /// factory declarations or SQL sink names do not prove which resource was
    /// actually opened.
    fn physical_identity(&self) -> Option<String> {
        None
    }
    /// Consume the changelog until the stream ends, then return.
    async fn write(&self, changes: ChangeStream) -> Result<(), ConnectorError>;
    /// Delivery guarantee this sink offers; defaults to [`SinkCapabilities::AtLeastOnce`].
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
    }
    /// Whether the sink can apply retractions (negative diffs).
    ///
    /// Defaults to `false`: a sink that has not declared support is treated as
    /// append-only, so the runtime refuses a plan that may retract (e.g. a
    /// grouped aggregate) before any write or ingestion.
    /// Override to `true` only when the sink truly handles deletes.
    fn accepts_retractions(&self) -> bool {
        false
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
