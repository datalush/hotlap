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
    async fn commit(&self) -> Result<(), ConnectorError>;
    /// Drop uncommitted data written since the last commit.
    async fn abort(&self) -> Result<(), ConnectorError>;
}
