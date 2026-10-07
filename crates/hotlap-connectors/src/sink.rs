//! Engine-owned sink contract (shape only in SP2; no Fluss implementation yet).

use std::pin::Pin;

use futures::Stream;
use hotlap::ChangeBatch;

use crate::error::ConnectorError;

/// A stream of change batches (Z-sets) to write.
pub type ChangeStream = Pin<Box<dyn Stream<Item = Result<ChangeBatch, ConnectorError>> + Send>>;

/// A sink of change batches with 2PC shape (real 2PC lands in SP4).
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    /// Consume the changelog until the stream ends, then return.
    async fn write(&self, changes: ChangeStream) -> Result<(), ConnectorError>;
    /// Commit the data written since the last commit.
    async fn commit(&self) -> Result<(), ConnectorError>;
    /// Drop uncommitted data written since the last commit.
    async fn abort(&self) -> Result<(), ConnectorError>;
}
