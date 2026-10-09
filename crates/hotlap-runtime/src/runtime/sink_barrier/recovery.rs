//! Sink capability decisions for interrupted checkpoints.

use super::SinkBarrier;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::sink::SinkCapabilities;

impl SinkBarrier {
    /// Whether every coordinated sink declares its `commit` re-drivable.
    pub fn redriable(&self) -> bool {
        self.sinks.iter().all(|sync| sync.sink().commit_redriable())
    }

    /// Whether every coordinated sink tolerates replay after an interrupted
    /// commit; transactional output is never replay-safe.
    pub fn replay_safe(&self) -> bool {
        self.sinks
            .iter()
            .all(|sync| sync.sink().capabilities() != SinkCapabilities::Transactional)
    }

    /// Re-drive `commit` for every sink after an interrupted commit.
    pub async fn redrive_commit(&self) -> Result<(), ConnectorError> {
        for sync in &self.sinks {
            sync.sink().commit().await?;
        }
        Ok(())
    }
}
