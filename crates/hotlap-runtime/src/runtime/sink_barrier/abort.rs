//! Bounded rollback of transactional sink participants.

use std::time::Duration;

use super::SinkBarrier;
use hotlap_connectors::error::ConnectorError;

const ABORT_TIMEOUT: Duration = Duration::from_secs(5);

impl SinkBarrier {
    /// Abort participants under one deadline and preserve the first typed error.
    pub(super) async fn abort_indices(&self, indices: &[usize]) -> Result<(), ConnectorError> {
        if indices.is_empty() {
            return Ok(());
        }
        let mut failure = None;
        let aborts = async {
            for &index in indices {
                if let Err(error) = self.sinks[index].sink().abort().await {
                    failure.get_or_insert(error);
                }
            }
        };
        match tokio::time::timeout(ABORT_TIMEOUT, aborts).await {
            Ok(()) => failure.map_or(Ok(()), Err),
            Err(_) => Err(failure
                .unwrap_or_else(|| ConnectorError::Infrastructure("sink abort timed out".into()))),
        }
    }
}
