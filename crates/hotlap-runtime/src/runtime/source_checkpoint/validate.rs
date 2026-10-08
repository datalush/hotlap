//! Cross-checks a [`SourcesCheckpoint`] against sources and engine snapshot.

use hotlap_connectors::error::ConnectorError;
use hotlap_engine::EngineSnapshot;

use crate::runtime::sources::Sources;

use super::SourcesCheckpoint;

/// Validate `checkpoint` against `sources` and `engine`.
pub(super) fn validate(
    _checkpoint: &SourcesCheckpoint,
    _sources: &Sources,
    _engine: &EngineSnapshot,
) -> Result<(), ConnectorError> {
    todo!("red phase")
}
