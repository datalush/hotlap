//! Versioned binary container for [`SourcesCheckpoint`].

use hotlap_connectors::error::ConnectorError;

use super::SourcesCheckpoint;

/// Encode `value` as `HLSR` + version + the engine's framed payload.
pub fn encode_sources(_value: &SourcesCheckpoint) -> Result<Vec<u8>, ConnectorError> {
    todo!("red phase")
}

/// Decode a sources checkpoint, rejecting foreign magic, versions or corruption.
pub fn decode_sources(_bytes: &[u8]) -> Result<SourcesCheckpoint, ConnectorError> {
    todo!("red phase")
}
