//! Durable, versioned checkpoint of a multi-source runtime.
//!
//! One format serves both single-source and multi-source runtimes: an `HLSR`
//! container header followed by the engine's own framed encoding of the
//! payload. There is no reader for older single-source formats and no migration
//! or fallback path.

mod codec;
mod validate;

use hotlap::InputId;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::SourceState;
use hotlap_engine::EngineSnapshot;

use crate::runtime::sources::Sources;

pub use codec::{decode_sources, encode_sources};

/// One source's durable state inside a [`SourcesCheckpoint`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SavedSource {
    /// Stable input identity, kept as the key the SQL layer also uses.
    pub id: InputId,
    /// Canonical relation name declared for this source.
    pub name: String,
    /// Arrow IPC schema bytes, used to reject an incompatible declaration.
    pub schema: Vec<u8>,
    /// Declared watermark lag, if the source has event-time.
    pub watermark_lag: Option<i64>,
    /// Event-time column index reported by the source.
    pub event_time_column: Option<usize>,
    /// Applied offsets per split, local to this source.
    pub state: SourceState,
}

/// The durable state of every source feeding one runtime.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SourcesCheckpoint {
    /// One entry per source, ordered by input id.
    pub entries: Vec<SavedSource>,
}

impl SourcesCheckpoint {
    /// Capture the applied state, schema and time configuration of `sources`.
    pub fn capture(_sources: &Sources) -> Result<Self, ConnectorError> {
        todo!("red phase")
    }

    /// Validate this checkpoint against the declared `sources` and `engine`.
    pub fn validate(
        &self,
        _sources: &Sources,
        _engine: &EngineSnapshot,
    ) -> Result<(), ConnectorError> {
        todo!("red phase")
    }
}

/// Map an engine codec error onto the connector error type.
pub(crate) fn codec_err(error: hotlap_engine::EngineError) -> ConnectorError {
    ConnectorError::Infrastructure(error.to_string())
}
