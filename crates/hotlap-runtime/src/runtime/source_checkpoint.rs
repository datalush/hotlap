//! Durable, versioned checkpoint of a multi-source runtime.
//!
//! One format serves both single-source and multi-source runtimes: an `HLSR`
//! container header followed by the engine's own framed encoding of the
//! payload. There is no reader for older single-source formats and no migration
//! or fallback path.

mod codec;
mod validate;

use hotlap::{InputId, Plan, ViewId};
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::SourceState;
use hotlap_engine::{EngineSnapshot, encode_schema};

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

/// One view's durable identity inside a [`SourcesCheckpoint`].
///
/// The engine snapshot stores a plan per numeric handle but not the
/// caller-facing name, so this registry records the name↔handle↔plan
/// association the view was captured with.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SavedView {
    /// Caller-facing view name.
    pub name: String,
    /// Numeric handle the name was bound to.
    pub id: ViewId,
    /// The plan the handle was compiled from.
    pub plan: Plan,
}

impl SavedView {
    /// Convert a facade registry (`name`, handle, plan) into saved views.
    pub fn from_registry(registry: &[(String, ViewId, Plan)]) -> Vec<Self> {
        registry
            .iter()
            .map(|(name, id, plan)| Self {
                name: name.clone(),
                id: *id,
                plan: plan.clone(),
            })
            .collect()
    }
}

/// The durable state of every source feeding one runtime, plus the named view
/// registry needed to bind restored state back to the same declarations.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SourcesCheckpoint {
    /// One entry per source, ordered by input id.
    pub entries: Vec<SavedSource>,
    /// One entry per declared view, ordered by handle.
    pub views: Vec<SavedView>,
}

impl SourcesCheckpoint {
    /// Capture the applied state, schema and time configuration of `sources`.
    ///
    /// Entries follow the id order of [`Sources`], so two sources that both
    /// use split 0 keep separate applied maps and never merge their offsets.
    pub fn capture(sources: &Sources) -> Result<Self, ConnectorError> {
        let mut entries = Vec::with_capacity(sources.entries().len());
        for input in sources.entries() {
            let schema = encode_schema(&input.source.schema()).map_err(encode_err)?;
            entries.push(SavedSource {
                id: input.id,
                name: input.name.clone(),
                schema,
                watermark_lag: input.watermark.map(|watermark| watermark.lag),
                event_time_column: input.source.event_time_column(),
                state: input.source.state(),
            });
        }
        Ok(Self {
            entries,
            views: Vec::new(),
        })
    }

    /// Capture the applied source state plus the live named view registry.
    ///
    /// `views` is the declared view set, so a restart can prove each restored
    /// handle still belongs to the same declaration instead of trusting schema
    /// alone.
    pub fn capture_with_views(
        sources: &Sources,
        views: &[SavedView],
    ) -> Result<Self, ConnectorError> {
        let mut checkpoint = Self::capture(sources)?;
        let mut saved = views.to_vec();
        saved.sort_by_key(|view| view.id);
        checkpoint.views = saved;
        Ok(checkpoint)
    }

    /// Validate this checkpoint against the declared `sources` and `engine`.
    pub fn validate(
        &self,
        sources: &Sources,
        engine: &EngineSnapshot,
    ) -> Result<(), ConnectorError> {
        validate::validate(self, sources, engine)
    }

    /// Validate the saved named views against `declared` and the `engine`.
    pub fn validate_views(
        &self,
        declared: &[SavedView],
        engine: &EngineSnapshot,
    ) -> Result<(), ConnectorError> {
        validate::validate_views(self, declared, engine)
    }

    /// The saved entry for `id`, or an explicit error when it is absent.
    ///
    /// Recovery already validates the id set, so a missing entry here means the
    /// caller skipped validation; it must never fall back to another source.
    pub fn entry(&self, id: InputId) -> Result<&SavedSource, ConnectorError> {
        self.entries
            .iter()
            .find(|entry| entry.id == id)
            .ok_or_else(|| {
                ConnectorError::Unsupported(format!("checkpoint has no source {}", id.0))
            })
    }
}

/// Map an engine encode error onto the connector error type.
///
/// Encoding runs while capturing live state, so a failure is internal, never
/// persisted corruption that recovery could tolerate.
pub(crate) fn encode_err(error: hotlap_engine::EngineError) -> ConnectorError {
    match error {
        hotlap_engine::EngineError::Unsupported(message) => ConnectorError::Unsupported(message),
        error => ConnectorError::Infrastructure(error.to_string()),
    }
}

/// Map an engine decode error onto the connector error type.
///
/// [`EngineError::Unsupported`] (an unknown inner frame or snapshot version)
/// stays `Unsupported`, so recovery can treat an incompatible format as fatal
/// instead of mistaking it for tolerated current-format corruption.
pub(crate) fn decode_err(error: hotlap_engine::EngineError) -> ConnectorError {
    match error {
        hotlap_engine::EngineError::Unsupported(message) => ConnectorError::Unsupported(message),
        error => ConnectorError::Corruption(error.to_string()),
    }
}
