//! Durable identity manifest captured before a sink prepare can have effects.

use hotlap::InputId;
use hotlap_connectors::error::ConnectorError;
use hotlap_engine::{decode_framed, encode_framed};

use crate::runtime::pipeline::SinkDescription;
use crate::runtime::sink_barrier::SinkBarrier;
use crate::runtime::source_checkpoint::SavedView;
use crate::runtime::sources::Sources;
use hotlap_engine::EngineSnapshot;

const MAGIC: [u8; 4] = *b"HLPM";
const VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ParticipantsManifest {
    pub sources: Vec<SavedPhysicalSource>,
    pub sinks: Vec<SavedSink>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SavedPhysicalSource {
    pub id: InputId,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SavedSink {
    pub binding_name: String,
    pub view: String,
    pub target: String,
    pub capabilities: String,
    pub accepts_retractions: bool,
    pub redriable: bool,
}

impl ParticipantsManifest {
    pub fn capture(sources: &Sources, sinks: &SinkBarrier) -> Result<Self, ConnectorError> {
        let sources = sources
            .entries()
            .iter()
            .map(|source| {
                let identity = source
                    .source
                    .physical_identity()
                    .filter(|identity| !identity.trim().is_empty())
                    .ok_or_else(|| {
                        ConnectorError::Unsupported(format!(
                            "source `{}` has no stable physical dataset identity",
                            source.name
                        ))
                    })?;
                Ok(SavedPhysicalSource {
                    id: source.id,
                    identity,
                })
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        Ok(Self {
            sources,
            sinks: sinks.participants()?,
        })
    }

    pub fn matches_sources(&self, sources: &Sources) -> Result<(), ConnectorError> {
        let current = sources
            .entries()
            .iter()
            .map(|source| {
                let identity = source
                    .source
                    .physical_identity()
                    .filter(|identity| !identity.trim().is_empty())
                    .ok_or_else(|| {
                        ConnectorError::Unsupported("source identity is absent".into())
                    })?;
                Ok(SavedPhysicalSource {
                    id: source.id,
                    identity,
                })
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        if self.sources != current {
            return Err(ConnectorError::Unsupported(
                "physical source participants do not match the checkpoint manifest".into(),
            ));
        }
        Ok(())
    }

    pub fn matches_saved_sources(
        &self,
        sources: &[crate::runtime::source_checkpoint::SavedSource],
    ) -> Result<(), ConnectorError> {
        let saved: Vec<_> = sources
            .iter()
            .map(|source| SavedPhysicalSource {
                id: source.id,
                identity: source.physical_identity.clone(),
            })
            .collect();
        if self.sources != saved {
            return Err(ConnectorError::Unsupported(
                "source checkpoint and participant manifest disagree".into(),
            ));
        }
        Ok(())
    }

    pub fn matches_sinks(&self, sinks: &SinkBarrier) -> Result<(), ConnectorError> {
        let current = sinks.participants()?;
        if self.sinks != current {
            return Err(ConnectorError::Unsupported(format!(
                "sink participants do not match the checkpoint manifest: saved={:?}, current={current:?}",
                self.sinks
            )));
        }
        Ok(())
    }

    pub fn matches_sink_descriptions(
        &self,
        descriptions: &[SinkDescription],
    ) -> Result<(), ConnectorError> {
        let current = descriptions
            .iter()
            .map(|description| {
                description.validate()?;
                Ok(SavedSink {
                    binding_name: description.binding_name.clone(),
                    view: description.view.clone(),
                    target: description.physical_identity.clone(),
                    capabilities: format!("{:?}", description.capabilities),
                    accepts_retractions: description.accepts_retractions,
                    redriable: description.commit_redriable,
                })
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        if self.sinks != current {
            return Err(ConnectorError::Unsupported(
                "sink descriptions do not match the checkpoint participant manifest".into(),
            ));
        }
        Ok(())
    }

    /// Ensure every declared sink's named view is present, identity-matched,
    /// and captured with its changelog tap enabled.
    pub fn validate_tapped_views(
        &self,
        views: &[SavedView],
        snapshot: &EngineSnapshot,
    ) -> Result<(), ConnectorError> {
        let mut view_ids = std::collections::BTreeSet::new();
        let mut view_names = std::collections::BTreeSet::new();
        for saved in views {
            if !view_ids.insert(saved.id) || !view_names.insert(saved.name.as_str()) {
                return Err(ConnectorError::Unsupported(
                    "saved view registry has duplicate names or ids".into(),
                ));
            }
        }

        let mut snapshot_views = std::collections::BTreeMap::new();
        for view in &snapshot.views {
            if snapshot_views.insert(view.id, view).is_some() {
                return Err(ConnectorError::Unsupported(
                    "engine snapshot has duplicate view ids".into(),
                ));
            }
        }
        for saved in views {
            let Some(view) = snapshot_views.get(&saved.id) else {
                return Err(ConnectorError::Unsupported(format!(
                    "saved view `{}` is missing from the engine snapshot",
                    saved.name
                )));
            };
            if view.plan != saved.plan {
                return Err(ConnectorError::Unsupported(format!(
                    "saved view `{}` does not match its engine snapshot plan",
                    saved.name
                )));
            }
        }

        for sink in &self.sinks {
            let Some(saved) = views.iter().find(|view| view.name == sink.view) else {
                return Err(ConnectorError::Unsupported(format!(
                    "sink `{}` references missing view `{}`",
                    sink.binding_name, sink.view
                )));
            };
            if !snapshot_views[&saved.id].tapped {
                return Err(ConnectorError::Unsupported(format!(
                    "sink `{}` references untapped view `{}` in the checkpoint",
                    sink.binding_name, sink.view
                )));
            }
        }
        Ok(())
    }
}

pub(crate) fn encode(manifest: &ParticipantsManifest) -> Result<Vec<u8>, ConnectorError> {
    let payload = encode_framed(manifest).map_err(|error| match error {
        hotlap_engine::EngineError::Unsupported(message) => ConnectorError::Unsupported(message),
        error => ConnectorError::Infrastructure(error.to_string()),
    })?;
    let mut bytes = Vec::with_capacity(8 + payload.len());
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<ParticipantsManifest, ConnectorError> {
    if bytes.get(..4) != Some(MAGIC.as_slice()) || bytes.len() < 8 {
        return Err(ConnectorError::Unsupported(
            "participant manifest is missing or has an unknown format".into(),
        ));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap_or_default());
    if version != VERSION {
        return Err(ConnectorError::Unsupported(format!(
            "unknown participant manifest version {version}"
        )));
    }
    decode_framed(&bytes[8..]).map_err(|error| match error {
        hotlap_engine::EngineError::Unsupported(message) => ConnectorError::Unsupported(message),
        error => ConnectorError::Corruption(error.to_string()),
    })
}

pub(crate) fn sink_identity(
    sink: &crate::runtime::shared_sink::SharedSink,
) -> Result<String, ConnectorError> {
    sink.physical_identity()
        .filter(|identity| !identity.trim().is_empty())
        .ok_or_else(|| ConnectorError::Unsupported("sink has no physical target identity".into()))
}
