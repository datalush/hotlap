//! Typed set of input sources, each carrying a stable identity.

mod streams;

use std::collections::HashSet;
use std::sync::Arc;

use hotlap::InputId;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, Split};

use crate::runtime::pipeline::Watermark;

pub use streams::{InputStream, SourceEvent};

/// One registered input: its identity, relation name and source instance.
pub struct InputSource {
    pub id: InputId,
    pub name: String,
    pub source: Arc<dyn Source>,
    pub watermark: Option<Watermark>,
}

/// An ordered, validated set of the inputs feeding one runtime.
pub struct Sources {
    entries: Vec<InputSource>,
}

impl Sources {
    /// Validate `entries` (non-empty, unique names and ids) and order them by id.
    pub fn new(mut entries: Vec<InputSource>) -> Result<Self, ConnectorError> {
        if entries.is_empty() {
            return Err(ConnectorError::Unsupported("no input sources".into()));
        }
        let mut names = HashSet::new();
        let mut ids = HashSet::new();
        for entry in &entries {
            if !names.insert(entry.name.clone()) {
                let name = entry.name.clone();
                return Err(ConnectorError::Unsupported(format!(
                    "duplicate source name: {name}"
                )));
            }
            if !ids.insert(entry.id) {
                let id = entry.id.0;
                return Err(ConnectorError::Unsupported(format!(
                    "duplicate source id: {id}"
                )));
            }
        }
        entries.sort_by_key(|entry| entry.id);
        Ok(Self { entries })
    }

    /// Every registered input, ordered by id.
    pub fn entries(&self) -> &[InputSource] {
        &self.entries
    }

    /// The input registered with `id`, or an explicit error when unknown.
    ///
    /// Unknown ids must never fall back to the first source: that would silently
    /// reroute a relation into another input.
    pub fn get(&self, id: InputId) -> Result<&InputSource, ConnectorError> {
        self.entries
            .iter()
            .find(|entry| entry.id == id)
            .ok_or_else(|| ConnectorError::Unsupported(format!("unknown source id: {}", id.0)))
    }

    /// Merge every split of every input into one identity-tagged stream.
    pub fn stream(&self) -> Result<InputStream, ConnectorError> {
        streams::tagged_stream(&self.entries)
    }

    /// Merge one explicitly resolved split list per input, in entry order.
    ///
    /// Recovery uses this with the splits a source returned from `resume`, so
    /// the merge reads exactly the restored offsets and never re-opens a source.
    pub fn stream_with(&self, splits: &[Vec<Split>]) -> Result<InputStream, ConnectorError> {
        streams::tagged_from(&self.entries, splits)
    }
}
