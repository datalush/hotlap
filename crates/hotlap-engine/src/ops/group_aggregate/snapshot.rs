//! Versioned export/import of [`GroupAggregate`]'s retained groups.

use hotlap_core::snapshot::{GroupEntry, GroupState};

use super::GroupAggregate;
use crate::core::ipc::{decode_schema, encode_schema};
use crate::error::EngineError;
use crate::keys::converter_for;

impl GroupAggregate {
    /// Exports the schema and live groups in deterministic key order.
    pub(crate) fn export_state(&self) -> Result<GroupState, EngineError> {
        let schema = match &self.schema {
            Some(schema) => Some(encode_schema(schema)?),
            None => None,
        };
        let mut groups: Vec<(Vec<u8>, GroupEntry)> = self
            .groups
            .iter()
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect();
        groups.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(GroupState { schema, groups })
    }

    /// Restores the schema and groups, resetting the test-only work counter.
    pub(crate) fn import_state(&mut self, state: &GroupState) -> Result<(), EngineError> {
        self.groups.clear();
        match &state.schema {
            Some(bytes) => {
                let schema = decode_schema(bytes)?;
                self.converter = Some(converter_for(&schema, &self.key)?);
                self.schema = Some(schema);
            }
            None => {
                self.converter = None;
                self.schema = None;
            }
        }
        for (key, entry) in &state.groups {
            self.groups.insert(key.clone(), entry.clone());
        }
        #[cfg(test)]
        {
            self.work = 0;
        }
        Ok(())
    }
}
