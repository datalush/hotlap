//! Versioned export/import of [`GroupCount`]'s retained counts.

use hotlap_core::snapshot::GroupState;

use super::GroupCount;
use crate::core::ipc::{decode_schema, encode_schema};
use crate::error::EngineError;
use crate::keys::converter_for;

impl GroupCount {
    /// Exports the schema and live counts in deterministic key order.
    pub(crate) fn export_state(&self) -> Result<GroupState, EngineError> {
        let schema = match &self.schema {
            Some(schema) => Some(encode_schema(schema)?),
            None => None,
        };
        let mut counts: Vec<(Vec<u8>, i64)> = self
            .counts
            .iter()
            .map(|(key, &count)| (key.clone(), count))
            .collect();
        counts.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(GroupState { schema, counts })
    }

    /// Restores the schema and counts, resetting the test-only work counter.
    pub(crate) fn import_state(&mut self, state: &GroupState) -> Result<(), EngineError> {
        self.counts.clear();
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
        for (key, count) in &state.counts {
            self.counts.insert(key.clone(), *count);
        }
        #[cfg(test)]
        {
            self.work = 0;
        }
        Ok(())
    }
}
