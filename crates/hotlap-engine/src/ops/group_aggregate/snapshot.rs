//! Versioned export/import of [`GroupAggregate`]'s retained groups.

use hotlap_core::snapshot::{GroupEntry, GroupState};

use super::GroupAggregate;
use super::accumulate::empty_entry;
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
        // Derived cut metadata is rebuilt from validated canonical source state.
        self.maintained_cut = None;
        let (schema, converter) = match &state.schema {
            Some(bytes) => {
                let schema = decode_schema(bytes)?;
                let converter = converter_for(&schema, &self.key)?;
                (Some(schema), Some(converter))
            }
            None => (None, None),
        };
        if state.schema.is_none() && !state.groups.is_empty() {
            return Err(snapshot_shape_error("groups exist without a schema"));
        }
        let expected = schema
            .as_ref()
            .map(|schema| empty_entry(&self.aggs, schema))
            .transpose()?;
        for (_, entry) in &state.groups {
            let Some(expected) = &expected else {
                return Err(snapshot_shape_error("group schema is missing"));
            };
            if entry.values.len() != expected.values.len()
                || entry
                    .values
                    .iter()
                    .zip(&expected.values)
                    .any(|(actual, expected)| !same_accumulator_shape(actual, expected))
            {
                return Err(snapshot_shape_error("aggregate accumulator shape mismatch"));
            }
        }
        self.groups.clear();
        self.schema = schema;
        self.converter = converter;
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

fn same_accumulator_shape(
    actual: &hotlap_core::snapshot::AggValue,
    expected: &hotlap_core::snapshot::AggValue,
) -> bool {
    use hotlap_core::snapshot::AggValue;
    matches!(
        (actual, expected),
        (AggValue::Count(_), AggValue::Count(_))
            | (AggValue::SumInteger { .. }, AggValue::SumInteger { .. })
            | (AggValue::SumFloat { .. }, AggValue::SumFloat { .. })
            | (AggValue::Avg { .. }, AggValue::Avg { .. })
            | (AggValue::Min(_), AggValue::Min(_))
            | (AggValue::Max(_), AggValue::Max(_))
    )
}

fn snapshot_shape_error(detail: &str) -> EngineError {
    EngineError::Infrastructure(format!("invalid restored GroupAggregate state: {detail}"))
}
