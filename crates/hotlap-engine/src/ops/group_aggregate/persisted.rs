//! Read-only materialization of persisted aggregate history for restore checks.

use hotlap_core::ZSetBatch;

use super::GroupAggregate;
use super::columns::render;
use super::materialize::Change;
use crate::error::EngineError;

impl GroupAggregate {
    /// Materializes authoritative groups without changing or reconstructing them.
    pub(crate) fn persisted_rows(&self) -> Result<Option<ZSetBatch>, EngineError> {
        let Some(_schema) = &self.schema else {
            if self.groups.is_empty() {
                return Ok(None);
            }
            return Err(EngineError::Infrastructure(
                "restored groups exist without their schema".into(),
            ));
        };
        self.converter.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("restored group converter missing".into())
        })?;
        let mut changes = Vec::with_capacity(self.groups.len());
        for (key, entry) in &self.groups {
            if entry.rows != 0 {
                changes.push(Change {
                    key: key.clone(),
                    previous: None,
                    current: Some(render(&entry.values)?),
                });
            }
        }
        self.materialize(&changes).map(Some)
    }
}
