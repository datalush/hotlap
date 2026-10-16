//! Versioned export/import of [`Join`]'s retained side relations.
//!
//! The delta join derives each change from the retained sides, so no cached
//! join product is stored or restored.

use hotlap_core::snapshot::JoinState;

use super::Join;
use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::core::ipc::{decode_zset, encode_zset};
use crate::error::EngineError;
use crate::keys::KeyConverter;

impl Join {
    /// Exports both side arrangements; the join product is not retained.
    ///
    /// Buffered side deltas live on the enclosing graph node and are filled in
    /// by the graph's export; this method leaves them `None`.
    pub(crate) fn export_state(&self) -> Result<JoinState, EngineError> {
        Self::export_external_state(self.left.as_ref(), self.right.as_ref(), None, None)
    }

    /// Serializes canonical external arrangements into this view's unchanged
    /// logical v1 join slots, including its independent pending side batches.
    pub(crate) fn export_external_state(
        left: Option<&KeyedArrangement>,
        right: Option<&KeyedArrangement>,
        left_pending: Option<&ZSetBatch>,
        right_pending: Option<&ZSetBatch>,
    ) -> Result<JoinState, EngineError> {
        let left = left
            .map(|arrangement| arrangement.to_zset().and_then(|zset| encode_zset(&zset)))
            .transpose()?;
        let right = right
            .map(|arrangement| arrangement.to_zset().and_then(|zset| encode_zset(&zset)))
            .transpose()?;
        Ok(JoinState {
            left,
            right,
            left_pending: left_pending.map(encode_zset).transpose()?,
            right_pending: right_pending.map(encode_zset).transpose()?,
        })
    }

    /// Restores both side arrangements.
    ///
    pub(crate) fn import_state(&mut self, state: &JoinState) -> Result<(), EngineError> {
        self.left = None;
        self.right = None;
        self.left_schema = None;
        self.right_schema = None;
        self.left_conv = None;
        self.right_conv = None;
        self.out_schema = None;
        self.out_conv = None;

        if let Some(table) = &state.left {
            let zset = decode_zset(table)?;
            let schema = zset.schema();
            let converter = KeyConverter::new(schema.as_ref(), &self.left_keys)?;
            let mut arrangement = KeyedArrangement::new(schema.clone(), &self.left_keys)?;
            arrangement.apply(&zset, &converter)?;
            self.left = Some(arrangement);
            self.left_conv = Some(converter);
            self.left_schema = Some(schema);
        }
        if let Some(table) = &state.right {
            let zset = decode_zset(table)?;
            let schema = zset.schema();
            let converter = KeyConverter::new(schema.as_ref(), &self.right_keys)?;
            let mut arrangement = KeyedArrangement::new(schema.clone(), &self.right_keys)?;
            arrangement.apply(&zset, &converter)?;
            self.right = Some(arrangement);
            self.right_conv = Some(converter);
            self.right_schema = Some(schema);
        }
        #[cfg(test)]
        {
            self.work = 0;
        }
        Ok(())
    }
}
