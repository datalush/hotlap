//! Versioned export/import of [`Join`]'s retained side relations and join.

use hotlap_core::snapshot::{JoinState, SnapshotTable};

use super::Join;
use crate::arrange::KeyedArrangement;
use crate::core::ipc::{decode_zset, encode_zset};
use crate::error::EngineError;
use crate::keys::KeyConverter;

impl Join {
    /// Exports both side arrangements and the cached per-key join.
    ///
    /// Buffered side deltas live on the enclosing graph node and are filled in
    /// by the graph's export; this method leaves them `None`.
    pub(crate) fn export_state(&self) -> Result<JoinState, EngineError> {
        let left = match &self.left {
            Some(arrangement) => Some(encode_zset(&arrangement.to_zset()?)?),
            None => None,
        };
        let right = match &self.right {
            Some(arrangement) => Some(encode_zset(&arrangement.to_zset()?)?),
            None => None,
        };
        let mut joined: Vec<(Vec<u8>, SnapshotTable)> = Vec::new();
        for (key, batch) in &self.joined {
            joined.push((key.clone(), encode_zset(batch)?));
        }
        joined.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(JoinState {
            left,
            right,
            joined,
            left_pending: None,
            right_pending: None,
        })
    }

    /// Restores both side arrangements and the cached per-key join.
    pub(crate) fn import_state(&mut self, state: &JoinState) -> Result<(), EngineError> {
        self.left = None;
        self.right = None;
        self.left_schema = None;
        self.right_schema = None;
        self.left_conv = None;
        self.right_conv = None;
        self.out_schema = None;
        self.out_conv = None;
        self.joined.clear();

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
        for (key, table) in &state.joined {
            self.joined.insert(key.clone(), decode_zset(table)?);
        }
        #[cfg(test)]
        {
            self.work = 0;
        }
        Ok(())
    }
}
