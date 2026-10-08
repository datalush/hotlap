use std::sync::Arc;

use arrow::array::Int64Array;

use super::{KeyedArrangement, decode};
use crate::batch::ZSetBatch;
use crate::error::EngineError;

impl KeyedArrangement {
    /// Materializes only the surviving entries whose key is `key` (byte form).
    ///
    /// Returns an empty batch over the arrangement schema when the key is
    /// absent, so callers can subtract it from a stored per-key state directly.
    pub fn to_zset_for_key(&self, key: &[u8]) -> Result<ZSetBatch, EngineError> {
        let Some(payloads) = self.state.get(key) else {
            return self.empty_zset();
        };
        let mut entries: Vec<(&[u8], i64)> = payloads
            .iter()
            .map(|(payload, &diff)| (payload.as_ref(), diff))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let keys: Vec<&[u8]> = vec![key; entries.len()];
        let payloads: Vec<&[u8]> = entries.iter().map(|entry| entry.0).collect();
        let key_arrays = decode(&self.key_converter, &keys)?;
        let payload_arrays = decode(&self.payload_converter, &payloads)?;
        let batch = self.assemble(&key_arrays, &payload_arrays)?;
        let diffs: Vec<i64> = entries.iter().map(|entry| entry.1).collect();
        Ok(ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs)))?)
    }
}
