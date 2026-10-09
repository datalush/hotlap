//! Per-delta key bookkeeping: prior output capture and change detection.

use std::collections::HashMap;

use arrow::row::Rows;

use crate::error::EngineError;

use super::GroupAggregate;
use super::columns::{OutputCell, render};
use super::materialize::Change;

/// The materialized prior output row of each distinct key a delta touches.
type PreviousRows = Vec<Option<Vec<OutputCell>>>;

impl GroupAggregate {
    /// Numbers the distinct keys of this delta and renders their prior output.
    ///
    /// The prior state is kept as the materialized output row, never a clone of
    /// the accumulator (which would copy a `min`/`max` multiset), so a delta on
    /// a high-cardinality key stays independent of its distinct values.
    pub(super) fn record_keys(
        &self,
        key_rows: &Rows,
        len: usize,
    ) -> Result<(Vec<Vec<u8>>, PreviousRows), EngineError> {
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut previous: PreviousRows = Vec::new();
        for row in 0..len {
            let bytes = key_rows.row(row).as_ref().to_vec();
            if let std::collections::hash_map::Entry::Vacant(slot) = index.entry(bytes.clone()) {
                let prior = match self.groups.get(&bytes) {
                    Some(entry) => Some(render(&entry.values)?),
                    None => None,
                };
                previous.push(prior);
                keys.push(bytes);
                slot.insert(keys.len() - 1);
            }
        }
        Ok((keys, previous))
    }

    /// Builds the upsert for one key, or `None` when the output did not change.
    ///
    /// The current row is rendered in place; the accumulator is never cloned.
    pub(super) fn change(
        &mut self,
        key: &[u8],
        previous: Option<Vec<OutputCell>>,
    ) -> Result<Option<Change>, EngineError> {
        let keep = matches!(self.groups.get(key), Some(entry) if entry.rows != 0);
        let current = if keep {
            let entry = self.groups.get(key).expect("group checked present");
            Some(render(&entry.values)?)
        } else {
            self.groups.remove(key);
            None
        };
        if previous == current {
            return Ok(None);
        }
        Ok(Some(Change {
            key: key.to_vec(),
            previous,
            current,
        }))
    }
}
