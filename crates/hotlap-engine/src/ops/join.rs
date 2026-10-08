mod helpers;

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;

use helpers::{concat_zsets, equi_join, joined_schema, subtract, touched_union};

/// Stateful delta-incremental inner equi-join of two keyed streams.
///
/// Both sides accumulate in [`KeyedArrangement`]s keyed by their join columns.
/// Each `apply` re-evaluates only the join keys touched by the two deltas,
/// emitting `retract old ++ insert new` per key against the stored join, so a
/// push costs `O(sum over touched keys |L[k]| * |R[k]| + |delta|)`, independent
/// of untouched keyspace.
///
/// Output is `left || right` plus the signed `diff` (the product of both diffs).
pub struct Join {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left: Option<KeyedArrangement>,
    right: Option<KeyedArrangement>,
    joined: HashMap<Vec<u8>, ZSetBatch>,
    left_schema: Option<SchemaRef>,
    right_schema: Option<SchemaRef>,
    #[cfg(test)]
    work: u64,
}

impl Join {
    /// Creates a join over the join column indices `left_keys` and `right_keys`.
    /// Side schemas are learned from the first `apply`.
    pub fn new(left_keys: &[usize], right_keys: &[usize]) -> Self {
        Self {
            left_keys: left_keys.to_vec(),
            right_keys: right_keys.to_vec(),
            left: None,
            right: None,
            joined: HashMap::new(),
            left_schema: None,
            right_schema: None,
            #[cfg(test)]
            work: 0,
        }
    }

    /// Applies one delta per side and returns the join changelog since the last
    /// call. Only the keys touched by these deltas are re-evaluated.
    pub fn apply(&mut self, left: &ZSetBatch, right: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
        if self.left_keys.len() != self.right_keys.len() {
            return Err(EngineError::Unsupported(
                "join sides must share the same key arity".to_string(),
            ));
        }
        self.accumulate(left, right)?;
        #[cfg(test)]
        {
            self.work = 0;
        }
        let touched = touched_union(left, &self.left_keys, right, &self.right_keys)?;
        let schema = self.output_schema()?;
        let mut parts = Vec::new();
        for key in &touched {
            let delta = self.recompute_key(key, &schema)?;
            if !delta.is_empty() {
                parts.push(delta);
            }
        }
        concat_zsets(&schema, &parts)
    }

    /// Number of join pairs re-evaluated by the last `apply` (test only).
    #[cfg(test)]
    pub fn work(&self) -> u64 {
        self.work
    }

    /// Re-evaluates one join key and returns the delta against the stored join.
    fn recompute_key(&mut self, key: &[u8], schema: &SchemaRef) -> Result<ZSetBatch, EngineError> {
        let left = self
            .left
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("left arrangement uninitialized".into()))?;
        let left_slice = left.to_zset_for_key(key)?;
        let right = self
            .right
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("right arrangement uninitialized".into()))?;
        let right_slice = right.to_zset_for_key(key)?;
        let (current, pairs) =
            equi_join(&left_slice, &self.left_keys, &right_slice, &self.right_keys)?;
        #[cfg(test)]
        {
            self.work += pairs as u64;
        }
        #[cfg(not(test))]
        let _ = pairs;
        let previous = self
            .joined
            .remove(key)
            .unwrap_or_else(|| ZSetBatch::empty(schema.clone()));
        let delta = subtract(&current, &previous)?;
        if !current.is_empty() {
            self.joined.insert(key.to_vec(), current);
        }
        Ok(delta)
    }

    /// Joined output schema (`left || right`), learned from the first apply.
    fn output_schema(&self) -> Result<SchemaRef, EngineError> {
        match (&self.left_schema, &self.right_schema) {
            (Some(left), Some(right)) => Ok(joined_schema(left, right)),
            _ => Err(EngineError::Infrastructure(
                "join schemas uninitialized".to_string(),
            )),
        }
    }

    /// Adds each side's delta to its arrangement, creating the arrangements from
    /// the incoming schemas on the first call.
    fn accumulate(&mut self, left: &ZSetBatch, right: &ZSetBatch) -> Result<(), EngineError> {
        if self.left.is_none() {
            self.left = Some(KeyedArrangement::new(left.schema(), &self.left_keys)?);
            self.left_schema = Some(left.schema());
        }
        if self.right.is_none() {
            self.right = Some(KeyedArrangement::new(right.schema(), &self.right_keys)?);
            self.right_schema = Some(right.schema());
        }
        let left_keys = KeyConverter::new(left.schema().as_ref(), &self.left_keys)?;
        let right_keys = KeyConverter::new(right.schema().as_ref(), &self.right_keys)?;
        let left_arrangement = self
            .left
            .as_mut()
            .ok_or_else(|| EngineError::Infrastructure("left arrangement uninitialized".into()))?;
        let right_arrangement = self
            .right
            .as_mut()
            .ok_or_else(|| EngineError::Infrastructure("right arrangement uninitialized".into()))?;
        left_arrangement.apply(left, &left_keys)?;
        right_arrangement.apply(right, &right_keys)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
