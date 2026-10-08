mod helpers;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;

use helpers::{snapshot, subtract};

/// Stateful incremental inner equi-join of two keyed streams.
///
/// Both sides accumulate in [`KeyedArrangement`]s keyed by their join columns,
/// so retractions update the stored state. Each `apply` feeds one delta per side,
/// recomputes the join, and emits the changelog against the previous relation.
///
/// Output is `left || right` plus the signed `diff` (the product of both diffs).
/// Complexity: `apply` recomputes from both arrangements, `O(|L| * |R|)` per
/// delta. TODO: incremental per-key join (out of 7c scope).
pub struct Join {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left: Option<KeyedArrangement>,
    right: Option<KeyedArrangement>,
    previous: Option<ZSetBatch>,
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
            previous: None,
        }
    }

    /// Applies one delta per side and returns the join changelog since the last
    /// call. The first call emits the full join snapshot.
    pub fn apply(&mut self, left: &ZSetBatch, right: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
        if self.left_keys.len() != self.right_keys.len() {
            return Err(EngineError::Unsupported(
                "join sides must share the same key arity".to_string(),
            ));
        }
        self.accumulate(left, right)?;
        let left_arrangement = self.left.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("left arrangement uninitialized".to_string())
        })?;
        let right_arrangement = self.right.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("right arrangement uninitialized".to_string())
        })?;
        let current = snapshot(
            left_arrangement,
            &self.left_keys,
            right_arrangement,
            &self.right_keys,
        )?;
        let delta = match &self.previous {
            None => current.clone(),
            Some(previous) => subtract(&current, previous)?,
        };
        self.previous = Some(current);
        Ok(delta)
    }

    /// Adds each side's delta to its arrangement, creating the arrangements from
    /// the incoming schemas on the first call.
    fn accumulate(&mut self, left: &ZSetBatch, right: &ZSetBatch) -> Result<(), EngineError> {
        if self.left.is_none() {
            self.left = Some(KeyedArrangement::new(left.schema(), &self.left_keys)?);
        }
        if self.right.is_none() {
            self.right = Some(KeyedArrangement::new(right.schema(), &self.right_keys)?);
        }
        let left_keys = KeyConverter::new(left.schema().as_ref(), &self.left_keys)?;
        let right_keys = KeyConverter::new(right.schema().as_ref(), &self.right_keys)?;
        let left_arrangement = self.left.as_mut().ok_or_else(|| {
            EngineError::Infrastructure("left arrangement uninitialized".to_string())
        })?;
        let right_arrangement = self.right.as_mut().ok_or_else(|| {
            EngineError::Infrastructure("right arrangement uninitialized".to_string())
        })?;
        left_arrangement.apply(left, &left_keys)?;
        right_arrangement.apply(right, &right_keys)?;
        Ok(())
    }
}
