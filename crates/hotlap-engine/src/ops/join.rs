mod helpers;
mod snapshot;
mod state;

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use arrow::row::RowConverter;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::{KeyConverter, full_converter};
use crate::zset::consolidate_with;

use helpers::{concat_zsets, touched_union};

/// Stateful delta-incremental inner equi-join of two keyed streams.
///
/// Both sides accumulate in [`KeyedArrangement`]s keyed by their join columns.
/// Each `apply` re-evaluates only the join keys touched by the two deltas,
/// emitting `retract old ++ insert new` per key against the stored join, so a
/// push costs `O(sum over touched keys |L[k]| * |R[k]| + |delta|)`, independent
/// of untouched keyspace.
///
/// Output is `left || right` plus the signed `diff` (the product of both diffs).
/// The schema is frozen after the first `apply`, so the side key converters and
/// the output full-row converter are built once and reused across pushes.
pub struct Join {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left: Option<KeyedArrangement>,
    right: Option<KeyedArrangement>,
    joined: HashMap<Vec<u8>, ZSetBatch>,
    left_schema: Option<SchemaRef>,
    right_schema: Option<SchemaRef>,
    out_schema: Option<SchemaRef>,
    left_conv: Option<KeyConverter>,
    right_conv: Option<KeyConverter>,
    out_conv: Option<RowConverter>,
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
            out_schema: None,
            left_conv: None,
            right_conv: None,
            out_conv: None,
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
        let schema = self.output_schema()?;
        if self.out_conv.is_none() {
            self.out_conv = Some(full_converter(schema.as_ref())?);
        }
        let touched = {
            let left_conv = self.left_converter()?;
            let right_conv = self.right_converter()?;
            touched_union(left, left_conv, right, right_conv)?
        };
        let mut parts = Vec::new();
        for key in &touched {
            let delta = self.recompute_key(key, &schema)?;
            if !delta.is_empty() {
                parts.push(delta);
            }
        }
        // Consolidate the per-key parts into one canonical, sorted changelog so
        // the join's output ordering does not depend on touched-key order.
        let out_conv = self.out_converter()?;
        consolidate_with(out_conv, &concat_zsets(&schema, &parts)?)
    }

    /// Number of join pairs re-evaluated by the last `apply` (test only).
    #[cfg(test)]
    pub fn work(&self) -> u64 {
        self.work
    }
}

#[cfg(test)]
mod tests;
