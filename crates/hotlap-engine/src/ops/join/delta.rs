//! Delta join within one key.
//!
//! Rather than materializing `L[k] x R[k]` and diffing it against a stored copy,
//! the change to the join is computed from this push's per-key deltas and the
//! previously retained per-key sides:
//!
//! `delta = dL x R_prev + L_prev x dR + dL x dR`
//!
//! which is the exact Z-set difference `(L_prev+dL) x (R_prev+dR) - L_prev x R_prev`.
//! The stored full product is therefore unnecessary, and a one-row delta on a
//! hot key costs `O(|R[k]|)` pairs instead of `O(|L[k]| * |R[k]|)`.

use std::collections::HashMap;

use arrow::array::UInt32Array;
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::Join;
use super::helpers::{concat_zsets, equi_join};
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;
use crate::zset::consolidate_with;

impl Join {
    /// Materializes the retained per-key sides *before* this push's deltas are
    /// folded in. Indexed like `keys`; absent keys yield empty batches.
    pub(super) fn previous_states(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<(ZSetBatch, ZSetBatch)>, EngineError> {
        let left = self
            .left
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("left arrangement uninitialized".into()))?;
        let right = self
            .right
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("right arrangement uninitialized".into()))?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push((left.to_zset_for_key(key)?, right.to_zset_for_key(key)?));
        }
        Ok(out)
    }

    /// Computes the join change for every touched key, returning the non-empty
    /// per-key deltas and the number of pairs evaluated.
    pub(super) fn delta_join(
        &self,
        keys: &[Vec<u8>],
        previous: &[(ZSetBatch, ZSetBatch)],
        left: &ZSetBatch,
        right: &ZSetBatch,
        out_schema: &SchemaRef,
    ) -> Result<(Vec<ZSetBatch>, u64), EngineError> {
        let left_conv = self.left_converter()?;
        let right_conv = self.right_converter()?;
        let left_deltas = delta_buckets(left, left_conv)?;
        let right_deltas = delta_buckets(right, right_conv)?;
        let left_schema = self
            .left_schema
            .clone()
            .ok_or_else(|| EngineError::Infrastructure("join left schema uninitialized".into()))?;
        let right_schema = self
            .right_schema
            .clone()
            .ok_or_else(|| EngineError::Infrastructure("join right schema uninitialized".into()))?;
        let empty_left = ZSetBatch::empty(left_schema);
        let empty_right = ZSetBatch::empty(right_schema);

        let mut parts = Vec::new();
        let mut work = 0u64;
        for (index, key) in keys.iter().enumerate() {
            let left_delta = left_deltas.get(key).unwrap_or(&empty_left);
            let right_delta = right_deltas.get(key).unwrap_or(&empty_right);
            let (delta, pairs) = self.delta_join_key(
                &previous[index].0,
                &previous[index].1,
                left_delta,
                right_delta,
                out_schema,
            )?;
            work += pairs;
            if !delta.is_empty() {
                parts.push(delta);
            }
        }
        Ok((parts, work))
    }

    /// Applies the delta-join formula for one key and returns the change plus
    /// the number of `(left, right)` pairs evaluated.
    fn delta_join_key(
        &self,
        prev_left: &ZSetBatch,
        prev_right: &ZSetBatch,
        left_delta: &ZSetBatch,
        right_delta: &ZSetBatch,
        out_schema: &SchemaRef,
    ) -> Result<(ZSetBatch, u64), EngineError> {
        let left_conv = self.left_converter()?;
        let right_conv = self.right_converter()?;
        let out_conv = self.out_converter()?;
        let (delta_right, pairs_dl_r) =
            equi_join(left_delta, left_conv, prev_right, right_conv, out_conv)?;
        let (left_delta_r, pairs_l_dr) =
            equi_join(prev_left, left_conv, right_delta, right_conv, out_conv)?;
        let (delta_pair, pairs_dl_dr) =
            equi_join(left_delta, left_conv, right_delta, right_conv, out_conv)?;
        let combined = concat_zsets(out_schema, &[delta_right, left_delta_r, delta_pair])?;
        let delta = consolidate_with(out_conv, &combined)?;
        Ok((delta, (pairs_dl_r + pairs_l_dr + pairs_dl_dr) as u64))
    }
}

/// Groups a delta by its key bytes, one sub-Z-set per distinct key.
fn delta_buckets(
    z: &ZSetBatch,
    converter: &KeyConverter,
) -> Result<HashMap<Vec<u8>, ZSetBatch>, EngineError> {
    let mut groups: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
    if !z.is_empty() {
        let rows = converter.convert(z.batch.columns())?;
        for index in 0..rows.num_rows() {
            groups
                .entry(rows.row(index).as_ref().to_vec())
                .or_default()
                .push(index as u32);
        }
    }
    let mut out = HashMap::with_capacity(groups.len());
    for (key, indices) in groups {
        out.insert(key, take_zset(z, &UInt32Array::from(indices))?);
    }
    Ok(out)
}

/// Selects rows by index, keeping the data columns and the diff column aligned.
fn take_zset(z: &ZSetBatch, indices: &UInt32Array) -> Result<ZSetBatch, EngineError> {
    let mut columns = Vec::with_capacity(z.batch.num_columns());
    for column in z.batch.columns() {
        columns.push(take(column.as_ref(), indices, None)?);
    }
    let batch = RecordBatch::try_new(z.batch.schema(), columns)?;
    let diff = take(z.diff.as_ref(), indices, None)?;
    Ok(ZSetBatch::new(batch, diff)?)
}
