//! Cached converters, schema derivation and side accumulation for [`Join`].

use arrow::datatypes::SchemaRef;
use arrow::row::RowConverter;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;

use super::helpers::{equi_join, joined_schema, subtract};
use super::Join;

impl Join {
    /// Cached converter for the left side's join-key columns.
    pub(super) fn left_converter(&self) -> Result<&KeyConverter, EngineError> {
        self.left_conv
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("left key converter missing".into()))
    }

    /// Cached converter for the right side's join-key columns.
    pub(super) fn right_converter(&self) -> Result<&KeyConverter, EngineError> {
        self.right_conv
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("right key converter missing".into()))
    }

    /// Cached full-row converter for the joined output schema.
    pub(super) fn out_converter(&self) -> Result<&RowConverter, EngineError> {
        self.out_conv
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("join output converter missing".into()))
    }

    /// Re-evaluates one join key and returns the delta against the stored join.
    pub(super) fn recompute_key(
        &mut self,
        key: &[u8],
        schema: &SchemaRef,
    ) -> Result<ZSetBatch, EngineError> {
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
        let right_conv = self.right_converter()?;
        let out_conv = self.out_converter()?;
        let (current, pairs) = equi_join(
            &left_slice,
            self.left_converter()?,
            &right_slice,
            right_conv,
            out_conv,
        )?;
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
        let delta = subtract(&current, &previous, self.out_converter()?)?;
        if !current.is_empty() {
            self.joined.insert(key.to_vec(), current);
        }
        Ok(delta)
    }

    /// Joined output schema (`left || right`), learned from the first apply.
    pub(super) fn output_schema(&self) -> Result<SchemaRef, EngineError> {
        match (&self.left_schema, &self.right_schema) {
            (Some(left), Some(right)) => Ok(joined_schema(left, right)),
            _ => Err(EngineError::Infrastructure(
                "join schemas uninitialized".to_string(),
            )),
        }
    }

    /// Adds each side's delta to its arrangement, creating the arrangements from
    /// the incoming schemas on the first call.
    pub(super) fn accumulate(
        &mut self,
        left: &ZSetBatch,
        right: &ZSetBatch,
    ) -> Result<(), EngineError> {
        if self.left.is_none() {
            self.left = Some(KeyedArrangement::new(left.schema(), &self.left_keys)?);
            self.left_schema = Some(left.schema());
            self.left_conv = Some(KeyConverter::new(left.schema().as_ref(), &self.left_keys)?);
        }
        if self.right.is_none() {
            self.right = Some(KeyedArrangement::new(right.schema(), &self.right_keys)?);
            self.right_schema = Some(right.schema());
            self.right_conv = Some(KeyConverter::new(right.schema().as_ref(), &self.right_keys)?);
        }
        let left_keys = self
            .left_conv
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("left key converter missing".into()))?;
        let left_arrangement = self
            .left
            .as_mut()
            .ok_or_else(|| EngineError::Infrastructure("left arrangement uninitialized".into()))?;
        left_arrangement.apply(left, left_keys)?;
        let right_keys = self
            .right_conv
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("right key converter missing".into()))?;
        let right_arrangement = self
            .right
            .as_mut()
            .ok_or_else(|| EngineError::Infrastructure("right arrangement uninitialized".into()))?;
        right_arrangement.apply(right, right_keys)?;
        Ok(())
    }
}
