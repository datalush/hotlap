//! Cached converters, schema derivation and side-state updates for [`Join`].

use arrow::datatypes::SchemaRef;
use arrow::row::RowConverter;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;

use super::Join;
use super::helpers::joined_schema;

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

    /// Joined output schema (`left || right`), built once from the first apply.
    pub(super) fn output_schema(&mut self) -> Result<SchemaRef, EngineError> {
        if self.out_schema.is_none() {
            let left = self.left_schema.as_ref().ok_or_else(|| {
                EngineError::Infrastructure("join left schema uninitialized".to_string())
            })?;
            let right = self.right_schema.as_ref().ok_or_else(|| {
                EngineError::Infrastructure("join right schema uninitialized".to_string())
            })?;
            self.out_schema = Some(joined_schema(left, right));
        }
        self.out_schema.clone().ok_or_else(|| {
            EngineError::Infrastructure("join output schema uninitialized".to_string())
        })
    }

    /// Prepares the arrangements and converters from the incoming schemas.
    ///
    /// Rejects a schema change on either side (the converters are cached and
    /// only valid for the schema they were built from) and rejects key columns
    /// whose `DataType`s differ across sides (their `arrow::row` encodings would
    /// not be byte-comparable).
    pub(super) fn ensure_ready(
        &mut self,
        left: &ZSetBatch,
        right: &ZSetBatch,
    ) -> Result<(), EngineError> {
        self.ensure_left(left)?;
        self.ensure_right(right)?;
        self.ensure_key_types_match()
    }

    /// Folds each side's delta into its retained arrangement.
    ///
    /// Runs after the previous per-key states have been captured and never
    /// learns new schemas; [`Join::ensure_ready`] must run first.
    pub(super) fn apply_deltas(
        &mut self,
        left: &ZSetBatch,
        right: &ZSetBatch,
    ) -> Result<(), EngineError> {
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

    /// Validates the left schema against the cache and learns it on first use.
    fn ensure_left(&mut self, z: &ZSetBatch) -> Result<(), EngineError> {
        match &self.left_schema {
            Some(existing) if existing != &z.schema() => Err(EngineError::Unsupported(
                "join left input schema changed".to_string(),
            )),
            Some(_) => Ok(()),
            None => {
                self.left = Some(KeyedArrangement::new(z.schema(), &self.left_keys)?);
                self.left_schema = Some(z.schema());
                self.left_conv = Some(KeyConverter::new(z.schema().as_ref(), &self.left_keys)?);
                Ok(())
            }
        }
    }

    /// Validates the right schema against the cache and learns it on first use.
    fn ensure_right(&mut self, z: &ZSetBatch) -> Result<(), EngineError> {
        match &self.right_schema {
            Some(existing) if existing != &z.schema() => Err(EngineError::Unsupported(
                "join right input schema changed".to_string(),
            )),
            Some(_) => Ok(()),
            None => {
                self.right = Some(KeyedArrangement::new(z.schema(), &self.right_keys)?);
                self.right_schema = Some(z.schema());
                self.right_conv = Some(KeyConverter::new(z.schema().as_ref(), &self.right_keys)?);
                Ok(())
            }
        }
    }

    /// Ensures each paired left/right key column has the same `DataType`.
    fn ensure_key_types_match(&self) -> Result<(), EngineError> {
        let left = self.left_schema.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("join left schema uninitialized".to_string())
        })?;
        let right = self.right_schema.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("join right schema uninitialized".to_string())
        })?;
        for (&left_index, &right_index) in self.left_keys.iter().zip(&self.right_keys) {
            let left_type = left.field(left_index).data_type();
            let right_type = right.field(right_index).data_type();
            if left_type != right_type {
                return Err(EngineError::Unsupported(format!(
                    "join key types differ: left column {left_index} is {left_type:?}, \
                     right column {right_index} is {right_type:?}"
                )));
            }
        }
        Ok(())
    }
}
