//! Incremental per-view output: an encoded-full-row map of summed diffs.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{Row, RowConverter};

use hotlap_core::ZSetBatch;

use crate::error::EngineError;
use crate::keys::full_converter;
use crate::zset::{encode_with, int64_diffs};

/// A view's consolidated output as an incremental map.
///
/// Keys are `arrow::row`-encoded full rows and values are the summed
/// multiplicity, so `consolidate`'s row identity is preserved. Pushing a delta
/// touches only its rows (O(delta)); [`ViewOutput::snapshot`] materializes and
/// sorts the map once (O(state)). Zero-sum entries are dropped eagerly.
///
/// The schema is frozen after the first push, so the [`RowConverter`] derived
/// from it is built once and reused for every later push.
#[derive(Default)]
pub(super) struct ViewOutput {
    schema: Option<SchemaRef>,
    converter: Option<RowConverter>,
    diffs: HashMap<Vec<u8>, i64>,
}

impl ViewOutput {
    /// Number of live consolidated rows retained.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.diffs.len()
    }

    /// Merges a view's output `delta` into the map, touching only its rows.
    ///
    /// Returns `Unsupported` if the delta's schema differs from the one already
    /// retained, mirroring the old accumulate-then-consolidate contract.
    pub(super) fn update(&mut self, delta: &ZSetBatch) -> Result<(), EngineError> {
        let schema = delta.schema();
        match &self.schema {
            Some(existing) if existing != &schema => {
                return Err(EngineError::Unsupported(
                    "view output schema changed between pushes".to_string(),
                ));
            }
            Some(_) => {}
            None => {
                self.converter = Some(full_converter(schema.as_ref())?);
                self.schema = Some(schema.clone());
            }
        }
        let converter = self.converter.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("view output converter missing".to_string())
        })?;
        let rows = encode_with(converter, &delta.batch)?;
        let diffs = int64_diffs(delta.diff())?;
        for index in 0..delta.len() {
            let key = rows.row(index).as_ref().to_vec();
            let sum = diffs.value(index);
            match self.diffs.get_mut(&key) {
                Some(entry) => {
                    *entry = entry.checked_add(sum).ok_or_else(|| {
                        EngineError::Infrastructure(
                            "view output diff sum overflowed i64".to_string(),
                        )
                    })?;
                    if *entry == 0 {
                        self.diffs.remove(&key);
                    }
                }
                None if sum != 0 => {
                    self.diffs.insert(key, sum);
                }
                None => {}
            }
        }
        Ok(())
    }

    /// Materializes and sorts the consolidated state into a [`ZSetBatch`].
    ///
    /// The encoding matches [`crate::zset::consolidate`], so the result is
    /// byte-identical to consolidating the accumulated deltas.
    pub(super) fn snapshot(&self) -> Result<ZSetBatch, EngineError> {
        let Some(schema) = &self.schema else {
            return Ok(ZSetBatch::empty(Arc::new(Schema::empty())));
        };
        if self.diffs.is_empty() {
            return Ok(ZSetBatch::empty(schema.clone()));
        }
        let converter = self.converter.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("view output converter missing".to_string())
        })?;
        let parser = converter.parser();
        let mut entries: Vec<(&[u8], i64)> = self
            .diffs
            .iter()
            .map(|(key, sum)| (key.as_slice(), *sum))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let parsed: Vec<Row<'_>> = entries.iter().map(|(key, _)| parser.parse(key)).collect();
        let columns = converter.convert_rows(parsed)?;
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let diff: ArrayRef = Arc::new(Int64Array::from(
            entries.iter().map(|(_, sum)| *sum).collect::<Vec<_>>(),
        ));
        Ok(ZSetBatch::new(batch, diff)?)
    }

    /// Materializes the output for checkpointing.
    ///
    /// Returns `None` when no push has established the output schema yet, so a
    /// restored output stays uninitialized instead of adopting an empty schema.
    pub(super) fn to_snapshot(&self) -> Result<Option<ZSetBatch>, EngineError> {
        if self.schema.is_none() {
            return Ok(None);
        }
        self.snapshot().map(Some)
    }
}
