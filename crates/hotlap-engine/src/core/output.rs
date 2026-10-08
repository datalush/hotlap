//! Incremental per-view output: an encoded-full-row map of summed diffs.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{Row, RowConverter, SortField};

use hotlap_core::ZSetBatch;

use crate::error::EngineError;
use crate::zset::{full_rows, int64_diffs};

/// A view's consolidated output as an incremental map.
///
/// Keys are `arrow::row`-encoded full rows and values are the summed
/// multiplicity, so `consolidate`'s row identity is preserved. Pushing a delta
/// touches only its rows (O(delta)); [`ViewOutput::snapshot`] materializes and
/// sorts the map once (O(state)). Zero-sum entries are dropped eagerly.
#[derive(Default)]
pub(super) struct ViewOutput {
    schema: Option<SchemaRef>,
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
            None => self.schema = Some(schema.clone()),
        }
        let rows = full_rows(&delta.batch)?;
        let diffs = int64_diffs(delta.diff())?;
        for index in 0..delta.len() {
            let key = rows.row(index).as_ref().to_vec();
            let sum = diffs.value(index);
            match self.diffs.get_mut(&key) {
                Some(entry) => {
                    *entry += sum;
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
        let converter = RowConverter::new(sort_fields(schema))?;
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
}

/// Builds the full-row sort fields used by both encoding and decoding.
fn sort_fields(schema: &SchemaRef) -> Vec<SortField> {
    schema
        .fields()
        .iter()
        .map(|field| SortField::new(field.data_type().clone()))
        .collect()
}
