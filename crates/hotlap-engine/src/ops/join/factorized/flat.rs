use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, UInt32Array, new_empty_array};
use arrow::compute::{concat, take};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::{KeyConverter, full_converter};
use crate::ops::join::delta::delta_buckets;
use crate::ops::join::helpers::touched_union;

struct PairTerm {
    left: ZSetBatch,
    right: ZSetBatch,
}

/// Enumerates exact join delta terms by key, never retaining Cartesian pairs.
/// The stored batches are only touched input rows and touched old-side buckets.
pub(super) struct FlatRowsCursor {
    terms: Vec<PairTerm>,
    schema: SchemaRef,
    max_rows: usize,
    term: usize,
    left_row: usize,
    right_row: usize,
}

impl FlatRowsCursor {
    #[cfg(test)]
    pub(super) fn new(
        left: &ZSetBatch,
        right: &ZSetBatch,
        max_rows: usize,
    ) -> Result<Self, EngineError> {
        Self::from_terms(
            vec![PairTerm {
                left: left.clone(),
                right: right.clone(),
            }],
            left.schema(),
            right.schema(),
            max_rows,
        )
    }

    pub(super) fn from_join_delta(
        old_left: &KeyedArrangement,
        old_right: &KeyedArrangement,
        delta_left: &ZSetBatch,
        delta_right: &ZSetBatch,
        left_keys: &[usize],
        right_keys: &[usize],
        max_rows: usize,
    ) -> Result<Self, EngineError> {
        validate_chunk_size(max_rows)?;
        if old_left.key_indices() != left_keys
            || old_right.key_indices() != right_keys
            || old_left.schema().as_ref() != delta_left.schema().as_ref()
            || old_right.schema().as_ref() != delta_right.schema().as_ref()
        {
            return Err(EngineError::Unsupported(
                "flat factor input schema or keys do not match old arrangements".into(),
            ));
        }
        let left_converter = KeyConverter::new(delta_left.schema().as_ref(), left_keys)?;
        let right_converter = KeyConverter::new(delta_right.schema().as_ref(), right_keys)?;
        if left_keys.len() != right_keys.len()
            || left_keys.iter().zip(right_keys).any(|(&left, &right)| {
                delta_left.schema().field(left).data_type()
                    != delta_right.schema().field(right).data_type()
            })
        {
            return Err(EngineError::Unsupported(
                "flat factor join keys must have matching types and arity".into(),
            ));
        }
        let left_delta = delta_buckets(delta_left, &left_converter)?;
        let right_delta = delta_buckets(delta_right, &right_converter)?;
        let keys = touched_union(delta_left, &left_converter, delta_right, &right_converter)?;
        let mut terms = Vec::new();
        for key in keys {
            let old_l = old_left.to_zset_for_key(&key)?;
            let old_r = old_right.to_zset_for_key(&key)?;
            let empty_l = ZSetBatch::empty(delta_left.schema());
            let empty_r = ZSetBatch::empty(delta_right.schema());
            let dl = left_delta.get(&key).unwrap_or(&empty_l);
            let dr = right_delta.get(&key).unwrap_or(&empty_r);
            for (left, right) in [(dl, &old_r), (&old_l, dr), (dl, dr)] {
                if !left.is_empty() && !right.is_empty() {
                    terms.push(PairTerm {
                        left: left.clone(),
                        right: right.clone(),
                    });
                }
            }
        }
        Self::from_terms(terms, delta_left.schema(), delta_right.schema(), max_rows)
    }

    fn from_terms(
        terms: Vec<PairTerm>,
        left_schema: arrow::datatypes::SchemaRef,
        right_schema: arrow::datatypes::SchemaRef,
        max_rows: usize,
    ) -> Result<Self, EngineError> {
        validate_chunk_size(max_rows)?;
        let mut fields = left_schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.extend(right_schema.fields().iter().cloned());
        Ok(Self {
            terms,
            schema: Arc::new(Schema::new(fields)),
            max_rows,
            term: 0,
            left_row: 0,
            right_row: 0,
        })
    }

    /// Returns at most `max_rows` pairs, potentially spanning term/key borders.
    /// Keep returned chunks private until `None`: later terms can fail checked
    /// multiplicity arithmetic, so partial publication would be unsafe.
    pub(super) fn next_chunk(&mut self) -> Result<Option<ZSetBatch>, EngineError> {
        let mut selected = Vec::with_capacity(self.max_rows);
        while selected.len() < self.max_rows && self.term < self.terms.len() {
            let term = &self.terms[self.term];
            if self.left_row == term.left.len() || self.right_row == term.right.len() {
                self.term += 1;
                self.left_row = 0;
                self.right_row = 0;
                continue;
            }
            let left_diff = int64_diff(&term.left)?.value(self.left_row);
            let right_diff = int64_diff(&term.right)?.value(self.right_row);
            let diff = left_diff.checked_mul(right_diff).ok_or_else(|| {
                EngineError::Infrastructure("join diff product overflowed i64".into())
            })?;
            selected.push((
                self.term,
                u32::try_from(self.left_row).map_err(|_| {
                    EngineError::Infrastructure("left batch index exceeds UInt32".into())
                })?,
                u32::try_from(self.right_row).map_err(|_| {
                    EngineError::Infrastructure("right batch index exceeds UInt32".into())
                })?,
                diff,
            ));
            self.right_row += 1;
            if self.right_row == term.right.len() {
                self.left_row += 1;
                self.right_row = 0;
            }
        }
        if selected.is_empty() {
            return Ok(None);
        }

        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for column in 0..self.schema.fields().len() {
            let mut pieces: Vec<ArrayRef> = Vec::with_capacity(selected.len());
            for (term_index, left_index, right_index, _) in &selected {
                let term = &self.terms[*term_index];
                let (source, index) = if column < term.left.batch.num_columns() {
                    (&term.left.batch, *left_index)
                } else {
                    (&term.right.batch, *right_index)
                };
                let source_column = if column < term.left.batch.num_columns() {
                    column
                } else {
                    column - term.left.batch.num_columns()
                };
                pieces.push(take(
                    source.column(source_column).as_ref(),
                    &UInt32Array::from(vec![index]),
                    None,
                )?);
            }
            let piece_refs: Vec<&dyn arrow::array::Array> =
                pieces.iter().map(|array| array.as_ref()).collect();
            columns.push(concat(&piece_refs)?);
        }
        let diffs = selected
            .into_iter()
            .map(|entry| entry.3)
            .collect::<Vec<_>>();
        Ok(Some(ZSetBatch::new(
            RecordBatch::try_new(self.schema.clone(), columns)?,
            Arc::new(Int64Array::from(diffs)),
        )?))
    }

    /// Privately consumes every bounded chunk and returns one consolidated Z-set.
    /// Errors discard the collector state; no partial output escapes this call.
    pub(super) fn collect(mut self) -> Result<ZSetBatch, EngineError> {
        let converter = full_converter(self.schema.as_ref())?;
        let mut consolidated: BTreeMap<Vec<u8>, (Vec<ArrayRef>, i64)> = BTreeMap::new();
        while let Some(chunk) = self.next_chunk()? {
            let rows = converter.convert_columns(chunk.batch.columns())?;
            let diffs = int64_diff(&chunk)?;
            for index in 0..chunk.len() {
                let key = rows.row(index).as_ref().to_vec();
                if let Some((_, total)) = consolidated.get_mut(&key) {
                    *total = total.checked_add(diffs.value(index)).ok_or_else(|| {
                        EngineError::Infrastructure("consolidated diff sum overflowed i64".into())
                    })?;
                } else {
                    let indices = UInt32Array::from(vec![u32::try_from(index).map_err(|_| {
                        EngineError::Infrastructure("flat chunk index exceeds UInt32".into())
                    })?]);
                    let values = chunk
                        .batch
                        .columns()
                        .iter()
                        .map(|column| take(column.as_ref(), &indices, None))
                        .collect::<Result<Vec<_>, _>>()?;
                    consolidated.insert(key, (values, diffs.value(index)));
                }
            }
        }

        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for column in 0..self.schema.fields().len() {
            let values = consolidated
                .values()
                .filter(|(_, diff)| *diff != 0)
                .map(|(values, _)| values[column].as_ref())
                .collect::<Vec<_>>();
            columns.push(if values.is_empty() {
                new_empty_array(self.schema.field(column).data_type())
            } else {
                concat(&values)?
            });
        }
        let diffs = consolidated
            .into_values()
            .filter_map(|(_, diff)| (diff != 0).then_some(diff))
            .collect::<Vec<_>>();
        Ok(ZSetBatch::new(
            RecordBatch::try_new(self.schema, columns)?,
            Arc::new(Int64Array::from(diffs)),
        )?)
    }
}

pub(super) fn validate_chunk_size(max_rows: usize) -> Result<(), EngineError> {
    if max_rows == 0 {
        return Err(EngineError::Unsupported(
            "flat factor chunk size must be positive".into(),
        ));
    }
    let selected_row_size = std::mem::size_of::<(usize, u32, u32, i64)>();
    if max_rows
        .checked_mul(selected_row_size)
        .is_none_or(|bytes| bytes > isize::MAX as usize)
    {
        return Err(EngineError::Unsupported(
            "flat factor chunk size exceeds addressable memory".into(),
        ));
    }
    Ok(())
}

fn int64_diff(batch: &ZSetBatch) -> Result<&Int64Array, EngineError> {
    batch
        .diff()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| EngineError::Unsupported("flat factor diff must be Int64".into()))
}
