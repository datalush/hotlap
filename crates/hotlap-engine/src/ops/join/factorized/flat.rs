use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{concat, concat_batches, take};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::{KeyConverter, full_converter};
use crate::ops::join::delta::delta_buckets;
use crate::ops::join::helpers::touched_union;
use crate::ops::join::{Join, PreparedJoinSources};

/// Rows and canonical source replacements prepared together, but not yet
/// published. Callers may first apply the rows to a downstream reducer.
pub(crate) struct PreparedFlatJoinUpdate {
    sources: PreparedJoinSources,
    output: ZSetBatch,
}

impl PreparedFlatJoinUpdate {
    #[cfg(test)]
    pub(crate) fn output(&self) -> &ZSetBatch {
        &self.output
    }

    /// Separates the PRE-state result from its staged owner update so
    /// independent readers can all consume the same old arrangements before
    /// the owner publishes the update.
    pub(crate) fn into_parts(self) -> (ZSetBatch, PreparedJoinSources) {
        (self.output, self.sources)
    }

    #[cfg(test)]
    pub(crate) fn commit(self, join: &mut Join) {
        join.commit_source_updates(self.sources);
    }
}

/// Stages canonical row/stat updates before materializing the delta from the
/// old arrangements. No source state is published until `commit`.
pub(crate) fn prepare_flat_join_update(
    join: &mut Join,
    left_delta: &ZSetBatch,
    right_delta: &ZSetBatch,
    max_rows: usize,
) -> Result<PreparedFlatJoinUpdate, EngineError> {
    let schema = join.cut_schema(left_delta, right_delta)?;
    let sources = join.prepare_source_updates(left_delta, right_delta)?;
    let output = materialize_flat_delta(join, &sources, left_delta, right_delta, schema, max_rows)?;
    Ok(PreparedFlatJoinUpdate { sources, output })
}

/// Materializes only from authoritative old arrangements and the raw Arrow
/// deltas. `sources` proves both common source updates already validated and
/// staged; it is deliberately not recomputed here.
pub(crate) fn materialize_flat_delta(
    join: &Join,
    _sources: &PreparedJoinSources,
    left_delta: &ZSetBatch,
    right_delta: &ZSetBatch,
    output_schema: arrow::datatypes::SchemaRef,
    max_rows: usize,
) -> Result<ZSetBatch, EngineError> {
    let left = join.left_for_cut().ok_or_else(|| {
        EngineError::Infrastructure("flat join left arrangement uninitialized".into())
    })?;
    let right = join.right_for_cut().ok_or_else(|| {
        EngineError::Infrastructure("flat join right arrangement uninitialized".into())
    })?;
    materialize_shared_flat_delta(
        left,
        right,
        left_delta,
        right_delta,
        left.key_indices(),
        right.key_indices(),
        output_schema,
        max_rows,
    )
}

/// Materializes the canonical flat delta from externally owned PRE-state
/// arrangements. The caller must stage source updates before calling, then
/// delay their commit until every consumer has read these arrangements.
#[allow(clippy::too_many_arguments)]
pub(crate) fn materialize_shared_flat_delta(
    old_left: &KeyedArrangement,
    old_right: &KeyedArrangement,
    left_delta: &ZSetBatch,
    right_delta: &ZSetBatch,
    left_keys: &[usize],
    right_keys: &[usize],
    output_schema: arrow::datatypes::SchemaRef,
    max_rows: usize,
) -> Result<ZSetBatch, EngineError> {
    let output = FlatRowsCursor::from_join_delta(
        old_left,
        old_right,
        left_delta,
        right_delta,
        left_keys,
        right_keys,
        max_rows,
    )?
    .collect()?;
    if output.schema() != output_schema {
        return Err(EngineError::Infrastructure(
            "flat join materializer produced an unexpected schema".into(),
        ));
    }
    Ok(output)
}

struct PairTerm {
    key_index: usize,
    left: ZSetBatch,
    right: ZSetBatch,
}

/// Enumerates exact join delta terms by key, never retaining Cartesian pairs.
/// The stored batches are only touched input rows and touched old-side buckets.
/// Chunking bounds pair workspace, not touched-key bucket storage.
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
                key_index: 0,
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
        for (key_index, key) in keys.into_iter().enumerate() {
            let old_l = old_left.to_zset_for_key(&key)?;
            let old_r = old_right.to_zset_for_key(&key)?;
            let empty_l = ZSetBatch::empty(delta_left.schema());
            let empty_r = ZSetBatch::empty(delta_right.schema());
            let dl = left_delta.get(&key).unwrap_or(&empty_l);
            let dr = right_delta.get(&key).unwrap_or(&empty_r);
            for (left, right) in [(dl, &old_r), (&old_l, dr), (dl, dr)] {
                if !left.is_empty() && !right.is_empty() {
                    terms.push(PairTerm {
                        key_index,
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

    /// Returns at most `max_rows` pairs from one formula term. The collector
    /// finishes that term's Dense-equivalent consolidation before requesting
    /// products from the next term or touched key.
    pub(super) fn next_chunk(&mut self) -> Result<Option<ZSetBatch>, EngineError> {
        let mut selected = Vec::with_capacity(self.max_rows);
        let mut selected_term = None;
        while selected.len() < self.max_rows && self.term < self.terms.len() {
            if selected_term.is_some_and(|selected| selected != self.term) {
                break;
            }
            let term = &self.terms[self.term];
            if self.left_row == term.left.len() || self.right_row == term.right.len() {
                self.term += 1;
                self.left_row = 0;
                self.right_row = 0;
                continue;
            }
            let left_diff = int64_diff(&term.left)?.value(self.left_row);
            let right_diff = int64_diff(&term.right)?.value(self.right_row);
            selected_term = Some(self.term);
            let diff = left_diff
                .checked_mul(right_diff)
                .ok_or(EngineError::NumericOverflow("join diff product"))?;
            #[cfg(test)]
            super::FLAT_PAIR_PRODUCTS.with(|count| count.set(count.get() + 1));
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
            self.left_row += 1;
            if self.left_row == term.left.len() {
                self.right_row += 1;
                self.left_row = 0;
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

    /// Privately consumes bounded pair chunks, retaining the complete current
    /// term until all its checked products exist. This matches Dense's product
    /// before consolidation ordering; chunks bound pair workspace only.
    pub(super) fn collect(mut self) -> Result<ZSetBatch, EngineError> {
        let mut term_index = None;
        let mut term_chunks = Vec::new();
        let mut key_index = None;
        let mut key_terms = Vec::new();
        let mut key_outputs = Vec::new();
        loop {
            self.advance_finished_terms();
            if self.term == self.terms.len() {
                break;
            }
            let next_term = self.term;
            let next_key = self.terms[next_term].key_index;
            if term_index.is_some_and(|old| old != next_term) {
                key_terms.push(consolidate_term_chunks(&self.schema, &term_chunks)?);
                term_chunks.clear();
            }
            if key_index.is_some_and(|old| old != next_key) {
                key_outputs.push(consolidate_join_terms(&self.schema, &key_terms)?);
                key_terms.clear();
            }
            term_index = Some(next_term);
            key_index = Some(next_key);
            if let Some(chunk) = self.next_chunk()? {
                term_chunks.push(chunk);
            }
        }

        if !term_chunks.is_empty() {
            key_terms.push(consolidate_term_chunks(&self.schema, &term_chunks)?);
        }
        if !key_terms.is_empty() {
            key_outputs.push(consolidate_join_terms(&self.schema, &key_terms)?);
        }
        consolidate_join_terms(&self.schema, &key_outputs)
    }

    fn advance_finished_terms(&mut self) {
        while self.term < self.terms.len()
            && (self.left_row == self.terms[self.term].left.len()
                || self.right_row == self.terms[self.term].right.len())
        {
            self.term += 1;
            self.left_row = 0;
            self.right_row = 0;
        }
    }
}

fn consolidate_term_chunks(
    schema: &SchemaRef,
    chunks: &[ZSetBatch],
) -> Result<ZSetBatch, EngineError> {
    let batches = chunks.iter().map(|chunk| &chunk.batch).collect::<Vec<_>>();
    let batch = concat_batches(schema, batches)?;
    let diffs = chunks
        .iter()
        .map(|chunk| chunk.diff.as_ref())
        .collect::<Vec<_>>();
    let diff = concat(&diffs)?;
    crate::zset::consolidate(&ZSetBatch::new(batch, diff)?)
}

fn consolidate_join_terms(
    schema: &SchemaRef,
    terms: &[ZSetBatch],
) -> Result<ZSetBatch, EngineError> {
    if terms.is_empty() {
        return Ok(ZSetBatch::empty(schema.clone()));
    }
    let batches = terms.iter().map(|term| &term.batch).collect::<Vec<_>>();
    let batch = concat_batches(schema, batches)?;
    let diffs = terms
        .iter()
        .map(|term| term.diff.as_ref())
        .collect::<Vec<_>>();
    let diff = concat(&diffs)?;
    let joined = ZSetBatch::new(batch, diff)?;
    let converter = full_converter(schema.as_ref())?;
    crate::zset::consolidate_with(&converter, &joined)
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
