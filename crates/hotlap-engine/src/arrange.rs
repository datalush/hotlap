mod per_key;
pub(crate) mod source_stats;
mod source_stats_registration;
#[doc(hidden)]
pub mod state;

#[cfg(test)]
mod factorized_join_count;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, new_empty_array,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, Row, RowConverter};

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::{KeyConverter, converter_for};
use crate::zset::int64_diffs;
use source_stats::{SourceColumnDemand, SourceColumnValue, SourceStats};
use state::RowState;

#[cfg(test)]
mod numeric_arrangement_tests;
#[cfg(test)]
mod source_stats_tests;

thread_local! {
    #[cfg(test)]
    static SOURCE_STATS_REBUILD_ROWS_VISITED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    #[cfg(test)]
    static SOURCE_STATS_KEY_DECODE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    #[cfg(test)]
    static SOURCE_STATS_PAYLOAD_ROWS_DECODED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    #[cfg(test)]
    static SOURCE_STATS_ORDINARY_VALUES_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Incremental keyed state: `key-row -> payload-row -> diff`.
///
/// Keys and payloads live in `arrow::row` byte format, so identity is
/// byte-based and independent of the Arrow array representation. Deltas
/// accumulate across epochs; Arrow arrays appear only when
/// [`KeyedArrangement::to_zset`] materializes the state.
pub struct KeyedArrangement {
    schema: SchemaRef,
    key_indices: Vec<usize>,
    payload_indices: Vec<usize>,
    key_converter: RowConverter,
    payload_converter: RowConverter,
    state: RowState,
    source_stats_demand: Option<Vec<SourceColumnDemand>>,
    source_stats: HashMap<Vec<u8>, SourceStats>,
}

pub(crate) struct StagedFinalWeights {
    rows: Vec<(OwnedRow, OwnedRow, i64)>,
    source_stats: HashMap<Vec<u8>, Option<SourceStats>>,
}

pub(crate) struct PreparedSourceUpdate {
    staged: StagedFinalWeights,
}

impl PreparedSourceUpdate {
    pub(crate) fn changed_keys(&self) -> impl Iterator<Item = &[u8]> {
        self.staged.source_stats.keys().map(|key| key.as_slice())
    }

    pub(crate) fn proposed_stats<'a>(
        &'a self,
        owner: &'a KeyedArrangement,
        key: &[u8],
    ) -> Option<&'a SourceStats> {
        match self.staged.source_stats.get(key) {
            Some(stats) => stats.as_ref(),
            None => owner.source_stats.get(key),
        }
    }

    pub(crate) fn commit(self, owner: &mut KeyedArrangement) {
        owner.commit_final_weights(self.staged);
    }
}

impl KeyedArrangement {
    pub(crate) fn prepare_source_update(
        &self,
        batch: &ZSetBatch,
        keys: &KeyConverter,
    ) -> Result<PreparedSourceUpdate, EngineError> {
        if keys.column_indices() != self.key_indices.as_slice() {
            return Err(EngineError::Unsupported(
                "key converter does not match arrangement keys".into(),
            ));
        }
        if batch.schema() != self.schema {
            return Err(EngineError::Unsupported(
                "arrangement input schema changed".into(),
            ));
        }
        let key_rows = keys.convert(batch.batch.columns())?;
        let payload_columns = self
            .payload_indices
            .iter()
            .map(|&i| batch.batch.column(i).clone())
            .collect::<Vec<_>>();
        let payload_rows = if payload_columns.is_empty() {
            None
        } else {
            Some(self.payload_converter.convert_columns(&payload_columns)?)
        };
        let diffs = int64_diffs(batch.diff())?;
        let mut staged = HashMap::<(Vec<u8>, OwnedRow), (i64, i64)>::new();
        for row in 0..batch.len() {
            let key = key_rows.row(row).as_ref().to_vec();
            let payload = match &payload_rows {
                Some(rows) => rows.row(row).owned(),
                None => self.unit_payload(),
            };
            let id = (key, payload);
            let old = staged.get(&id).map_or_else(
                || self.state.get(&id.0, &id.1).copied().unwrap_or(0),
                |pair| pair.1,
            );
            let new = old
                .checked_add(diffs.value(row))
                .ok_or(EngineError::NumericOverflow(
                    "arrangement multiplicity addition",
                ))?;
            staged
                .entry(id)
                .and_modify(|pair| pair.1 = new)
                .or_insert((old, new));
        }
        let rows = staged
            .into_iter()
            .map(|((key, payload), (old, new))| (key, payload.as_ref().to_vec(), old, new))
            .collect::<Vec<_>>();
        Ok(PreparedSourceUpdate {
            staged: self.stage_final_weights(&self.schema, &rows)?,
        })
    }

    #[allow(dead_code)] // The aggregate consumer is the next approved child slice.
    pub(crate) fn source_stats(&self, key: &[u8]) -> Option<&SourceStats> {
        self.source_stats.get(key)
    }

    #[cfg(test)]
    pub(crate) fn source_stats_rebuild_rows_visited_for_test() -> usize {
        SOURCE_STATS_REBUILD_ROWS_VISITED.with(std::cell::Cell::get)
    }

    #[cfg(test)]
    pub(crate) fn reset_source_stats_rebuild_rows_visited_for_test() {
        SOURCE_STATS_REBUILD_ROWS_VISITED.with(|visited| visited.set(0));
        SOURCE_STATS_KEY_DECODE_CALLS.with(|decodes| decodes.set(0));
        SOURCE_STATS_PAYLOAD_ROWS_DECODED.with(|count| count.set(0));
        SOURCE_STATS_ORDINARY_VALUES_READ.with(|count| count.set(0));
    }

    #[cfg(test)]
    pub(crate) fn source_stats_key_decode_calls_for_test() -> usize {
        SOURCE_STATS_KEY_DECODE_CALLS.with(std::cell::Cell::get)
    }

    #[cfg(test)]
    pub(crate) fn source_stats_payload_rows_decoded_for_test() -> usize {
        SOURCE_STATS_PAYLOAD_ROWS_DECODED.with(std::cell::Cell::get)
    }

    #[cfg(test)]
    pub(crate) fn source_stats_ordinary_values_read_for_test() -> usize {
        SOURCE_STATS_ORDINARY_VALUES_READ.with(std::cell::Cell::get)
    }

    #[cfg(test)]
    pub(crate) fn source_stats_entries_for_test(&self) -> usize {
        self.source_stats.len()
    }

    #[cfg(test)]
    pub(crate) fn source_stats_capacity_for_test(&self) -> usize {
        self.source_stats.capacity()
    }

    #[cfg(test)]
    pub(crate) fn bucket_kind_for_test(&self, key: &[u8]) -> Option<&'static str> {
        let key = self.key_converter.parser().parse(key).owned();
        self.state.bucket_kind(key.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn weight_for_test(&self, key_bytes: &[u8], payload_bytes: &[u8]) -> i64 {
        let key = self.key_converter.parser().parse(key_bytes).owned();
        let payload = self.payload_converter.parser().parse(payload_bytes).owned();
        self.state.weight(key.as_ref(), &payload)
    }

    #[cfg(test)]
    pub(crate) fn stage_batch_for_test(
        &self,
        batch: &ZSetBatch,
        keys: &KeyConverter,
    ) -> Result<(Vec<(OwnedRow, OwnedRow, i64)>, Vec<StagedProbeRow>), EngineError> {
        if keys.column_indices() != self.key_indices.as_slice() {
            return Err(EngineError::Unsupported(
                "key converter does not match arrangement keys".into(),
            ));
        }
        let key_rows = keys.convert(batch.batch.columns())?;
        let payload_columns = self
            .payload_indices
            .iter()
            .map(|&i| batch.batch.column(i).clone())
            .collect::<Vec<_>>();
        let payload_rows = self.payload_converter.convert_columns(&payload_columns)?;
        let diffs = int64_diffs(batch.diff())?;
        let mut staged = HashMap::<(Vec<u8>, OwnedRow), i64>::new();
        let mut decoded = Vec::with_capacity(batch.len());
        for row in 0..batch.len() {
            let key = key_rows.row(row).owned();
            let payload = payload_rows.row(row).owned();
            let id = (key.as_ref().to_vec(), payload.clone());
            let old = staged
                .get(&id)
                .copied()
                .unwrap_or_else(|| self.state.weight(&id.0, &id.1));
            let new = old
                .checked_add(diffs.value(row))
                .ok_or(EngineError::NumericOverflow(
                    "arrangement multiplicity addition",
                ))?;
            staged.insert(id, new);
            decoded.push(StagedProbeRow {
                key: key.as_ref().to_vec(),
                old_weight: old,
                new_weight: new,
            });
        }
        Ok((
            staged
                .into_iter()
                .map(|((key, payload), weight)| {
                    (
                        self.key_converter.parser().parse(&key).owned(),
                        payload,
                        weight,
                    )
                })
                .collect(),
            decoded,
        ))
    }

    #[cfg(test)]
    pub(crate) fn commit_batch_for_test(&mut self, rows: Vec<(OwnedRow, OwnedRow, i64)>) {
        self.state.commit_weights(rows);
    }

    pub(crate) fn stage_final_weights(
        &self,
        schema: &SchemaRef,
        rows: &[(Vec<u8>, Vec<u8>, i64, i64)],
    ) -> Result<StagedFinalWeights, EngineError> {
        if schema != &self.schema {
            return Err(EngineError::Unsupported(
                "cut arrangement schema changed".into(),
            ));
        }
        let mut staged = Vec::with_capacity(rows.len());
        let mut source_stats = HashMap::<Vec<u8>, Option<SourceStats>>::new();
        source_stats.reserve(rows.len());
        let source_values = match &self.source_stats_demand {
            Some(demand) => Some(source_stats_registration::values_for_changed_rows(
                self, rows, demand,
            )?),
            None => None,
        };
        for (index, (key_bytes, payload_bytes, old, new)) in rows.iter().enumerate() {
            let key = self.key_converter.parser().parse(key_bytes).owned();
            let payload = self.payload_converter.parser().parse(payload_bytes).owned();
            let actual = self.state.get(key.as_ref(), &payload).copied().unwrap_or(0);
            if actual != *old {
                return Err(EngineError::Unsupported(
                    "cut arrangement old weight changed".into(),
                ));
            }
            if self.source_stats_demand.is_some() {
                let values = &source_values.as_ref().expect("demand values staged")[index];
                let current = match source_stats.get(key_bytes.as_slice()) {
                    Some(Some(stats)) => stats.clone(),
                    Some(None) => SourceStats::default(),
                    None => self
                        .source_stats
                        .get(key_bytes.as_slice())
                        .cloned()
                        .unwrap_or_default(),
                };
                let next = current.stage_weight_change(*old, *new, values)?;
                source_stats.insert(key_bytes.clone(), (next.support > 0).then_some(next));
            }
            staged.push((key, payload, *new));
        }
        Ok(StagedFinalWeights {
            rows: staged,
            source_stats,
        })
    }

    pub(crate) fn commit_final_weights(&mut self, staged: StagedFinalWeights) {
        for (key, payload, weight) in staged.rows {
            self.state.set_weight(key, payload, weight);
        }
        for (key, stats) in staged.source_stats {
            if let Some(stats) = stats {
                self.source_stats.insert(key, stats);
            } else {
                self.source_stats.remove(&key);
            }
        }
    }

    pub(crate) fn has_storage(&self) -> bool {
        self.state.has_storage()
    }

    pub(crate) fn memory_usage(&self) -> Result<(usize, usize, usize), EngineError> {
        let (payload, state_capacity, mut structural) = self.state.memory_usage()?;
        let mut key_capacity = state_capacity;
        let index_capacity = self
            .key_indices
            .capacity()
            .checked_add(self.payload_indices.capacity())
            .and_then(|count| count.checked_mul(std::mem::size_of::<usize>()))
            .ok_or_else(|| {
                EngineError::Infrastructure("arrangement index capacity overflow".into())
            })?;
        key_capacity = key_capacity
            .checked_add(index_capacity)
            .ok_or_else(|| EngineError::Infrastructure("arrangement capacity overflow".into()))?;
        structural = structural
            .checked_add(
                self.source_stats
                    .capacity()
                    .checked_mul(std::mem::size_of::<(Vec<u8>, SourceStats)>())
                    .ok_or_else(|| {
                        EngineError::Infrastructure("source stats capacity overflow".into())
                    })?,
            )
            .ok_or_else(|| EngineError::Infrastructure("source stats size overflow".into()))?;
        for (key, stats) in &self.source_stats {
            key_capacity = key_capacity
                .checked_add(key.capacity())
                .ok_or_else(|| EngineError::Infrastructure("source stats key overflow".into()))?;
            structural = structural
                .checked_add(
                    stats
                        .columns
                        .capacity()
                        .checked_mul(std::mem::size_of::<source_stats::SourceColumnStats>())
                        .ok_or_else(|| {
                            EngineError::Infrastructure("source stats columns overflow".into())
                        })?,
                )
                .ok_or_else(|| EngineError::Infrastructure("source stats size overflow".into()))?;
        }
        if let Some(demand) = &self.source_stats_demand {
            structural = structural
                .checked_add(
                    demand
                        .capacity()
                        .checked_mul(std::mem::size_of::<SourceColumnDemand>())
                        .ok_or_else(|| {
                            EngineError::Infrastructure("source demand capacity overflow".into())
                        })?,
                )
                .ok_or_else(|| EngineError::Infrastructure("source demand size overflow".into()))?;
        }
        Ok((payload, key_capacity, structural))
    }

    /// Builds an arrangement over the key columns named by `key_indices`.
    ///
    /// Remaining schema columns are payload. Keys must be non-empty and in range.
    pub fn new(schema: SchemaRef, key_indices: &[usize]) -> Result<Self, EngineError> {
        if key_indices.is_empty()
            || key_indices
                .iter()
                .any(|&index| index >= schema.fields().len())
        {
            return Err(EngineError::Unsupported(
                "arrangement keys must be non-empty and in range".to_string(),
            ));
        }
        let payload_indices: Vec<usize> = (0..schema.fields().len())
            .filter(|index| !key_indices.contains(index))
            .collect();
        Ok(Self {
            key_converter: converter_for(&schema, key_indices)?,
            payload_converter: converter_for(&schema, &payload_indices)?,
            schema,
            key_indices: key_indices.to_vec(),
            payload_indices,
            state: RowState::default(),
            source_stats_demand: None,
            source_stats: HashMap::new(),
        })
    }

    /// Applies the signed diffs of `batch` to the state.
    pub fn apply(&mut self, batch: &ZSetBatch, keys: &KeyConverter) -> Result<(), EngineError> {
        self.adjust(batch, keys, 1)
    }

    /// Inserts the rows of `batch`, adding each row's `diff`.
    pub fn insert(&mut self, batch: &ZSetBatch, keys: &KeyConverter) -> Result<(), EngineError> {
        self.apply(batch, keys)
    }

    /// Retracts the rows of `batch`, subtracting each row's `diff`.
    pub fn retract(&mut self, batch: &ZSetBatch, keys: &KeyConverter) -> Result<(), EngineError> {
        self.adjust(batch, keys, -1)
    }

    /// Number of surviving `(key, payload)` entries.
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// Returns `true` when no entry survives.
    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    pub(crate) fn iter_key_bytes(&self) -> impl Iterator<Item = &[u8]> {
        self.state.keys()
    }

    /// Schema column indices treated as keys by this arrangement.
    pub fn key_indices(&self) -> &[usize] {
        &self.key_indices
    }

    /// Arrow schema used to decode this arrangement's key and payload rows.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Iterates `(key bytes, payload bytes, diff)` in deterministic order.
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>, i64)> + '_ {
        self.state
            .sorted()
            .into_iter()
            .map(|(key, payload, diff)| (key.to_vec(), payload.as_ref().to_vec(), diff))
    }

    /// Materializes the state as a deterministic [`ZSetBatch`].
    ///
    /// Rows are ordered by key bytes, then payload bytes.
    pub fn to_zset(&self) -> Result<ZSetBatch, EngineError> {
        let entries = self.state.sorted();
        if entries.is_empty() {
            return self.empty_zset();
        }
        let keys: Vec<&[u8]> = entries.iter().map(|entry| entry.0.as_slice()).collect();
        let payloads: Vec<&[u8]> = entries.iter().map(|entry| entry.1.as_ref()).collect();
        let key_arrays = decode(&self.key_converter, &keys)?;
        let payload_arrays = decode(&self.payload_converter, &payloads)?;
        let batch = self.assemble(&key_arrays, &payload_arrays)?;
        let diffs: Vec<i64> = entries.iter().map(|entry| entry.2).collect();
        Ok(ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs)))?)
    }

    /// Adds `sign * diff` for every row of `batch` to the state.
    fn adjust(
        &mut self,
        batch: &ZSetBatch,
        keys: &KeyConverter,
        sign: i64,
    ) -> Result<(), EngineError> {
        if keys.column_indices() != self.key_indices.as_slice() {
            return Err(EngineError::Unsupported(
                "key converter does not match arrangement keys".to_string(),
            ));
        }
        let key_rows = keys.convert(batch.batch.columns())?;
        let payload_columns: Vec<ArrayRef> = self
            .payload_indices
            .iter()
            .map(|&index| batch.batch.column(index).clone())
            .collect();
        let payload_rows = if payload_columns.is_empty() {
            None
        } else {
            Some(self.payload_converter.convert_columns(&payload_columns)?)
        };
        let diffs = int64_diffs(batch.diff())?;
        let source_demand = self.source_stats_demand.as_deref();
        let mut staged: HashMap<(Vec<u8>, OwnedRow), i64> = HashMap::new();
        let mut touched = Vec::new();
        for index in 0..batch.len() {
            let key = key_rows.row(index).owned();
            let payload = match &payload_rows {
                Some(rows) => rows.row(index).owned(),
                None => self.unit_payload(),
            };
            let incoming =
                sign.checked_mul(diffs.value(index))
                    .ok_or(EngineError::NumericOverflow(
                        "arrangement sign multiplication",
                    ))?;
            if incoming == 0 {
                continue;
            }
            let identity = (key.as_ref().to_vec(), payload.clone());
            let old = match staged.get(&identity) {
                Some(&weight) => weight,
                None => self
                    .state
                    .get(&identity.0, &identity.1)
                    .copied()
                    .unwrap_or(0),
            };
            let next = old
                .checked_add(incoming)
                .ok_or(EngineError::NumericOverflow(
                    "arrangement multiplicity addition",
                ))?;
            if !staged.contains_key(&identity) {
                let values = if let Some(demand) = source_demand {
                    batch_values(&batch.batch, index, demand)?
                } else {
                    Vec::new()
                };
                touched.push((key, payload, old, values));
            }
            staged.insert(identity, next);
        }
        let mut source_updates = HashMap::<Vec<u8>, SourceStats>::new();
        if source_demand.is_some() {
            for (key, payload, old, values) in &touched {
                let identity = (key.as_ref().to_vec(), payload.clone());
                let new = staged[&identity];
                let current = source_updates
                    .get(key.as_ref())
                    .or_else(|| self.source_stats.get(key.as_ref()))
                    .cloned()
                    .unwrap_or_default();
                source_updates.insert(
                    key.as_ref().to_vec(),
                    current.stage_weight_change(*old, new, values)?,
                );
            }
        }
        for (key, payload, _, _) in touched {
            let weight = staged[&(key.as_ref().to_vec(), payload.clone())];
            self.state.set_weight(key, payload, weight);
        }
        for (key, stats) in source_updates {
            if stats.support == 0 {
                self.source_stats.remove(&key);
            } else {
                self.source_stats.insert(key, stats);
            }
        }
        Ok(())
    }

    /// Returns the canonical payload used when the arrangement has no payload.
    pub(super) fn unit_payload(&self) -> OwnedRow {
        self.payload_converter.parser().parse(&[]).owned()
    }

    /// Rebuilds a full-schema batch from decoded key and payload columns.
    fn assemble(
        &self,
        key_arrays: &[ArrayRef],
        payload_arrays: &[ArrayRef],
    ) -> Result<RecordBatch, EngineError> {
        let columns = (0..self.schema.fields().len())
            .map(|field| {
                self.key_indices
                    .iter()
                    .position(|&index| index == field)
                    .map(|position| key_arrays[position].clone())
                    .or_else(|| {
                        self.payload_indices
                            .iter()
                            .position(|&index| index == field)
                            .map(|position| payload_arrays[position].clone())
                    })
                    .ok_or_else(|| EngineError::Infrastructure(format!("column {field} missing")))
            })
            .collect::<Result<Vec<ArrayRef>, EngineError>>()?;
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    /// Builds an empty batch with one zero-length array per schema field.
    fn empty_zset(&self) -> Result<ZSetBatch, EngineError> {
        let columns: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();
        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(ZSetBatch::new(
            batch,
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        )?)
    }
}

#[cfg(test)]
pub(crate) struct StagedProbeRow {
    pub(crate) key: Vec<u8>,
    pub(crate) old_weight: i64,
    pub(crate) new_weight: i64,
}

/// Decodes `arrow::row` bytes back into column arrays for `converter`.
fn decode(converter: &RowConverter, rows: &[&[u8]]) -> Result<Vec<ArrayRef>, EngineError> {
    let parser = converter.parser();
    let parsed: Vec<Row<'_>> = rows.iter().map(|row| parser.parse(row)).collect();
    Ok(converter.convert_rows(parsed)?)
}

fn batch_values(
    batch: &RecordBatch,
    row: usize,
    demand: &[SourceColumnDemand],
) -> Result<Vec<SourceColumnValue>, EngineError> {
    let values = demand
        .iter()
        .map(|column| source_column_value(batch.column(column.index).as_ref(), row, column.sum))
        .collect::<Result<Vec<_>, _>>()?;
    #[cfg(test)]
    SOURCE_STATS_ORDINARY_VALUES_READ.with(|count| count.set(count.get() + values.len()));
    Ok(values)
}

pub(super) fn source_column_value(
    array: &dyn Array,
    row: usize,
    sum: bool,
) -> Result<SourceColumnValue, EngineError> {
    let is_null = array.is_null(row);
    let integer = if sum && !is_null {
        if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
            Some(i128::from(values.value(row)))
        } else if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
            Some(i128::from(values.value(row)))
        } else if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
            Some(i128::from(values.value(row)))
        } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            Some(i128::from(values.value(row)))
        } else {
            return Err(EngineError::Unsupported(
                "source SUM stats require signed integer values".into(),
            ));
        }
    } else {
        None
    };
    Ok(SourceColumnValue {
        is_null,
        integer,
        sum,
    })
}
