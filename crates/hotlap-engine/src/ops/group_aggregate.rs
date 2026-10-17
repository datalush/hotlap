//! Stateful incremental grouped aggregates over a keyed delta.
//!
//! The reducer keeps one accumulator set per key and updates only the keys the
//! incoming delta touches, so a push costs O(delta), not O(keyspace). State is
//! retraction-aware: a negative Z-set weight folds back out of the accumulators
//! (and out of the group's row multiplicity). Each `apply` emits an upsert
//! changelog: the previous aggregate row with diff `-1` and the new one with
//! diff `+1`, for every key whose values changed.

mod accumulate;
mod changes;
pub(crate) mod columns;
mod fold;
pub(crate) mod maintained;
pub(crate) mod materialize;
mod persisted;
pub(crate) mod read;
mod snapshot;
mod validate;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};

use crate::core::ipc::typed_keys::{TypedKeyRow, schema_uses_native_capture};
use hotlap_core::plan::AggSpec;
use hotlap_core::snapshot::{AggValue, GroupEntry};

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::converter_for;
use crate::zset::int64_diffs;

use accumulate::empty_entry;
use fold::fold_row;
use materialize::Change;

/// Incremental grouped aggregate over the key columns named by `key`.
pub struct GroupAggregate {
    key: Vec<usize>,
    aggs: Vec<AggSpec>,
    schema: Option<SchemaRef>,
    converter: Option<RowConverter>,
    groups: HashMap<Vec<u8>, GroupEntry>,
    native_keys: Option<HashMap<Vec<u8>, TypedKeyRow>>,
    /// Query-derived source memo; logical v1 snapshots continue to store only `groups`.
    maintained_cut: Option<maintained::MaintainedGroupCut>,
    #[cfg(test)]
    work: u64,
}

impl GroupAggregate {
    pub(crate) fn maintained_cut(&self) -> Option<&maintained::MaintainedGroupCut> {
        self.maintained_cut.as_ref()
    }

    pub(crate) fn empty_cut_entry(&self) -> Result<GroupEntry, EngineError> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("group schema missing for certified cut".into())
        })?;
        accumulate::empty_entry(&self.aggs, schema)
    }

    pub(crate) fn initialize_maintained_cut(
        &mut self,
        mapping: &hotlap_core::plan_normalize::ColumnMapping,
        left: &crate::arrange::KeyedArrangement,
        right: &crate::arrange::KeyedArrangement,
    ) -> Result<bool, EngineError> {
        if self.key != mapping.group_key
            || self.aggs.len() != mapping.aggregate_inputs.len()
            || self
                .aggs
                .iter()
                .zip(&mapping.aggregate_inputs)
                .any(|(agg, input)| agg.input != *input)
        {
            return Ok(false);
        }
        if let Some(existing) = &self.maintained_cut {
            return Ok(existing.mapping == *mapping);
        }
        let Some(cut) = maintained::MaintainedGroupCut::build(mapping, &self.aggs, left, right)?
        else {
            return Ok(false);
        };
        self.maintained_cut = Some(cut);
        Ok(true)
    }

    pub(crate) fn initialize_maintained_cut_projected(
        &mut self,
        mapping: &hotlap_core::plan_normalize::ColumnMapping,
        left: &crate::arrange::KeyedArrangement,
        right: &crate::arrange::KeyedArrangement,
        projection: &[usize],
    ) -> Result<bool, EngineError> {
        let Some(schema) = &self.schema else {
            return Ok(false);
        };
        if self.key.len() != mapping.group_key.len()
            || self.aggs.len() != mapping.aggregate_inputs.len()
            || mapping.aggregate_input_types.len() != self.aggs.len()
        {
            return Ok(false);
        }
        for (output_key, &join_key_position) in self.key.iter().zip(&mapping.group_key) {
            let Some(expected_type) = mapping.join_key_types.get(join_key_position) else {
                return Ok(false);
            };
            if schema.fields().get(*output_key).map(|f| f.data_type()) != Some(expected_type) {
                return Ok(false);
            }
            let Some(&projected_column) = projection.get(*output_key) else {
                return Ok(false);
            };
            let Some(&left_column) = mapping.left_join_key.get(join_key_position) else {
                return Ok(false);
            };
            let Some(&right_column) = mapping.right_join_key.get(join_key_position) else {
                return Ok(false);
            };
            let right_column = left
                .schema()
                .fields()
                .len()
                .checked_add(right_column)
                .ok_or_else(|| {
                    EngineError::Infrastructure("projected join column overflow".into())
                })?;
            if projected_column != left_column && projected_column != right_column {
                return Ok(false);
            }
        }
        for ((agg, source_input), source_type) in self
            .aggs
            .iter()
            .zip(&mapping.aggregate_inputs)
            .zip(&mapping.aggregate_input_types)
        {
            match (agg.input, source_input, source_type) {
                (None, None, None) => {}
                (Some(output_input), Some(source_input), Some(source_type)) => {
                    if projection.get(output_input) != Some(source_input)
                        || schema.fields().get(output_input).map(|f| f.data_type())
                            != Some(source_type)
                    {
                        return Ok(false);
                    }
                }
                _ => return Ok(false),
            }
        }
        if let Some(existing) = &self.maintained_cut {
            return Ok(existing.mapping == *mapping);
        }
        let Some(cut) = maintained::MaintainedGroupCut::build(mapping, &self.aggs, left, right)?
        else {
            return Ok(false);
        };
        self.maintained_cut = Some(cut);
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn maintained_memo(
        &self,
    ) -> Option<&crate::ops::join::factorized::group_memo::GroupMemo> {
        self.maintained_cut.as_ref().map(|cut| &cut.memo)
    }

    pub(crate) fn validate_maintained_history(&self) -> Result<(), EngineError> {
        if let Some(cut) = &self.maintained_cut {
            cut.validate_authoritative_history(&self.aggs, &self.groups)?;
        }
        Ok(())
    }

    pub(crate) fn stage_maintained_memo(
        &self,
        updates: Vec<(
            Vec<u8>,
            Option<crate::ops::join::factorized::group_memo::Contribution>,
        )>,
    ) -> Result<Option<crate::ops::join::factorized::group_memo::GroupMemoStage>, EngineError> {
        self.maintained_cut
            .as_ref()
            .map(|cut| cut.stage(updates))
            .transpose()
    }

    pub(crate) fn commit_maintained_memo(
        &mut self,
        stage: crate::ops::join::factorized::group_memo::GroupMemoStage,
    ) {
        if let Some(cut) = &mut self.maintained_cut {
            cut.commit(stage);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn retained_groups(&self) -> &HashMap<Vec<u8>, GroupEntry> {
        &self.groups
    }

    pub(crate) fn aggregates(&self) -> &[AggSpec] {
        &self.aggs
    }

    pub(crate) fn group_key(&self) -> &[usize] {
        &self.key
    }

    pub(crate) fn schema_for_cut(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    pub(crate) fn initialize_cut(&mut self, schema: SchemaRef) -> Result<(), EngineError> {
        let empty = ZSetBatch::empty(schema);
        self.ensure_schema(&empty)
    }

    #[allow(dead_code)]
    pub(crate) fn commit_factorized_consumed(
        &mut self,
        changes: Vec<(Vec<u8>, Option<GroupEntry>)>,
    ) {
        for (key, entry) in changes {
            if let Some(entry) = entry {
                self.groups.insert(key, entry);
            } else {
                self.groups.remove(&key);
            }
        }
    }

    pub(crate) fn has_minmax_state(&self) -> bool {
        self.groups.values().any(|entry| {
            entry.values.iter().any(|value| match value {
                AggValue::Min(values) | AggValue::Max(values) => values
                    .memory_usage_summary()
                    .is_some_and(|(entries, _)| entries > 0),
                _ => false,
            })
        })
    }

    pub(crate) fn has_group_map_storage(&self) -> bool {
        self.groups.capacity() > 0
    }

    pub(crate) fn memory_usage(
        &self,
    ) -> Result<super::super::core::memory::GroupMemory, crate::error::EngineError> {
        let mut payload = 0usize;
        let mut capacity = 0usize;
        let mut accumulator_capacity = 0usize;
        let mut minmax_entries = 0usize;
        let mut minmax_estimate = 0usize;
        for key in self.groups.keys() {
            payload = payload.checked_add(key.len()).ok_or_else(|| {
                crate::error::EngineError::Infrastructure("memory report overflow".into())
            })?;
            capacity = capacity.checked_add(key.capacity()).ok_or_else(|| {
                crate::error::EngineError::Infrastructure("memory report overflow".into())
            })?;
        }
        let mut native_estimate = 0usize;
        if let Some(native_keys) = &self.native_keys {
            for (key, row) in native_keys {
                payload =
                    crate::core::memory::checked_add(payload, key.len(), "native key payload")?;
                capacity = crate::core::memory::checked_add(
                    capacity,
                    key.capacity(),
                    "native key capacity",
                )?;
                for column in row.batch().columns() {
                    payload = crate::core::memory::checked_add(
                        payload,
                        column.get_array_memory_size(),
                        "native array payload",
                    )?;
                }
            }
            native_estimate = native_keys
                .capacity()
                .checked_mul(std::mem::size_of::<(Vec<u8>, TypedKeyRow)>())
                .ok_or_else(|| {
                    crate::error::EngineError::Infrastructure("native key estimate overflow".into())
                })?;
        }
        for entry in self.groups.values() {
            accumulator_capacity = accumulator_capacity
                .checked_add(crate::core::memory::checked_capacity_bytes(
                    entry.values.capacity(),
                    std::mem::size_of::<hotlap_core::snapshot::AggValue>(),
                )?)
                .ok_or_else(|| {
                    crate::error::EngineError::Infrastructure(
                        "memory report accumulator capacity overflow".into(),
                    )
                })?;
            for value in &entry.values {
                let multiset = match value {
                    AggValue::Min(values) | AggValue::Max(values) => Some(values),
                    _ => None,
                };
                if let Some(multiset) = multiset {
                    let (entries, estimate) = multiset.memory_usage_summary().ok_or_else(|| {
                        crate::error::EngineError::Infrastructure(
                            "memory report MIN/MAX estimate overflow".into(),
                        )
                    })?;
                    minmax_entries = crate::core::memory::checked_add(
                        minmax_entries,
                        entries,
                        "MIN/MAX entry count",
                    )?;
                    minmax_estimate = crate::core::memory::checked_add(
                        minmax_estimate,
                        estimate,
                        "MIN/MAX estimate",
                    )?;
                }
            }
        }
        let config_capacity = crate::core::memory::checked_add(
            crate::core::memory::checked_capacity_bytes(
                self.key.capacity(),
                std::mem::size_of::<usize>(),
            )?,
            crate::core::memory::checked_capacity_bytes(
                self.aggs.capacity(),
                std::mem::size_of::<AggSpec>(),
            )?,
            "aggregate configuration capacity",
        )?;
        if let Some(cut) = &self.maintained_cut {
            let (_, memo_capacity) = cut.memo.memory_usage()?;
            capacity = crate::core::memory::checked_add(
                capacity,
                memo_capacity,
                "maintained group memo capacity",
            )?;
            let config = cut.known_capacity_bytes()?;
            capacity = crate::core::memory::checked_add(
                capacity,
                config,
                "maintained group config capacity",
            )?;
        }
        Ok(super::super::core::memory::GroupMemory {
            payload,
            capacity: crate::core::memory::checked_add(
                crate::core::memory::checked_add(capacity, accumulator_capacity, "capacity")?,
                config_capacity,
                "capacity",
            )?,
            estimate: self
                .groups
                .capacity()
                .checked_mul(std::mem::size_of::<(
                    Vec<u8>,
                    hotlap_core::snapshot::GroupEntry,
                )>())
                .ok_or_else(|| {
                    crate::error::EngineError::Infrastructure("memory report overflow".into())
                })?
                .checked_add(native_estimate)
                .ok_or_else(|| {
                    crate::error::EngineError::Infrastructure("memory report overflow".into())
                })?,
            minmax_entries,
            minmax_estimate,
        })
    }

    /// Creates a reducer over `key` computing `aggs` in order.
    pub fn new(key: &[usize], aggs: Vec<AggSpec>) -> Self {
        Self {
            key: key.to_vec(),
            aggs,
            schema: None,
            converter: None,
            groups: HashMap::new(),
            native_keys: None,
            maintained_cut: None,
            #[cfg(test)]
            work: 0,
        }
    }

    /// Applies one input delta and returns only the changed aggregate rows.
    pub fn apply(&mut self, z: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
        self.ensure_schema(z)?;
        let key_rows = self.encode_keys(z)?;
        // Reject an invalid delta before any accumulator is mutated, so a
        // failed `apply` leaves the prior state exactly as it was.
        validate::validate_delta(&self.groups, &self.aggs, &z.schema(), z, &key_rows)?;
        let (keys, mut previous) = self.record_keys(&key_rows, z.len())?;
        let mut native_additions = HashMap::new();
        if self.native_keys.is_some() {
            let key_columns: Vec<ArrayRef> = self
                .key
                .iter()
                .map(|&i| z.batch.column(i).clone())
                .collect();
            let key_schema = Arc::new(Schema::new(
                self.key
                    .iter()
                    .map(|&i| z.schema().field(i).clone())
                    .collect::<Vec<_>>(),
            ));
            let key_batch = RecordBatch::try_new(key_schema, key_columns)?;
            let diffs = int64_diffs(z.diff())?;
            for row in 0..z.len() {
                let canonical = key_rows.row(row).as_ref().to_vec();
                if diffs.value(row) != 0
                    && !self.groups.contains_key(&canonical)
                    && !native_additions.contains_key(&canonical)
                {
                    native_additions.insert(canonical, TypedKeyRow::capture(&key_batch, row)?);
                }
            }
            return self.apply_native_staged(z, &key_rows, keys, previous, native_additions);
        }
        #[cfg(test)]
        {
            self.work = keys.len() as u64;
        }
        self.fold(z, &key_rows)?;
        let mut changes: Vec<Change> = Vec::new();
        for (slot, key) in keys.iter().enumerate() {
            if let Some(change) = self.change(key, previous[slot].take())? {
                changes.push(change);
            }
        }
        if let Some(cache) = &mut self.native_keys {
            for (key, representative) in native_additions {
                if self.groups.contains_key(&key) {
                    cache.insert(key, representative);
                }
            }
        }
        let output = self.materialize(&changes);
        if let Some(cache) = &mut self.native_keys {
            cache.retain(|key, _| self.groups.contains_key(key));
        }
        output
    }

    /// Number of keys touched by the last `apply` (test instrumentation only).
    #[cfg(test)]
    pub fn work(&self) -> u64 {
        self.work
    }

    /// Validates keys and aggs against the input schema, learning it on first use.
    fn ensure_schema(&mut self, z: &ZSetBatch) -> Result<(), EngineError> {
        let schema = z.schema();
        let width = schema.fields().len();
        if self.key.iter().any(|&index| index >= width) {
            return Err(EngineError::Unsupported(
                "group key out of range".to_string(),
            ));
        }
        accumulate::validate_aggs(&self.aggs, schema.as_ref())?;
        match &self.schema {
            Some(existing) if existing != &schema => Err(EngineError::Unsupported(
                "group input schema changed".to_string(),
            )),
            Some(_) => Ok(()),
            None => {
                self.converter = Some(if self.key.is_empty() {
                    RowConverter::new(vec![SortField::new(arrow::datatypes::DataType::UInt8)])?
                } else {
                    converter_for(&schema, &self.key)?
                });
                let key_schema = Arc::new(Schema::new(
                    self.key
                        .iter()
                        .map(|&i| schema.field(i).clone())
                        .collect::<Vec<_>>(),
                ));
                if schema_uses_native_capture(&key_schema) {
                    self.native_keys = Some(HashMap::new());
                }
                self.schema = Some(schema);
                Ok(())
            }
        }
    }

    /// Encodes the delta's key columns into comparable row bytes.
    fn encode_keys(&self, z: &ZSetBatch) -> Result<Rows, EngineError> {
        let converter = self
            .converter
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("group converter missing".to_string()))?;
        let columns: Vec<ArrayRef> = self
            .key
            .iter()
            .map(|&index| z.batch.column(index).clone())
            .collect();
        if self.key.is_empty() {
            return Ok(
                converter
                    .convert_columns(&[
                        Arc::new(arrow::array::UInt8Array::from(vec![0; z.len()])) as ArrayRef,
                    ])?,
            );
        }
        Ok(converter.convert_columns(&columns)?)
    }

    /// Folds every row of the delta into its key's accumulators.
    fn fold(&mut self, z: &ZSetBatch, key_rows: &Rows) -> Result<(), EngineError> {
        let diffs = int64_diffs(z.diff())?;
        let schema = self
            .schema
            .clone()
            .ok_or_else(|| EngineError::Infrastructure("group schema missing".to_string()))?;
        let template = empty_entry(&self.aggs, &schema)?;
        for row in 0..z.len() {
            let diff = diffs.value(row);
            if diff == 0 {
                continue;
            }
            let bytes = key_rows.row(row).as_ref().to_vec();
            let entry = self.groups.entry(bytes).or_insert_with(|| template.clone());
            fold_row(entry, &self.aggs, &z.batch, row, diff)?;
        }
        Ok(())
    }

    /// Stages only touched native-key groups and their exact emitted output before publishing.
    /// Scalar keys retain the existing in-place fast path and incur no group cloning.
    fn apply_native_staged(
        &mut self,
        z: &ZSetBatch,
        key_rows: &Rows,
        keys: Vec<Vec<u8>>,
        mut previous: Vec<Option<Vec<columns::OutputCell>>>,
        additions: HashMap<Vec<u8>, TypedKeyRow>,
    ) -> Result<ZSetBatch, EngineError> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("group schema missing".into()))?;
        let template = empty_entry(&self.aggs, schema)?;
        let mut staged = HashMap::with_capacity(keys.len());
        for key in &keys {
            if let Some(entry) = self.groups.get(key) {
                staged.insert(key.clone(), entry.clone());
            }
        }
        let diffs = int64_diffs(z.diff())?;
        for row in 0..z.len() {
            let diff = diffs.value(row);
            if diff == 0 {
                continue;
            }
            let key = key_rows.row(row).as_ref().to_vec();
            let entry = staged.entry(key).or_insert_with(|| template.clone());
            fold_row(entry, &self.aggs, &z.batch, row, diff)?;
        }

        let mut changes = Vec::new();
        for (slot, key) in keys.iter().enumerate() {
            let current = match staged.get(key) {
                Some(entry) if entry.rows != 0 => Some(columns::render(&entry.values)?),
                _ => {
                    staged.remove(key);
                    None
                }
            };
            if previous[slot] != current {
                changes.push(Change {
                    key: key.clone(),
                    previous: previous[slot].take(),
                    current,
                });
            }
        }

        let mut representatives = HashMap::with_capacity(changes.len());
        let native = self.native_keys.as_ref().expect("native path selected");
        for change in &changes {
            let representative = native
                .get(&change.key)
                .or_else(|| additions.get(&change.key))
                .ok_or_else(|| {
                    EngineError::Infrastructure(
                        "native group key representative missing during staged output".into(),
                    )
                })?;
            representatives.insert(change.key.clone(), representative.clone());
        }
        let output = materialize::materialize(
            &self.key,
            &self.aggs,
            self.schema.as_ref(),
            self.converter.as_ref(),
            Some(&representatives),
            &changes,
        )?;

        for key in &keys {
            self.groups.remove(key);
        }
        self.groups.extend(staged);
        let cache = self.native_keys.as_mut().expect("native path selected");
        for (key, representative) in additions {
            if self.groups.contains_key(&key) {
                cache.insert(key, representative);
            }
        }
        cache.retain(|key, _| self.groups.contains_key(key));
        #[cfg(test)]
        {
            self.work = keys.len() as u64;
        }
        Ok(output)
    }

    /// Defers to [`materialize`] with this reducer's frozen schema.
    fn materialize(&self, changes: &[Change]) -> Result<ZSetBatch, EngineError> {
        materialize::materialize(
            &self.key,
            &self.aggs,
            self.schema.as_ref(),
            self.converter.as_ref(),
            self.native_keys.as_ref(),
            changes,
        )
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod minmax_tests;

#[cfg(test)]
mod special_tests;

#[cfg(test)]
mod copy_tests;

#[cfg(test)]
mod atomic_tests;
