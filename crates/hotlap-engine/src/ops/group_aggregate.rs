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
mod columns;
mod fold;
mod materialize;
pub(crate) mod read;
mod snapshot;
mod validate;

use std::collections::HashMap;

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, Rows};

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
    #[cfg(test)]
    work: u64,
}

impl GroupAggregate {
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
        self.materialize(&changes)
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
        if self.key.is_empty() || self.key.iter().any(|&index| index >= width) {
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
                self.converter = Some(converter_for(&schema, &self.key)?);
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

    /// Defers to [`materialize`] with this reducer's frozen schema.
    fn materialize(&self, changes: &[Change]) -> Result<ZSetBatch, EngineError> {
        materialize::materialize(
            &self.key,
            &self.aggs,
            self.schema.as_ref(),
            self.converter.as_ref(),
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
