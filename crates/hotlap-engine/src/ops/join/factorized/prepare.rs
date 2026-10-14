use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::row::RowConverter;
use hotlap_core::plan::{AggFunc, AggSpec};
use hotlap_core::plan_normalize::{ColumnMapping, Recognition};
use hotlap_core::snapshot::{AggValue, GroupEntry};

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;

use super::groups::aggregate_groups;
use super::history::validate_old_group_state;
use super::keyed::compute_key_stats;
use super::stage::{SideStage, TouchedRow, stage_side_cached};

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum DenseReason {
    Unrecognized,
    StateShape,
    InputMapping,
    NegativeOrOverflow,
    UnsupportedNumeric,
    UnsupportedConsumer,
    InvalidChunkSize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ConsumerCaps {
    FactorizedAggregates,
    FlatRows { max_rows: usize },
}

#[allow(dead_code)]
impl ConsumerCaps {
    pub const DEFAULT_FLAT_CHUNK_ROWS: usize = 1024;

    pub const fn flat_rows(max_rows: usize) -> Self {
        Self::FlatRows { max_rows }
    }
}

#[derive(Clone)]
pub struct DeltaPair {
    pub epoch: u64,
    pub left: Arc<ZSetBatch>,
    pub right: Arc<ZSetBatch>,
}

impl DeltaPair {
    pub fn new(epoch: u64, left: Arc<ZSetBatch>, right: Arc<ZSetBatch>) -> Self {
        Self { epoch, left, right }
    }
}

#[allow(dead_code)]
pub struct PreparedCut {
    pub epoch: u64,
    pub inputs: DeltaPair,
    pub mapping: ColumnMapping,
    pub left: Vec<TouchedRow>,
    pub right: Vec<TouchedRow>,
    pub groups: Vec<GroupChange>,
    /// Prior entries for touched groups, retained for old-row changelog rendering.
    pub prior_groups: Vec<(Vec<u8>, GroupEntry)>,
    pub side_bounds: (u128, u128),
    pub staged_touched: Vec<(Vec<u8>, Vec<u8>, i64)>,
    /// Present only for a guarded flat-row consumer; already fully consolidated.
    pub flat_rows: Option<ZSetBatch>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupChange {
    Upsert { key: Vec<u8>, entry: GroupEntry },
    Remove { key: Vec<u8> },
}

#[allow(clippy::large_enum_variant)]
#[allow(dead_code)]
pub enum CutDecision {
    Dense(DenseReason),
    Eligible(PreparedCut),
}

impl CutDecision {
    #[allow(dead_code)]
    pub(super) fn prepared(&self) -> Option<&PreparedCut> {
        match self {
            Self::Dense(_) => None,
            Self::Eligible(prepared) => Some(prepared),
        }
    }
}

pub struct OldState<'a> {
    pub left: &'a KeyedArrangement,
    pub right: &'a KeyedArrangement,
    pub groups: &'a HashMap<Vec<u8>, GroupEntry>,
}

pub(super) struct OldBuckets<'a> {
    left_arrangement: &'a KeyedArrangement,
    right_arrangement: &'a KeyedArrangement,
    left: HashMap<Vec<u8>, ZSetBatch>,
    right: HashMap<Vec<u8>, ZSetBatch>,
}

impl<'a> OldBuckets<'a> {
    pub(super) fn new(left: &'a KeyedArrangement, right: &'a KeyedArrangement) -> Self {
        Self {
            left_arrangement: left,
            right_arrangement: right,
            left: HashMap::new(),
            right: HashMap::new(),
        }
    }

    pub(super) fn is_left(&self, arrangement: &KeyedArrangement) -> bool {
        std::ptr::eq(self.left_arrangement, arrangement)
    }

    pub(super) fn left(&mut self, key: &[u8]) -> Result<ZSetBatch, EngineError> {
        if let Some(batch) = self.left.get(key) {
            return Ok(batch.clone());
        }
        #[cfg(test)]
        super::OLD_BUCKET_DECODES.with(|count| count.set(count.get() + 1));
        let batch = self.left_arrangement.to_zset_for_key(key)?;
        self.left.insert(key.to_vec(), batch.clone());
        Ok(batch)
    }

    pub(super) fn right(&mut self, key: &[u8]) -> Result<ZSetBatch, EngineError> {
        if let Some(batch) = self.right.get(key) {
            return Ok(batch.clone());
        }
        #[cfg(test)]
        super::OLD_BUCKET_DECODES.with(|count| count.set(count.get() + 1));
        let batch = self.right_arrangement.to_zset_for_key(key)?;
        self.right.insert(key.to_vec(), batch.clone());
        Ok(batch)
    }
}

pub struct PrepareInput<'a> {
    pub recognition: &'a Recognition,
    pub aggregates: &'a [AggSpec],
    pub inputs: DeltaPair,
    pub state: OldState<'a>,
    pub consumer: ConsumerCaps,
}

pub fn prepare(request: PrepareInput<'_>) -> CutDecision {
    #[cfg(test)]
    let mut phase = std::time::Instant::now();
    #[cfg(test)]
    let mut record_phase = |index: usize| {
        super::PREPARE_STAGE_NS.with(|value| {
            let mut values = value.get();
            values[index] = values[index].saturating_add(phase.elapsed().as_nanos());
            value.set(values);
        });
        phase = std::time::Instant::now();
    };
    let PrepareInput {
        recognition,
        aggregates,
        inputs,
        state,
        consumer,
    } = request;
    let Recognition::Eligible(mapping) = recognition else {
        return CutDecision::Dense(DenseReason::Unrecognized);
    };
    let flat_chunk_rows = match consumer {
        ConsumerCaps::FactorizedAggregates => None,
        ConsumerCaps::FlatRows { max_rows } => {
            if super::flat::validate_chunk_size(max_rows).is_err() {
                return CutDecision::Dense(DenseReason::InvalidChunkSize);
            }
            Some(max_rows)
        }
    };
    let flat_rows = flat_chunk_rows.is_some();
    if flat_rows {
        if !aggregates.is_empty()
            || !mapping.aggregate_inputs.is_empty()
            || !mapping.aggregate_input_types.is_empty()
        {
            return CutDecision::Dense(DenseReason::UnsupportedConsumer);
        }
        if !state.groups.is_empty() {
            return CutDecision::Dense(DenseReason::StateShape);
        }
    } else if aggregates.is_empty()
        || aggregates.len() != mapping.aggregate_inputs.len()
        || aggregates.len() != mapping.aggregate_input_types.len()
        || state
            .groups
            .values()
            .any(|entry| entry.values.len() != aggregates.len())
    {
        return CutDecision::Dense(DenseReason::StateShape);
    }
    if !flat_rows
        && state.groups.values().any(|entry| {
            entry.rows <= 0
                || entry.values.len() != aggregates.len()
                || entry.values.iter().zip(aggregates).any(|(value, agg)| {
                    match (agg.func, agg.input, value) {
                        (AggFunc::Count, None, AggValue::Count(count)) => {
                            *count != entry.rows || *count < 0
                        }
                        (AggFunc::Count, Some(_), AggValue::Count(count)) => {
                            *count < 0 || *count > entry.rows
                        }
                        (AggFunc::Sum, Some(_), AggValue::SumInteger { sum, count }) => {
                            *count < 0 || *count > entry.rows || i64::try_from(*sum).is_err()
                        }
                        (AggFunc::Avg, Some(_), AggValue::Avg { count, .. }) => {
                            *count < 0 || *count > entry.rows
                        }
                        _ => true,
                    }
                })
        })
    {
        return CutDecision::Dense(DenseReason::StateShape);
    }
    if state.left.key_indices() != mapping.left_join_key
        || state.right.key_indices() != mapping.right_join_key
        || inputs.left.schema().as_ref() != state.left.schema().as_ref()
        || inputs.right.schema().as_ref() != state.right.schema().as_ref()
    {
        return CutDecision::Dense(DenseReason::InputMapping);
    }
    #[cfg(test)]
    record_phase(0);
    if !flat_rows {
        for ((agg, mapped_input), ty) in aggregates
            .iter()
            .zip(&mapping.aggregate_inputs)
            .zip(&mapping.aggregate_input_types)
        {
            if agg.input != *mapped_input {
                return CutDecision::Dense(DenseReason::InputMapping);
            }
            if !matches!(
                (agg.func, agg.input, ty),
                (AggFunc::Count, None, None)
                    | (AggFunc::Count, Some(_), Some(_))
                    | (
                        AggFunc::Sum,
                        Some(_),
                        Some(DataType::Int32 | DataType::Int64)
                    )
                    | (
                        AggFunc::Avg,
                        Some(_),
                        Some(DataType::Int32 | DataType::Int64)
                    )
            ) {
                return CutDecision::Dense(DenseReason::UnsupportedNumeric);
            }
        }
    }
    let left_extra = encoded_keys(&inputs.right, &mapping.right_join_key);
    let right_extra = encoded_keys(&inputs.left, &mapping.left_join_key);
    let (left_extra, right_extra) = match (left_extra, right_extra) {
        (Ok(left), Ok(right)) => (left, right),
        _ => return CutDecision::Dense(DenseReason::InputMapping),
    };
    let mut buckets = OldBuckets::new(state.left, state.right);
    let left = match stage_side_cached(
        &inputs.left,
        &mapping.left_join_key,
        &left_extra,
        state.left,
        &mut buckets,
    ) {
        Ok(value) => value,
        Err(_) => return CutDecision::Dense(DenseReason::NegativeOrOverflow),
    };
    let right = match stage_side_cached(
        &inputs.right,
        &mapping.right_join_key,
        &right_extra,
        state.right,
        &mut buckets,
    ) {
        Ok(value) => value,
        Err(_) => return CutDecision::Dense(DenseReason::NegativeOrOverflow),
    };
    let bounds = match if flat_rows {
        validate_flat_pair_bounds(&left, &right)
    } else {
        validate_pair_bounds(&left, &right)
    } {
        Some(value) => value,
        None => return CutDecision::Dense(DenseReason::NegativeOrOverflow),
    };
    #[cfg(test)]
    record_phase(1);
    let touched: HashSet<Vec<u8>> = left
        .rows
        .iter()
        .chain(&right.rows)
        .map(|r| r.identity.join_key.clone())
        .collect();
    let mut staged_touched = Vec::new();
    for (side, schema, keys) in [
        (&left, inputs.left.schema(), &mapping.left_join_key),
        (&right, inputs.right.schema(), &mapping.right_join_key),
    ] {
        match super::stage::decode_rows(&schema, keys, &side.rows) {
            Ok(rows) => staged_touched.extend(rows),
            Err(_) => return CutDecision::Dense(DenseReason::InputMapping),
        }
    }
    #[cfg(test)]
    record_phase(2);
    if flat_rows {
        let flat_rows = match super::flat::FlatRowsCursor::from_join_delta(
            state.left,
            state.right,
            &inputs.left,
            &inputs.right,
            &mapping.left_join_key,
            &mapping.right_join_key,
            flat_chunk_rows.expect("flat capability carries a positive chunk size"),
        )
        .and_then(super::flat::FlatRowsCursor::collect)
        {
            Ok(rows) => rows,
            Err(_) => return CutDecision::Dense(DenseReason::NegativeOrOverflow),
        };
        return CutDecision::Eligible(PreparedCut {
            epoch: inputs.epoch,
            inputs,
            mapping: mapping.clone(),
            left: left.rows,
            right: right.rows,
            groups: Vec::new(),
            prior_groups: Vec::new(),
            side_bounds: bounds,
            staged_touched,
            flat_rows: Some(flat_rows),
        });
    }
    let state_abs_bounds = match validate_old_group_state(
        mapping,
        aggregates,
        &touched,
        state.groups,
        state.left,
        state.right,
        &inputs,
        &mut buckets,
    ) {
        Ok(bounds) => bounds,
        Err(reason) => return CutDecision::Dense(reason),
    };
    #[cfg(test)]
    record_phase(3);
    let key_stats =
        match compute_key_stats(mapping, aggregates, &inputs, &left, &touched, &mut buckets) {
            Ok(value) => value,
            Err(reason) => return CutDecision::Dense(reason),
        };
    #[cfg(test)]
    record_phase(4);
    let candidates = match aggregate_groups(
        mapping,
        aggregates,
        state.groups,
        &key_stats.by_join_key,
        &key_stats.group_by_join_key,
        &key_stats.row_deltas,
        &state_abs_bounds,
    ) {
        Some(value) => value,
        None => return CutDecision::Dense(DenseReason::UnsupportedNumeric),
    };
    #[cfg(test)]
    record_phase(5);
    let changed_keys: HashSet<Vec<u8>> = candidates
        .iter()
        .map(|change| match change {
            GroupChange::Upsert { key, .. } | GroupChange::Remove { key } => key.clone(),
        })
        .collect();
    let prior_groups = state
        .groups
        .iter()
        .filter(|(key, _)| changed_keys.contains(*key))
        .map(|(key, entry)| (key.clone(), entry.clone()))
        .collect();
    #[cfg(test)]
    record_phase(6);
    CutDecision::Eligible(PreparedCut {
        epoch: inputs.epoch,
        inputs,
        mapping: mapping.clone(),
        left: left.rows,
        right: right.rows,
        groups: candidates,
        prior_groups,
        side_bounds: bounds,
        staged_touched,
        flat_rows: None,
    })
}

fn encoded_keys(batch: &ZSetBatch, columns: &[usize]) -> Result<Vec<Vec<u8>>, EngineError> {
    let converter = KeyConverter::new(batch.schema().as_ref(), columns)?;
    let rows = converter.convert(batch.batch.columns())?;
    let mut keys = (0..rows.num_rows())
        .map(|i| rows.row(i).as_ref().to_vec())
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    Ok(keys)
}

pub(super) fn group_key_for(
    join: &RowConverter,
    group: &RowConverter,
    key: &[u8],
    positions: &[usize],
) -> Result<Vec<u8>, EngineError> {
    let parser = join.parser();
    let row = parser.parse(key);
    let arrays = join.convert_rows([row])?;
    let selected: Vec<_> = positions
        .iter()
        .map(|&i| {
            arrays
                .get(i)
                .cloned()
                .ok_or_else(|| EngineError::Unsupported("group key position out of range".into()))
        })
        .collect::<Result<_, _>>()?;
    let encoded = group.convert_columns(&selected)?;
    Ok(encoded.row(0).as_ref().to_vec())
}

pub(super) fn bucket_for(
    z: &ZSetBatch,
    key: &KeyConverter,
    target: &[u8],
) -> Result<ZSetBatch, EngineError> {
    let rows = key.convert(z.batch.columns())?;
    let selected: Vec<u32> = (0..rows.num_rows())
        .filter(|&i| rows.row(i).as_ref() == target)
        .map(|i| i as u32)
        .collect();
    let indices = arrow::array::UInt32Array::from(selected);
    let cols = z
        .batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(z.schema(), cols)?;
    let diff = arrow::compute::take(z.diff().as_ref(), &indices, None)?;
    Ok(ZSetBatch::new(batch, Arc::new(diff))?)
}

pub(super) fn combine_bucket(old: &ZSetBatch, delta: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
    if old.is_empty() {
        return crate::zset::consolidate(delta);
    }
    if delta.is_empty() {
        return crate::zset::consolidate(old);
    }
    let mut rows = Vec::new();
    let mut diffs = Vec::new();
    for z in [old, delta] {
        let d = z
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| EngineError::Unsupported("diff not i64".into()))?;
        for row in 0..z.len() {
            let values = z
                .batch
                .columns()
                .iter()
                .map(|a| {
                    arrow::compute::take(
                        a.as_ref(),
                        &arrow::array::UInt32Array::from(vec![row as u32]),
                        None,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            rows.push(values);
            diffs.push(d.value(row));
        }
    }
    let mut columns = Vec::new();
    for col in 0..old.batch.num_columns() {
        let arrays: Vec<_> = rows.iter().map(|r| r[col].as_ref()).collect();
        columns.push(arrow::compute::concat(arrays.as_slice())?);
    }
    let batch = RecordBatch::try_new(old.schema(), columns)?;
    let combined = ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs)))?;
    crate::zset::consolidate(&combined)
}

fn validate_pair_bounds(left: &SideStage, right: &SideStage) -> Option<(u128, u128)> {
    let keys: HashSet<_> = left
        .key_row_counts
        .keys()
        .chain(right.key_row_counts.keys())
        .cloned()
        .collect();
    let mut bound = 0_u128;
    let mut old = 0_u128;
    for key in keys {
        let l = left.key_row_counts.get(&key).copied().unwrap_or(0);
        let r = right.key_row_counts.get(&key).copied().unwrap_or(0);
        let dl = left
            .rows
            .iter()
            .filter(|x| x.identity.join_key == key)
            .try_fold(0_u128, |s, x| {
                s.checked_add(u128::from(x.old.abs_diff(x.new)))
            })?;
        let dr = right
            .rows
            .iter()
            .filter(|x| x.identity.join_key == key)
            .try_fold(0_u128, |s, x| {
                s.checked_add(u128::from(x.old.abs_diff(x.new)))
            })?;
        let b = dl
            .checked_mul(r)?
            .checked_add(l.checked_mul(dr)?)?
            .checked_add(dl.checked_mul(dr)?)?;
        if l.checked_mul(r)?.checked_add(b)? > i64::MAX as u128 {
            return None;
        }
        old = old.checked_add(l.checked_mul(r)?)?;
        bound = bound.checked_add(b)?;
    }
    Some((bound, old))
}

/// Conservative full-key proof for flat weighted terms, based on raw input
/// absolute bounds rather than net row deltas. This proves each pair product,
/// every per-row term subtotal, and old/proposed full-key multiplicity totals.
fn validate_flat_pair_bounds(left: &SideStage, right: &SideStage) -> Option<(u128, u128)> {
    let keys: HashSet<Vec<u8>> = left
        .key_row_counts
        .keys()
        .chain(right.key_row_counts.keys())
        .chain(left.key_delta_abs_bounds.keys())
        .chain(right.key_delta_abs_bounds.keys())
        .cloned()
        .collect();
    let limit = i64::MAX as u128;
    let mut delta_bound = 0_u128;
    let mut old_bound = 0_u128;
    for key in keys {
        let l = left.key_row_counts.get(&key).copied().unwrap_or(0);
        let r = right.key_row_counts.get(&key).copied().unwrap_or(0);
        let dl = left.key_delta_abs_bounds.get(&key).copied().unwrap_or(0);
        let dr = right.key_delta_abs_bounds.get(&key).copied().unwrap_or(0);
        let old_pairs = l.checked_mul(r)?;
        let term_bound = dl
            .checked_mul(r)?
            .checked_add(l.checked_mul(dr)?)?
            .checked_add(dl.checked_mul(dr)?)?;
        let new_left = l.checked_add(dl)?;
        let new_right = r.checked_add(dr)?;
        let new_pairs = new_left.checked_mul(new_right)?;
        if old_pairs > limit || term_bound > limit || new_pairs > limit {
            return None;
        }
        old_bound = old_bound.checked_add(old_pairs)?;
        delta_bound = delta_bound.checked_add(term_bound)?;
    }
    Some((delta_bound, old_bound))
}
