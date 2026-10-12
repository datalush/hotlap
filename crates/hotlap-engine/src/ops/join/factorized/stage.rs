use std::collections::{HashMap, HashSet};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use hotlap_core::snapshot::GroupEntry;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RowIdentity {
    pub join_key: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TouchedRow {
    pub identity: RowIdentity,
    pub old: i64,
    pub new: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SideStage {
    pub rows: Vec<TouchedRow>,
    pub absolute_weight_bound: u128,
    pub old_weight_total: u128,
    pub all_old: Vec<(Vec<u8>, Vec<u8>, i64)>,
    pub key_indices: Vec<usize>,
    pub schema: SchemaRef,
    pub key_row_counts: HashMap<Vec<u8>, u128>,
    /// Sum of absolute raw input diffs per touched join key, before row consolidation.
    pub key_delta_abs_bounds: HashMap<Vec<u8>, u128>,
}

pub(super) fn stage_side(
    batch: &ZSetBatch,
    key_indices: &[usize],
    additional_keys: &[Vec<u8>],
    old: &KeyedArrangement,
) -> Result<SideStage, EngineError> {
    let schema = batch.schema();
    let keys = KeyConverter::new(schema.as_ref(), key_indices)?;
    let payload_indices: Vec<usize> = (0..schema.fields().len())
        .filter(|index| !key_indices.contains(index))
        .collect();
    let payload_converter = converter(schema.clone(), &payload_indices)?;
    let empty_payload = payload_converter.parser().parse(&[]).owned();
    let key_rows = keys.convert(batch.batch.columns())?;
    let payload_columns: Vec<ArrayRef> = payload_indices
        .iter()
        .map(|&index| batch.batch.column(index).clone())
        .collect();
    let payload_rows = if payload_columns.is_empty() {
        None
    } else {
        Some(payload_converter.convert_columns(&payload_columns)?)
    };
    let diffs = batch
        .diff()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| EngineError::Unsupported("factorized diff must be Int64".into()))?;
    let mut old_rows = HashMap::<RowIdentity, i64>::new();
    let mut key_row_counts = HashMap::<Vec<u8>, u128>::new();
    let mut touched_keys = HashSet::new();
    for index in 0..batch.len() {
        touched_keys.insert(key_rows.row(index).as_ref().to_vec());
    }
    touched_keys.extend(additional_keys.iter().cloned());
    for key in &touched_keys {
        let bucket = old.to_zset_for_key(key)?;
        let old_keys = keys.convert(bucket.batch.columns())?;
        let old_payload_cols: Vec<ArrayRef> = payload_indices
            .iter()
            .map(|&i| bucket.batch.column(i).clone())
            .collect();
        let old_payloads = if old_payload_cols.is_empty() {
            None
        } else {
            Some(payload_converter.convert_columns(&old_payload_cols)?)
        };
        let old_diffs = bucket
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| EngineError::Unsupported("old arrangement diff must be Int64".into()))?;
        for row in 0..bucket.len() {
            let identity = RowIdentity {
                join_key: old_keys.row(row).as_ref().to_vec(),
                payload: old_payloads.as_ref().map_or_else(
                    || empty_payload.as_ref().to_vec(),
                    |p| p.row(row).as_ref().to_vec(),
                ),
            };
            let weight = old_diffs.value(row);
            if weight < 0 || old_rows.insert(identity.clone(), weight).is_some() {
                return Err(EngineError::Unsupported(
                    "invalid old arrangement bucket".into(),
                ));
            }
            let total = key_row_counts.entry(identity.join_key.clone()).or_default();
            *total = total
                .checked_add(u128::try_from(weight).map_err(|_| {
                    EngineError::Unsupported("negative old arrangement weight".into())
                })?)
                .ok_or(EngineError::NumericOverflow("old keyed count"))?;
        }
    }
    let mut touched_order = Vec::new();
    let mut touched = HashMap::<RowIdentity, (i64, i64)>::new();
    let mut bounds = HashMap::<Vec<u8>, u128>::new();
    for index in 0..batch.len() {
        if diffs.is_null(index) {
            return Err(EngineError::Unsupported("null factorized diff".into()));
        }
        let delta = diffs.value(index);
        if delta == i64::MIN {
            return Err(EngineError::NumericOverflow("factorized bounded abs"));
        }
        let identity = RowIdentity {
            join_key: key_rows.row(index).as_ref().to_vec(),
            payload: payload_rows.as_ref().map_or_else(
                || empty_payload.as_ref().to_vec(),
                |rows| rows.row(index).as_ref().to_vec(),
            ),
        };
        let old_weight = old_rows.get(&identity).copied().unwrap_or(0);
        let entry = touched.entry(identity.clone()).or_insert_with(|| {
            touched_order.push(identity.clone());
            (old_weight, old_weight)
        });
        let previous_prefix = entry.1;
        if delta < 0 && previous_prefix == 0 {
            return Err(EngineError::Unsupported(
                "retracts absent arrangement identity".into(),
            ));
        }
        let next = entry
            .1
            .checked_add(delta)
            .ok_or(EngineError::NumericOverflow("factorized row multiplicity"))?;
        if next < 0 {
            return Err(EngineError::Unsupported(
                "negative ordered row prefix".into(),
            ));
        }
        entry.1 = next;
        let key_bound = bounds.entry(identity.join_key).or_default();
        *key_bound = key_bound
            .checked_add(u128::from(delta.unsigned_abs()))
            .ok_or(EngineError::NumericOverflow(
                "factorized absolute row bound",
            ))?;
    }
    let rows = touched_order
        .into_iter()
        .map(|identity| {
            let (old, new) = touched
                .remove(&identity)
                .expect("touched identity recorded");
            TouchedRow { identity, old, new }
        })
        .collect();
    let absolute_weight_bound = bounds
        .values()
        .try_fold(0_u128, |sum, value| sum.checked_add(*value))
        .ok_or(EngineError::NumericOverflow(
            "factorized absolute side bound",
        ))?;
    let old_weight_total = key_row_counts
        .values()
        .try_fold(0_u128, |sum, value| sum.checked_add(*value))
        .ok_or(EngineError::NumericOverflow("factorized old side total"))?;
    Ok(SideStage {
        rows,
        absolute_weight_bound,
        old_weight_total,
        all_old: Vec::new(),
        key_indices: key_indices.to_vec(),
        schema: schema.clone(),
        key_row_counts,
        key_delta_abs_bounds: bounds,
    })
}

#[allow(dead_code)]
pub(super) fn encode_group_key(
    schema: SchemaRef,
    source_columns: &[usize],
    group_positions: &[usize],
    key_bytes: &[u8],
) -> Result<Vec<u8>, EngineError> {
    if group_positions.is_empty() {
        return Err(EngineError::Unsupported(
            "empty factorized group key".into(),
        ));
    }
    if group_positions.is_empty() {
        return Err(EngineError::Unsupported(
            "empty factorized group key".into(),
        ));
    }
    let key_converter = converter(schema.clone(), source_columns)?;
    if group_positions
        .iter()
        .any(|&position| position >= source_columns.len())
    {
        return Err(EngineError::Unsupported(
            "group key position out of range".into(),
        ));
    }
    let parser = key_converter.parser();
    let parsed = parser.parse(key_bytes);
    let decoded_key_columns = key_converter.convert_rows([parsed])?;
    let projected: Vec<ArrayRef> = group_positions
        .iter()
        .map(|&position| decoded_key_columns[position].clone())
        .collect();
    let group_columns: Vec<usize> = group_positions
        .iter()
        .map(|&position| source_columns[position])
        .collect();
    let group_converter = converter(schema, &group_columns)?;
    let encoded = group_converter.convert_columns(&projected)?;
    Ok(encoded.row(0).as_ref().to_vec())
}

#[allow(clippy::type_complexity)]
pub(super) fn decode_rows(
    schema: &SchemaRef,
    key_indices: &[usize],
    rows: &[TouchedRow],
) -> Result<Vec<(Vec<u8>, Vec<u8>, i64)>, EngineError> {
    let key_converter = converter(schema.clone(), key_indices)?;
    let payload_indices: Vec<usize> = (0..schema.fields().len())
        .filter(|index| !key_indices.contains(index))
        .collect();
    let payload_converter = converter(schema.clone(), &payload_indices)?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        let key = key_converter.parser().parse(&row.identity.join_key).owned();
        let payload = payload_converter
            .parser()
            .parse(&row.identity.payload)
            .owned();
        output.push((key.as_ref().to_vec(), payload.as_ref().to_vec(), row.new));
    }
    Ok(output)
}

pub(super) fn stage_groups(
    old: &[(Vec<u8>, GroupEntry)],
    changes: &[super::prepare::GroupChange],
) -> Result<Vec<(Vec<u8>, GroupEntry)>, EngineError> {
    let mut staged = HashMap::new();
    for (key, entry) in old {
        if staged.insert(key.clone(), entry.clone()).is_some() {
            return Err(EngineError::Unsupported("duplicate old group key".into()));
        }
    }
    for change in changes {
        match change {
            super::prepare::GroupChange::Upsert { key, entry } => {
                staged.insert(key.clone(), entry.clone());
            }
            super::prepare::GroupChange::Remove { key } => {
                staged.remove(key);
            }
        }
    }
    let mut output: Vec<_> = staged.into_iter().collect();
    output.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(output)
}

#[allow(dead_code)]
pub(super) fn encode_full_row(
    schema: SchemaRef,
    row: &RecordBatch,
    index: usize,
) -> Result<Vec<u8>, EngineError> {
    let indices: Vec<_> = (0..schema.fields().len()).collect();
    let converter = converter(schema, &indices)?;
    let rows = converter.convert_columns(row.columns())?;
    Ok(rows.row(index).as_ref().to_vec())
}

pub(super) fn converter(schema: SchemaRef, columns: &[usize]) -> Result<RowConverter, EngineError> {
    let mut fields = Vec::with_capacity(columns.len());
    for &index in columns {
        let field = schema.fields().get(index).ok_or_else(|| {
            EngineError::Unsupported("factorized column mapping out of range".into())
        })?;
        fields.push(SortField::new(field.data_type().clone()));
    }
    Ok(RowConverter::new(fields)?)
}
