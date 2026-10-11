use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::OwnedRow;
use hotlap_core::ZSetBatch;

use crate::arrange::{KeyedArrangement, state::RowState};

use super::domain::NotEligible;
use super::domain::{count_bound, row_after, side_after};
use super::support::{Key, Output};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Summary {
    pub key: Key,
    pub left: u128,
    pub right: u128,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Work {
    pub row_lookups: usize,
    pub touched_keys: usize,
}

struct StagedRow {
    key: OwnedRow,
    key_value: Key,
    payload: OwnedRow,
    net: i128,
    absolute: u128,
}

struct StagedKey {
    key_value: Key,
    net: i128,
    absolute: u128,
}

pub(super) struct Candidate {
    left: KeyedArrangement,
    right: KeyedArrangement,
    summaries: HashMap<Vec<u8>, Summary>,
    output_schema: SchemaRef,
    work: Work,
    fail_output: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StateDigest {
    left: Vec<(Vec<u8>, Vec<u8>, i64)>,
    right: Vec<(Vec<u8>, Vec<u8>, i64)>,
    summaries: BTreeMap<Vec<u8>, Summary>,
    output: Output,
    work: Work,
}

impl Candidate {
    pub(super) fn new(
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        left_key: usize,
        right_key: usize,
    ) -> Result<Self, NotEligible> {
        validate_schema(&left_schema, left_key)?;
        validate_schema(&right_schema, right_key)?;
        let left = KeyedArrangement::new(left_schema.clone(), &[left_key])
            .map_err(|_| NotEligible::Schema)?;
        let right = KeyedArrangement::new(right_schema.clone(), &[right_key])
            .map_err(|_| NotEligible::Schema)?;
        let output_schema = reference_output_schema(
            left_schema.clone(),
            right_schema.clone(),
            left_key,
            right_key,
        )?;
        Ok(Self {
            left,
            right,
            summaries: HashMap::new(),
            output_schema,
            work: Work::default(),
            fail_output: false,
        })
    }

    pub(super) fn apply(
        &mut self,
        left: &ZSetBatch,
        right: &ZSetBatch,
    ) -> Result<ZSetBatch, NotEligible> {
        let staged_left = stage(left, &self.left)?;
        let staged_right = stage(right, &self.right)?;
        let mut keys: HashMap<Vec<u8>, StagedKey> = HashMap::new();
        collect_key_totals(&mut keys, &staged_left)?;
        collect_key_totals(&mut keys, &staged_right)?;

        let mut left_sides = HashMap::new();
        let mut right_sides = HashMap::new();
        let mut left_after = Vec::with_capacity(staged_left.len());
        let mut right_after = Vec::with_capacity(staged_right.len());
        let work = Work {
            row_lookups: staged_left.len() + staged_right.len(),
            touched_keys: keys.len(),
        };
        for row in &staged_left {
            let old = self
                .left
                .state
                .get(row.key.as_ref())
                .and_then(|rows| rows.get(&row.payload))
                .copied()
                .unwrap_or(0);
            let after = row_after(old, row.net, row.absolute)
                .ok_or_else(|| row_error(old, row.net, row.absolute))?;
            left_after.push((after, old));
            add_side(&mut left_sides, row.key.as_ref(), row.net, row.absolute)?;
        }
        for row in &staged_right {
            let old = self
                .right
                .state
                .get(row.key.as_ref())
                .and_then(|rows| rows.get(&row.payload))
                .copied()
                .unwrap_or(0);
            let after = row_after(old, row.net, row.absolute)
                .ok_or_else(|| row_error(old, row.net, row.absolute))?;
            right_after.push((after, old));
            add_side(&mut right_sides, row.key.as_ref(), row.net, row.absolute)?;
        }

        let mut changes = Vec::<(Key, i64, i64)>::new();
        let mut summary_after = Vec::with_capacity(keys.len());
        for (bytes, staged) in &keys {
            let previous = self.summaries.get(bytes).cloned().unwrap_or(Summary {
                key: staged.key_value,
                left: 0,
                right: 0,
            });
            let (left_net, left_abs) = left_sides.get(bytes).copied().unwrap_or_default();
            let (right_net, right_abs) = right_sides.get(bytes).copied().unwrap_or_default();
            let left = side_after(previous.left, left_net).ok_or(NotEligible::NegativeState)?;
            let right = side_after(previous.right, right_net).ok_or(NotEligible::NegativeState)?;
            count_bound(previous.left, previous.right, left_abs, right_abs)
                .ok_or(NotEligible::NumericBound)?;
            let new_count = count_bound(left, right, 0, 0).ok_or(NotEligible::NumericBound)?;
            let old_count = previous
                .left
                .checked_mul(previous.right)
                .ok_or(NotEligible::NumericBound)?;
            if old_count != new_count {
                if old_count != 0 {
                    changes.push((
                        previous.key,
                        i64::try_from(old_count).map_err(|_| NotEligible::NumericBound)?,
                        -1,
                    ));
                }
                if new_count != 0 {
                    changes.push((
                        staged.key_value,
                        i64::try_from(new_count).map_err(|_| NotEligible::NumericBound)?,
                        1,
                    ));
                }
            }
            summary_after.push((
                bytes.clone(),
                Summary {
                    key: previous.key,
                    left,
                    right,
                },
            ));
        }
        let output = self.build_output(&changes)?;

        commit_rows(&mut self.left.state, &staged_left, &left_after);
        commit_rows(&mut self.right.state, &staged_right, &right_after);
        for (bytes, summary) in summary_after {
            if summary.left == 0 && summary.right == 0 {
                self.summaries.remove(&bytes);
            } else {
                self.summaries.insert(bytes, summary);
            }
        }
        self.work = work;
        Ok(output)
    }

    fn build_output(&self, changes: &[(Key, i64, i64)]) -> Result<ZSetBatch, NotEligible> {
        if self.fail_output {
            return Err(NotEligible::Encoding);
        }
        let key_array = Int64Array::from(changes.iter().map(|change| change.0).collect::<Vec<_>>());
        let count_array = Int64Array::from(
            changes
                .iter()
                .map(|change| Some(change.1))
                .collect::<Vec<_>>(),
        );
        let diffs = Int64Array::from(
            changes
                .iter()
                .map(|change| Some(change.2))
                .collect::<Vec<_>>(),
        );
        let mut columns = Vec::with_capacity(self.output_schema.fields().len());
        for index in 0..self.output_schema.fields().len() {
            let array: ArrayRef = match index {
                0 => Arc::new(key_array.clone()),
                1 => Arc::new(count_array.clone()),
                _ => return Err(NotEligible::Encoding),
            };
            columns.push(array);
        }
        let batch = RecordBatch::try_new(self.output_schema.clone(), columns)
            .map_err(|_| NotEligible::Encoding)?;
        let raw = ZSetBatch::new(batch, Arc::new(diffs)).map_err(|_| NotEligible::Encoding)?;
        crate::zset::consolidate(&raw).map_err(|_| NotEligible::Encoding)
    }

    pub(super) fn snapshot(&self) -> Output {
        let mut result = Output::new();
        for summary in self.summaries.values() {
            let count = summary.left * summary.right;
            if count != 0
                && let Ok(count) = i64::try_from(count)
            {
                result.insert((summary.key, count), 1);
            }
        }
        result
    }

    pub(super) fn work(&self) -> Work {
        self.work
    }
    pub(super) fn summary_count(&self) -> usize {
        self.summaries.len()
    }
    pub(super) fn digest(&self) -> StateDigest {
        let mut summaries: Vec<_> = self
            .summaries
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        summaries.sort_by(|left, right| left.0.cmp(&right.0));
        StateDigest {
            left: self.left.iter().collect(),
            right: self.right.iter().collect(),
            summaries: summaries.into_iter().collect(),
            output: self.snapshot(),
            work: self.work,
        }
    }
    pub(super) fn inject_output_failure(&mut self, enabled: bool) {
        self.fail_output = enabled;
    }
}

fn validate_schema(schema: &Schema, key: usize) -> Result<(), NotEligible> {
    if key >= schema.fields().len()
        || schema.fields()[key].data_type() != &DataType::Int64
        || schema
            .fields()
            .iter()
            .any(|field| field.data_type() != &DataType::Int64)
    {
        return Err(NotEligible::Schema);
    }
    Ok(())
}

fn reference_output_schema(
    left: SchemaRef,
    right: SchemaRef,
    left_key: usize,
    right_key: usize,
) -> Result<SchemaRef, NotEligible> {
    let mut join = crate::ops::join::Join::new(&[left_key], &[right_key]);
    let joined = join
        .apply(
            &super::support::empty_batch(left),
            &super::support::empty_batch(right),
        )
        .map_err(|_| NotEligible::Schema)?;
    let mut group = crate::ops::group_aggregate::GroupAggregate::new(
        &[left_key],
        vec![hotlap_core::AggSpec::count()],
    );
    Ok(group
        .apply(&joined)
        .map_err(|_| NotEligible::Schema)?
        .schema())
}

fn stage(batch: &ZSetBatch, arrangement: &KeyedArrangement) -> Result<Vec<StagedRow>, NotEligible> {
    if batch.schema() != arrangement.schema || batch.diff().data_type() != &DataType::Int64 {
        return Err(NotEligible::Schema);
    }
    let mut selected = Vec::with_capacity(arrangement.key_indices.len());
    selected.extend(
        arrangement
            .key_indices
            .iter()
            .map(|&index| batch.batch.column(index).clone()),
    );
    let key_rows = arrangement
        .key_converter
        .convert_columns(&selected)
        .map_err(|_| NotEligible::Encoding)?;
    let payload_columns: Vec<ArrayRef> = arrangement
        .payload_indices
        .iter()
        .map(|&i| batch.batch.column(i).clone())
        .collect();
    let payload_rows = if payload_columns.is_empty() {
        None
    } else {
        Some(
            arrangement
                .payload_converter
                .convert_columns(&payload_columns)
                .map_err(|_| NotEligible::Encoding)?,
        )
    };
    let diffs = batch
        .diff()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(NotEligible::Schema)?;
    let mut rows: HashMap<(OwnedRow, OwnedRow), (Key, i128, u128)> = HashMap::new();
    for index in 0..batch.len() {
        if diffs.is_null(index) {
            return Err(NotEligible::Schema);
        }
        let diff = i128::from(diffs.value(index));
        let key_row = key_rows.row(index).owned();
        let key_value = if batch
            .batch
            .column(arrangement.key_indices[0])
            .is_null(index)
        {
            None
        } else {
            Some(
                batch
                    .batch
                    .column(arrangement.key_indices[0])
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or(NotEligible::Schema)?
                    .value(index),
            )
        };
        let entry = rows
            .entry((
                key_row,
                payload_rows.as_ref().map_or_else(
                    || arrangement.unit_payload(),
                    |rows| rows.row(index).owned(),
                ),
            ))
            .or_insert((key_value, 0, 0));
        entry.1 = entry.1.checked_add(diff).ok_or(NotEligible::NumericBound)?;
        entry.2 = entry
            .2
            .checked_add(diff.unsigned_abs())
            .ok_or(NotEligible::NumericBound)?;
    }
    Ok(rows
        .into_iter()
        .map(|((key, payload), (key_value, net, absolute))| StagedRow {
            key,
            key_value,
            payload,
            net,
            absolute,
        })
        .collect())
}

fn collect_key_totals(
    keys: &mut HashMap<Vec<u8>, StagedKey>,
    rows: &[StagedRow],
) -> Result<(), NotEligible> {
    for row in rows {
        let entry = keys
            .entry(row.key.as_ref().to_vec())
            .or_insert_with(|| StagedKey {
                key_value: row.key_value,
                net: 0,
                absolute: 0,
            });
        entry.net = entry
            .net
            .checked_add(row.net)
            .ok_or(NotEligible::NumericBound)?;
        entry.absolute = entry
            .absolute
            .checked_add(row.absolute)
            .ok_or(NotEligible::NumericBound)?;
    }
    Ok(())
}

fn add_side(
    sides: &mut HashMap<Vec<u8>, (i128, u128)>,
    key: &[u8],
    net: i128,
    absolute: u128,
) -> Result<(), NotEligible> {
    let entry = sides.entry(key.to_vec()).or_default();
    entry.0 = entry.0.checked_add(net).ok_or(NotEligible::NumericBound)?;
    entry.1 = entry
        .1
        .checked_add(absolute)
        .ok_or(NotEligible::NumericBound)?;
    Ok(())
}

fn row_error(old: i64, _net: i128, absolute: u128) -> NotEligible {
    if (old as u128)
        .checked_add(absolute)
        .is_none_or(|bound| bound > i64::MAX as u128)
    {
        NotEligible::RowBound
    } else {
        NotEligible::NegativeState
    }
}

fn commit_rows(state: &mut RowState, rows: &[StagedRow], after: &[(i64, i64)]) {
    for (row, &(value, old)) in rows.iter().zip(after) {
        let delta = i128::from(value) - i128::from(old);
        if delta != 0 {
            let delta = i64::try_from(delta).expect("validated row delta");
            state.add(row.key.clone(), row.payload.clone(), delta);
        }
    }
}
