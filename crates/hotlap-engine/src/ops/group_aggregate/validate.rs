//! Pre-validation of a delta so a rejected fold never leaves partial state.

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use arrow::row::Rows;

use hotlap_core::plan::AggSpec;
use hotlap_core::snapshot::{AggValue, GroupEntry, OrderedMultiset};

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::zset::int64_diffs;

use super::accumulate::empty_entry;
use super::fold::fold_row;

/// Validates that folding `z` cannot fail, without mutating `groups`.
///
/// Every fallible update (row-count overflow, integer-sum overflow, negative
/// special multiplicity) is simulated on a scalar-only shadow of each touched
/// group, in input order, so a later error leaves the live state untouched.
/// `min`/`max` multisets are never copied: the shadow uses empty multisets,
/// whose `add` cannot fail.
pub(super) fn validate_delta(
    groups: &HashMap<Vec<u8>, GroupEntry>,
    aggs: &[AggSpec],
    schema: &SchemaRef,
    z: &ZSetBatch,
    key_rows: &Rows,
) -> Result<(), EngineError> {
    let diffs = int64_diffs(z.diff())?;
    let empty = empty_entry(aggs, schema)?;
    let mut shadows: HashMap<Vec<u8>, GroupEntry> = HashMap::new();
    for row in 0..z.len() {
        let diff = diffs.value(row);
        if diff == 0 {
            continue;
        }
        let bytes = key_rows.row(row).as_ref().to_vec();
        let shadow = shadows.entry(bytes.clone()).or_insert_with(|| {
            groups
                .get(&bytes)
                .map(scalar_shadow)
                .unwrap_or_else(|| empty.clone())
        });
        fold_row(shadow, aggs, &z.batch, row, diff)?;
    }
    Ok(())
}

/// Copies only the scalar accumulator state of `entry`; multisets start empty.
fn scalar_shadow(entry: &GroupEntry) -> GroupEntry {
    GroupEntry {
        rows: entry.rows,
        values: entry.values.iter().map(shadow_value).collect(),
    }
}

/// The scalar-only counterpart of `value`; `min`/`max` become empty multisets.
fn shadow_value(value: &AggValue) -> AggValue {
    match value {
        AggValue::Count(count) => AggValue::Count(*count),
        AggValue::SumInteger { sum, count } => AggValue::SumInteger {
            sum: *sum,
            count: *count,
        },
        AggValue::SumFloat {
            sum,
            count,
            nan,
            pos_inf,
            neg_inf,
        } => AggValue::SumFloat {
            sum: *sum,
            count: *count,
            nan: *nan,
            pos_inf: *pos_inf,
            neg_inf: *neg_inf,
        },
        AggValue::Avg {
            sum,
            count,
            nan,
            pos_inf,
            neg_inf,
        } => AggValue::Avg {
            sum: *sum,
            count: *count,
            nan: *nan,
            pos_inf: *pos_inf,
            neg_inf: *neg_inf,
        },
        AggValue::Min(_) => AggValue::Min(OrderedMultiset::default()),
        AggValue::Max(_) => AggValue::Max(OrderedMultiset::default()),
    }
}
