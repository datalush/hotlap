use std::collections::{HashMap, HashSet};

use arrow::row::{RowConverter, SortField};
use hotlap_core::plan::{AggFunc, AggSpec};
use hotlap_core::plan_normalize::ColumnMapping;

use crate::keys::KeyConverter;

use super::prepare::{
    DeltaPair, DenseReason, OldBuckets, bucket_for, combine_bucket, group_key_for,
};
use super::stage::SideStage;
use super::stats::{
    Stats, delta_bound, delta_product, sum_delta_bound, summarize, validate_i128_abs_bound,
};

pub(super) struct KeyStats {
    pub(super) by_join_key: HashMap<Vec<u8>, Vec<Stats>>,
    pub(super) group_by_join_key: HashMap<Vec<u8>, Vec<u8>>,
    pub(super) row_deltas: HashMap<Vec<u8>, (i128, u128)>,
}

pub(super) fn compute_key_stats(
    mapping: &ColumnMapping,
    aggs: &[AggSpec],
    inputs: &DeltaPair,
    _left_stage: &SideStage,
    touched: &HashSet<Vec<u8>>,
    buckets: &mut OldBuckets<'_>,
) -> Result<KeyStats, DenseReason> {
    let left_keys = KeyConverter::new(inputs.left.schema().as_ref(), &mapping.left_join_key)
        .map_err(|_| DenseReason::InputMapping)?;
    let right_keys = KeyConverter::new(inputs.right.schema().as_ref(), &mapping.right_join_key)
        .map_err(|_| DenseReason::InputMapping)?;
    let group_fields = mapping
        .group_key
        .iter()
        .map(|&i| {
            mapping
                .join_key_types
                .get(i)
                .cloned()
                .ok_or(DenseReason::InputMapping)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let group_conv = RowConverter::new(group_fields.into_iter().map(SortField::new).collect())
        .map_err(|_| DenseReason::InputMapping)?;
    let join_conv = RowConverter::new(
        mapping
            .join_key_types
            .iter()
            .cloned()
            .map(SortField::new)
            .collect(),
    )
    .map_err(|_| DenseReason::InputMapping)?;
    let left_width = inputs.left.schema().fields().len();
    let mut stats = HashMap::new();
    let mut groups = HashMap::new();
    let mut row_deltas = HashMap::new();
    for key in touched {
        let old_left = buckets.left(key).map_err(|_| DenseReason::InputMapping)?;
        let old_right = buckets.right(key).map_err(|_| DenseReason::InputMapping)?;
        let delta_left =
            bucket_for(&inputs.left, &left_keys, key).map_err(|_| DenseReason::InputMapping)?;
        let delta_right =
            bucket_for(&inputs.right, &right_keys, key).map_err(|_| DenseReason::InputMapping)?;
        let new_left =
            combine_bucket(&old_left, &delta_left).map_err(|_| DenseReason::StateShape)?;
        let new_right =
            combine_bucket(&old_right, &delta_right).map_err(|_| DenseReason::StateShape)?;
        let group = group_key_for(&join_conv, &group_conv, key, &mapping.group_key)
            .map_err(|_| DenseReason::InputMapping)?;
        groups.insert(key.clone(), group);
        let lo = summarize(&old_left, 0, aggs, mapping, left_width)
            .map_err(|_| DenseReason::UnsupportedNumeric)?;
        let ro = summarize(&old_right, 1, aggs, mapping, left_width)
            .map_err(|_| DenseReason::UnsupportedNumeric)?;
        let ld = summarize(&delta_left, 0, aggs, mapping, left_width)
            .map_err(|_| DenseReason::UnsupportedNumeric)?;
        let rd = summarize(&delta_right, 1, aggs, mapping, left_width)
            .map_err(|_| DenseReason::UnsupportedNumeric)?;
        let ln = summarize(&new_left, 0, aggs, mapping, left_width)
            .map_err(|_| DenseReason::UnsupportedNumeric)?;
        let rn = summarize(&new_right, 1, aggs, mapping, left_width)
            .map_err(|_| DenseReason::UnsupportedNumeric)?;
        let values = aggs
            .iter()
            .enumerate()
            .map(|(i, agg)| {
                aggregate_delta(
                    agg,
                    left_width,
                    BucketStats {
                        old: &lo[i],
                        delta: &ld[i],
                        new: &ln[i],
                    },
                    BucketStats {
                        old: &ro[i],
                        delta: &rd[i],
                        new: &rn[i],
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let row_delta = delta_product(
            lo[0].rows, ld[0].rows, ro[0].rows, rd[0].rows, ln[0].rows, rn[0].rows,
        )?;
        let row_bound = delta_bound(
            ld[0].abs_rows,
            lo[0].abs_rows,
            rd[0].abs_rows,
            ro[0].abs_rows,
        )?;
        row_deltas.insert(key.clone(), (row_delta, row_bound));
        stats.insert(key.clone(), values);
    }
    Ok(KeyStats {
        by_join_key: stats,
        group_by_join_key: groups,
        row_deltas,
    })
}

#[derive(Clone, Copy)]
struct BucketStats<'a> {
    old: &'a Stats,
    delta: &'a Stats,
    new: &'a Stats,
}

fn aggregate_delta(
    agg: &AggSpec,
    left_width: usize,
    left: BucketStats<'_>,
    right: BucketStats<'_>,
) -> Result<Stats, DenseReason> {
    if agg.input.is_none() {
        return Ok(Stats {
            rows: delta_product(
                left.old.rows,
                left.delta.rows,
                right.old.rows,
                right.delta.rows,
                left.new.rows,
                right.new.rows,
            )?,
            abs_rows: delta_bound(
                left.delta.abs_rows,
                left.old.abs_rows,
                right.delta.abs_rows,
                right.old.abs_rows,
            )?,
            ..Stats::default()
        });
    }
    let (value, other) = if agg.input.unwrap() < left_width {
        (left, right)
    } else {
        (right, left)
    };
    let count_delta = delta_product(
        value.old.nonnull,
        value.delta.nonnull,
        other.old.rows,
        other.delta.rows,
        value.new.nonnull,
        other.new.rows,
    )?;
    let count_bound = delta_bound(
        value.delta.abs_nonnull,
        value.old.abs_nonnull,
        other.delta.abs_rows,
        other.old.abs_rows,
    )?;
    if agg.func == AggFunc::Avg
        && (count_bound > (1_u128 << 53) || value.delta.abs_nonnull > (1_u128 << 53))
    {
        return Err(DenseReason::UnsupportedNumeric);
    }
    match agg.func {
        AggFunc::Count => Ok(Stats {
            rows: count_delta,
            abs_rows: count_bound,
            nonnull: count_delta,
            abs_nonnull: count_bound,
            ..Stats::default()
        }),
        AggFunc::Sum | AggFunc::Avg => {
            let sum_delta = delta_product(
                value.old.sum,
                value.delta.sum,
                other.old.rows,
                other.delta.rows,
                value.new.sum,
                other.new.rows,
            )?;
            let abs_sum = sum_delta_bound(
                value.delta.abs_sum,
                value.old.abs_sum,
                other.delta.abs_rows,
                other.old.abs_rows,
            )?;
            validate_i128_abs_bound(abs_sum)?;
            if agg.func == AggFunc::Avg && abs_sum > (1_u128 << 53) {
                return Err(DenseReason::UnsupportedNumeric);
            }
            Ok(Stats {
                rows: count_delta,
                abs_rows: count_bound,
                nonnull: count_delta,
                abs_nonnull: count_bound,
                sum: sum_delta,
                abs_sum,
                ..Stats::default()
            })
        }
        _ => Err(DenseReason::UnsupportedNumeric),
    }
}
