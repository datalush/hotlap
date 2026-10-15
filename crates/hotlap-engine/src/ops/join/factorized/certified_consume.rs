//! COUNT and integer-SUM group consumer over maintained source and group state.

use std::collections::BTreeSet;

use arrow::datatypes::{SchemaRef, i256};
use hotlap_core::plan_normalize::ColumnMapping;
use hotlap_core::snapshot::{AggValue, GroupEntry};

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::ops::{GroupAggregate, Join};

use super::certificate::{self, BoundKind, CertificateDecision};
use super::consume;
use super::group_memo::{Contribution, GroupMemoStage};
use super::prepare::GroupChange;
use super::raw_bounds::{self, KeyNumericBounds};

pub(crate) struct GroupedUpdate<'a> {
    pub(crate) mapping: &'a ColumnMapping,
    pub(crate) left_delta: &'a ZSetBatch,
    pub(crate) right_delta: &'a ZSetBatch,
    pub(crate) output_schema: SchemaRef,
    pub(crate) projection: Option<&'a [usize]>,
}

pub(crate) enum CertifiedDecision {
    Certified(PreparedCertifiedGroups),
    NeedsRows(Option<GroupMemoStage>),
}

/// Maintains the same source/group derived state for the test-only forced-row
/// oracle, without publishing the certified aggregate result.
pub(crate) fn prepare_dense_metadata_with_sources(
    join: &mut Join,
    reducer: &mut GroupAggregate,
    update: GroupedUpdate<'_>,
    sources: &crate::ops::join::PreparedJoinSources,
) -> Result<Option<GroupMemoStage>, EngineError> {
    let GroupedUpdate {
        mapping,
        left_delta,
        right_delta,
        output_schema,
        projection,
    } = update;
    if certificate::compile_metric_bindings(mapping, reducer.aggregates()).is_none() {
        return Ok(None);
    }
    let Some(demand) = super::demand::compile_demand(
        mapping,
        reducer.aggregates(),
        &left_delta.schema(),
        &right_delta.schema(),
    )
    .ok() else {
        return Ok(None);
    };
    join.register_demand(demand)?;
    reducer.initialize_cut(output_schema)?;
    let (Some(left), Some(right)) = (join.left_for_cut(), join.right_for_cut()) else {
        return Err(EngineError::Infrastructure(
            "initialized join has missing source arrangement".into(),
        ));
    };
    let initialized = match projection {
        Some(projection) => {
            reducer.initialize_maintained_cut_projected(mapping, left, right, projection)?
        }
        None => reducer.initialize_maintained_cut(mapping, left, right)?,
    };
    if !initialized {
        return Ok(None);
    }
    stage_all_proposed_memo(join, reducer, sources)
}

pub(crate) struct PreparedCertifiedGroups {
    #[cfg(test)]
    sources: Option<Box<crate::ops::join::PreparedJoinSources>>,
    memo: GroupMemoStage,
    groups: Vec<(Vec<u8>, Option<GroupEntry>)>,
    output: ZSetBatch,
}

impl PreparedCertifiedGroups {
    pub(crate) fn output(&self) -> &ZSetBatch {
        &self.output
    }

    #[cfg(test)]
    pub(crate) fn group_changes(&self) -> &[(Vec<u8>, Option<GroupEntry>)] {
        &self.groups
    }

    /// All possible failures (including rendering) have completed before this
    /// infallible publication point.
    #[cfg(test)]
    pub(crate) fn commit(self, join: &mut Join, reducer: &mut GroupAggregate) {
        let Self {
            sources,
            memo,
            groups,
            output: _,
        } = self;
        if let Some(sources) = sources {
            join.commit_source_updates(*sources);
        }
        reducer.commit_maintained_memo(memo);
        reducer.commit_factorized_consumed(groups);
    }

    /// Publishes only grouped metadata when the caller owns the already-staged
    /// common source update and commits it at the same transaction boundary.
    pub(crate) fn commit_group_state(self, reducer: &mut GroupAggregate) {
        let Self { memo, groups, .. } = self;
        reducer.commit_maintained_memo(memo);
        reducer.commit_factorized_consumed(groups);
    }
}

impl CertifiedDecision {
    /// Call only after the ordinary Dense Join + GroupAggregate path succeeds.
    /// Dense join already updated canonical rows and source stats exactly once.
    pub(crate) fn commit_dense_metadata(self, reducer: &mut GroupAggregate) {
        if let Self::NeedsRows(Some(stage)) = self {
            reducer.commit_maintained_memo(stage);
        }
    }
}

/// Prepares a bounded grouped update from maintained source stats, original raw
/// Arrow prefix bounds, and the authoritative GroupAggregate history. It never
/// materializes joined pairs or calls the Dense operators.
#[cfg(test)]
pub(crate) fn prepare_grouped_update(
    join: &mut Join,
    reducer: &mut GroupAggregate,
    mapping: &ColumnMapping,
    left_delta: &ZSetBatch,
    right_delta: &ZSetBatch,
    output_schema: SchemaRef,
) -> Result<CertifiedDecision, EngineError> {
    prepare_grouped_update_inner(
        join,
        reducer,
        GroupedUpdate {
            mapping,
            left_delta,
            right_delta,
            output_schema,
            projection: None,
        },
        None,
    )
}

/// Same certified preparation using a source update already staged by the
/// graph. This lets a flat fallback reuse that exact update without rebuilding
/// source deltas or source statistics.
pub(crate) fn prepare_grouped_update_with_sources(
    join: &mut Join,
    reducer: &mut GroupAggregate,
    update: GroupedUpdate<'_>,
    sources: &crate::ops::join::PreparedJoinSources,
) -> Result<CertifiedDecision, EngineError> {
    prepare_grouped_update_inner(join, reducer, update, Some(sources))
}

#[cfg(test)]
pub(crate) fn prepare_grouped_update_projected(
    join: &mut Join,
    reducer: &mut GroupAggregate,
    mapping: &ColumnMapping,
    left_delta: &ZSetBatch,
    right_delta: &ZSetBatch,
    output_schema: SchemaRef,
    projection: &[usize],
) -> Result<CertifiedDecision, EngineError> {
    prepare_grouped_update_inner(
        join,
        reducer,
        GroupedUpdate {
            mapping,
            left_delta,
            right_delta,
            output_schema,
            projection: Some(projection),
        },
        None,
    )
}

fn prepare_grouped_update_inner(
    join: &mut Join,
    reducer: &mut GroupAggregate,
    update: GroupedUpdate<'_>,
    preprepared_sources: Option<&crate::ops::join::PreparedJoinSources>,
) -> Result<CertifiedDecision, EngineError> {
    let GroupedUpdate {
        mapping,
        left_delta,
        right_delta,
        output_schema,
        projection,
    } = update;
    // Classify against the actual logical aggregate types before registering
    // source demands or constructing source-derived memo metrics. Valid Dense
    // aggregates outside this integer domain must never be forced through the
    // summary builder (for example Float64 SUM has no exact integer SUM stat).
    let Some(bindings) = certificate::compile_metric_bindings(mapping, reducer.aggregates()) else {
        return Ok(CertifiedDecision::NeedsRows(None));
    };
    // The integer AVG certificate is prepared separately from row consumption;
    // ntf6 will connect it to the renderer after its bitwise-output proof lands.
    if bindings
        .iter()
        .any(|binding| binding.kind == certificate::BoundKind::IntegerAvg)
    {
        return Ok(CertifiedDecision::NeedsRows(None));
    }
    if projection.is_none()
        && (reducer.group_key() != mapping.group_key
            || reducer
                .aggregates()
                .iter()
                .zip(&mapping.aggregate_inputs)
                .any(|(aggregate, input)| aggregate.input != *input))
    {
        return Ok(CertifiedDecision::NeedsRows(None));
    }
    if let Some(projection) = projection
        && !projected_mapping_matches(
            mapping,
            reducer,
            &output_schema,
            projection,
            left_delta.schema().fields().len(),
            right_delta.schema().fields().len(),
        )
    {
        return Ok(CertifiedDecision::NeedsRows(None));
    }
    let Some(demand) = super::demand::compile_demand(
        mapping,
        reducer.aggregates(),
        &left_delta.schema(),
        &right_delta.schema(),
    )
    .ok() else {
        return Ok(CertifiedDecision::NeedsRows(None));
    };
    if join.cut_schema(left_delta, right_delta).is_err()
        || join.register_demand(demand).is_err()
        || reducer.initialize_cut(output_schema).is_err()
    {
        return Ok(CertifiedDecision::NeedsRows(None));
    }
    let (Some(left), Some(right)) = (join.left_for_cut(), join.right_for_cut()) else {
        return Err(EngineError::Infrastructure(
            "initialized join has missing source arrangement".into(),
        ));
    };
    let initialized = match projection {
        Some(projection) => {
            reducer.initialize_maintained_cut_projected(mapping, left, right, projection)?
        }
        None => reducer.initialize_maintained_cut(mapping, left, right)?,
    };
    if !initialized {
        return Ok(CertifiedDecision::NeedsRows(None));
    }
    let Some(cut) = reducer.maintained_cut() else {
        return Ok(CertifiedDecision::NeedsRows(None));
    };
    // The common arrangement owner stages both row weights and their exact
    // source metadata before any candidate result is made visible.
    #[cfg(test)]
    let mut owned_sources = None;
    let sources = match preprepared_sources {
        Some(sources) => sources,
        None => {
            #[cfg(test)]
            {
                owned_sources = Some(join.prepare_source_updates(left_delta, right_delta)?);
                owned_sources
                    .as_ref()
                    .expect("the source update was prepared above")
            }
            #[cfg(not(test))]
            return Ok(CertifiedDecision::NeedsRows(None));
        }
    };
    let left_raw = match raw_bounds::collect_raw_delta(
        left_delta,
        &mapping.left_join_key,
        &cut.left_raw_columns,
    ) {
        Ok(raw) => raw,
        Err(_) => {
            let stage = stage_all_proposed_memo(join, reducer, sources)?;
            return Ok(CertifiedDecision::NeedsRows(stage));
        }
    };
    let right_raw = match raw_bounds::collect_raw_delta(
        right_delta,
        &mapping.right_join_key,
        &cut.right_raw_columns,
    ) {
        Ok(raw) => raw,
        Err(_) => {
            let stage = stage_all_proposed_memo(join, reducer, sources)?;
            return Ok(CertifiedDecision::NeedsRows(stage));
        }
    };

    let mut touched = BTreeSet::new();
    touched.extend(left_raw.keys().cloned());
    touched.extend(right_raw.keys().cloned());
    touched.extend(sources.changed_keys());
    let (Some(left), Some(right)) = (join.left_for_cut(), join.right_for_cut()) else {
        unreachable!("source arrangements were initialized above");
    };
    let mut memo_updates = Vec::with_capacity(touched.len());
    for full_key in &touched {
        let next_left = sources.proposed_left(join, full_key);
        let next_right = sources.proposed_right(join, full_key);
        let group_key = cut.codec.encode(full_key)?;
        let contribution = match (next_left, next_right) {
            (None, None) => None,
            (left_stats, right_stats) => Some(Contribution::from_optional_source_stats(
                group_key.clone(),
                left_stats,
                right_stats,
                &cut.demands,
            )?),
        };
        memo_updates.push((full_key.clone(), contribution));
    }
    let Some(memo_stage) = reducer.stage_maintained_memo(memo_updates)? else {
        return Ok(CertifiedDecision::NeedsRows(None));
    };
    let mut key_bounds = Vec::<(Vec<u8>, KeyNumericBounds)>::with_capacity(touched.len());
    for full_key in touched {
        let group_key = cut.codec.encode(&full_key)?;
        let Some(bounds) = raw_bounds::derive_key_bounds(
            left.source_stats(&full_key),
            right.source_stats(&full_key),
            left_raw.get(&full_key),
            right_raw.get(&full_key),
            &cut.demands,
        ) else {
            return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
        };
        key_bounds.push((group_key, bounds));
    }
    let Some(group_bounds) = certificate::aggregate_group_bounds(key_bounds) else {
        return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
    };
    let Some(cut) = reducer.maintained_cut() else {
        unreachable!("maintained cut remains initialized");
    };
    let old_groups = reducer.retained_groups();
    let mut group_changes = Vec::with_capacity(group_bounds.len());
    for (key, bounds) in group_bounds {
        let old = old_groups.get(&key);
        let old_memo = cut.memo.group(&key);
        let proposed_memo = cut.memo.group_after_stage(&memo_stage, &key);
        if certificate::certify_group(old, old_memo, proposed_memo, &bounds, &bindings)
            != CertificateDecision::Certified
        {
            return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
        }
        let bounded_row_count = match proposed_rows(old, &bounds) {
            Some(count) => count,
            None => return Ok(CertifiedDecision::NeedsRows(Some(memo_stage))),
        };
        let row_metric =
            proposed_memo.and_then(|group| group.metrics.get(cut.row_cardinality_index));
        let row_count = match row_metric {
            Some(metric)
                if metric.eligibility
                    == crate::ops::join::factorized::group_memo::MetricLimit::Exact =>
            {
                to_i64(metric.subtotal)
            }
            None if bounded_row_count == 0 => Some(0),
            _ => None,
        };
        if row_count != Some(bounded_row_count) {
            return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
        }
        let row_count = bounded_row_count;
        if row_count == 0 {
            if old.is_some() {
                group_changes.push(GroupChange::Remove { key });
            }
            continue;
        }
        let Some(proposed_memo) = proposed_memo else {
            return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
        };
        let mut entry = match old {
            Some(entry) => entry.clone(),
            None => match reducer.empty_cut_entry() {
                Ok(entry) => entry,
                Err(_) => return Ok(CertifiedDecision::NeedsRows(Some(memo_stage))),
            },
        };
        if entry.values.len() != reducer.aggregates().len() {
            return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
        }
        entry.rows = row_count;
        for binding in &bindings {
            let Some(metric) = proposed_memo.metrics.get(binding.memo_index) else {
                return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
            };
            let value = match binding.kind {
                BoundKind::Count | BoundKind::NullableCount => {
                    let Some(count) = to_i64(metric.subtotal) else {
                        return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
                    };
                    AggValue::Count(count)
                }
                BoundKind::IntegerSum => {
                    let Some(nn_index) = binding.nn_memo_index else {
                        return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
                    };
                    let Some(nn) = proposed_memo
                        .metrics
                        .get(nn_index)
                        .and_then(|m| to_i64(m.subtotal))
                    else {
                        return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
                    };
                    let Some(sum) = to_i128(metric.subtotal) else {
                        return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
                    };
                    AggValue::SumInteger { sum, count: nn }
                }
                BoundKind::IntegerAvg => {
                    return Ok(CertifiedDecision::NeedsRows(Some(memo_stage)));
                }
            };
            entry.values[binding.aggregate_index] = value;
        }
        group_changes.push(GroupChange::Upsert { key, entry });
    }
    group_changes.sort_by(|a, b| group_change_key(a).cmp(group_change_key(b)));
    let consumed = match consume::consume_changes(
        &group_changes,
        old_groups,
        reducer
            .schema_for_cut()
            .ok_or_else(|| EngineError::Infrastructure("group schema missing".into()))?
            .clone(),
        reducer.group_key(),
        reducer.aggregates(),
    ) {
        Ok(consumed) => consumed,
        Err(_) => return Ok(CertifiedDecision::NeedsRows(Some(memo_stage))),
    };
    Ok(CertifiedDecision::Certified(PreparedCertifiedGroups {
        #[cfg(test)]
        sources: owned_sources.map(Box::new),
        memo: memo_stage,
        groups: consumed.changes,
        output: consumed.output,
    }))
}

fn stage_all_proposed_memo(
    join: &Join,
    reducer: &GroupAggregate,
    sources: &crate::ops::join::PreparedJoinSources,
) -> Result<Option<GroupMemoStage>, EngineError> {
    let Some(cut) = reducer.maintained_cut() else {
        return Ok(None);
    };
    let touched = sources.changed_keys();
    let mut updates = Vec::with_capacity(touched.len());
    for full_key in touched {
        let left = sources.proposed_left(join, &full_key);
        let right = sources.proposed_right(join, &full_key);
        let group_key = cut.codec.encode(&full_key)?;
        let contribution = match (left, right) {
            (None, None) => None,
            (left, right) => Some(Contribution::from_optional_source_stats(
                group_key,
                left,
                right,
                &cut.demands,
            )?),
        };
        updates.push((full_key, contribution));
    }
    reducer.stage_maintained_memo(updates)
}

fn projected_mapping_matches(
    mapping: &ColumnMapping,
    reducer: &GroupAggregate,
    schema: &SchemaRef,
    projection: &[usize],
    left_width: usize,
    right_width: usize,
) -> bool {
    let Some(join_width) = left_width.checked_add(right_width) else {
        return false;
    };
    if projection.iter().any(|&column| column >= join_width)
        || reducer.group_key().len() != mapping.group_key.len()
        || reducer.aggregates().len() != mapping.aggregate_inputs.len()
        || reducer.aggregates().len() != mapping.aggregate_input_types.len()
    {
        return false;
    }
    for (&output_index, &group_position) in reducer.group_key().iter().zip(&mapping.group_key) {
        let Some(expected_type) = mapping.join_key_types.get(group_position) else {
            return false;
        };
        if schema
            .fields()
            .get(output_index)
            .map(|field| field.data_type())
            != Some(expected_type)
        {
            return false;
        }
        let (Some(&left_key), Some(&right_key)) = (
            mapping.left_join_key.get(group_position),
            mapping.right_join_key.get(group_position),
        ) else {
            return false;
        };
        let Some(&projected_column) = projection.get(output_index) else {
            return false;
        };
        let Some(right_key) = left_width.checked_add(right_key) else {
            return false;
        };
        if projected_column != left_key && projected_column != right_key {
            return false;
        }
    }
    for ((aggregate, &source_input), source_type) in reducer
        .aggregates()
        .iter()
        .zip(&mapping.aggregate_inputs)
        .zip(&mapping.aggregate_input_types)
    {
        match (aggregate.input, source_input, source_type) {
            (None, None, None) => {}
            (Some(output_index), Some(source_index), Some(source_type)) => {
                if projection.get(output_index) != Some(&source_index)
                    || schema
                        .fields()
                        .get(output_index)
                        .map(|field| field.data_type())
                        != Some(source_type)
                {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

fn proposed_rows(old: Option<&GroupEntry>, bounds: &certificate::GroupPrefixBounds) -> Option<i64> {
    let old_rows = i256::from_i128(i128::from(old.map_or(0, |entry| entry.rows)));
    let rows = old_rows
        .checked_add(bounds.rows.positive)?
        .checked_sub(bounds.rows.negative)?;
    to_i64(rows).filter(|rows| *rows >= 0)
}

fn to_i128(value: i256) -> Option<i128> {
    value.to_string().parse().ok()
}

fn to_i64(value: i256) -> Option<i64> {
    i64::try_from(to_i128(value)?).ok()
}

fn group_change_key(change: &GroupChange) -> &[u8] {
    match change {
        GroupChange::Upsert { key, .. } | GroupChange::Remove { key } => key,
    }
}
