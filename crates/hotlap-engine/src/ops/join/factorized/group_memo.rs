//! Sparse per-full-join-key contributions to canonical grouped metrics.
//!
//! This metadata is deliberately separate from authoritative `GroupEntry`
//! rendering state. It records only current source-derived contributions.

use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::i256;

use super::group_metrics::{JoinMetricDemand, contribution_metrics};
use crate::arrange::source_stats::SourceStats;
use crate::error::EngineError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetricLimit {
    Exact,
    OverLimit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetricContribution {
    pub(crate) value: i256,
    pub(crate) abs_value: i256,
    pub(crate) limit: i256,
    pub(crate) over_limit: bool,
}

impl MetricContribution {
    #[cfg(test)]
    pub(crate) fn exact(value: i256, abs_value: i256) -> Self {
        Self::exact_with_limit(value, abs_value, i256::from_i128(i128::MAX))
    }

    pub(crate) fn exact_with_limit(value: i256, abs_value: i256, limit: i256) -> Self {
        Self {
            value,
            abs_value,
            limit,
            over_limit: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn over_limit(sign: i8) -> Self {
        Self::over_limit_with_limit(sign, i256::from_i128(i128::MAX))
    }

    fn over_limit_with_limit(sign: i8, limit: i256) -> Self {
        Self {
            value: i256::from_i128(i128::from(sign)),
            abs_value: i256::ZERO,
            limit,
            over_limit: true,
        }
    }

    pub(crate) fn bounded_product(
        left: i256,
        right: i256,
        limit: i256,
    ) -> Result<Self, EngineError> {
        if left == i256::ZERO || right == i256::ZERO {
            return Ok(Self::exact_with_limit(i256::ZERO, i256::ZERO, limit));
        }
        let left_abs = if left < i256::ZERO {
            i256::ZERO.checked_sub(left).ok_or_else(wide_error)?
        } else {
            left
        };
        let right_abs = if right < i256::ZERO {
            i256::ZERO.checked_sub(right).ok_or_else(wide_error)?
        } else {
            right
        };
        if left_abs > limit / right_abs {
            return Ok(Self::over_limit_with_limit(
                if (left < i256::ZERO) ^ (right < i256::ZERO) {
                    -1
                } else {
                    1
                },
                limit,
            ));
        }
        let value = left.checked_mul(right).ok_or_else(wide_error)?;
        Ok(Self::exact_with_limit(
            value,
            left_abs.checked_mul(right_abs).ok_or_else(wide_error)?,
            limit,
        ))
    }

    pub(crate) fn bounded_product_with_absolute(
        left: i256,
        right: i256,
        absolute: i256,
        limit: i256,
    ) -> Result<Self, EngineError> {
        let mut value = Self::bounded_product(left, right, limit)?;
        if absolute < i256::ZERO || absolute < value.abs_value {
            return Err(EngineError::Infrastructure(
                "source absolute metric bound is inconsistent".into(),
            ));
        }
        if absolute > limit {
            value =
                Self::over_limit_with_limit(if value.value < i256::ZERO { -1 } else { 1 }, limit);
        } else {
            value.abs_value = absolute;
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Contribution {
    group_key: Vec<u8>,
    metrics: Vec<MetricContribution>,
    has_negative_source: bool,
}

impl Contribution {
    #[cfg(test)]
    pub(crate) fn new(group_key: Vec<u8>, metrics: Vec<MetricContribution>) -> Self {
        Self {
            group_key,
            metrics,
            has_negative_source: false,
        }
    }

    /// Construct contribution metadata from the actual proposed canonical
    /// source summaries; callers cannot independently assert the sign flag.
    #[cfg(test)]
    pub(crate) fn from_source_stats(
        group_key: Vec<u8>,
        left: &SourceStats,
        right: &SourceStats,
        demands: &[JoinMetricDemand],
    ) -> Result<Self, EngineError> {
        Self::from_optional_source_stats(group_key, Some(left), Some(right), demands)
    }

    pub(crate) fn from_optional_source_stats(
        group_key: Vec<u8>,
        left: Option<&SourceStats>,
        right: Option<&SourceStats>,
        demands: &[JoinMetricDemand],
    ) -> Result<Self, EngineError> {
        Ok(Self {
            group_key,
            metrics: contribution_metrics(left, right, demands)?,
            has_negative_source: left.is_some_and(|stats| stats.negative_support != 0)
                || right.is_some_and(|stats| stats.negative_support != 0),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupMetric {
    pub(crate) subtotal: i256,
    pub(crate) abs_subtotal: i256,
    pub(crate) eligibility: MetricLimit,
    over_limit_keys: usize,
    pub(crate) limit: i256,
}

impl Default for GroupMetric {
    fn default() -> Self {
        Self {
            subtotal: i256::ZERO,
            abs_subtotal: i256::ZERO,
            eligibility: MetricLimit::Exact,
            over_limit_keys: 0,
            limit: i256::from_i128(i128::MAX),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupContribution {
    pub(crate) metrics: Vec<GroupMetric>,
    /// Number of retained full join keys in this requested group with a
    /// physically negative row on either source (independent of net weight).
    pub(crate) negative_source_keys: usize,
    physical_keys: usize,
}

/// Exact group totals fit i256 in the supported cardinality domain: fewer than
/// 2^64 physical full-join keys contribute to a group, each exact per-key
/// magnitude is at most T <= 2^127-1, and thus the signed subtotal and sum of
/// absolute contributions are < 2^191. COUNT's T is only i64::MAX. Updates
/// still use checked i256 arithmetic; a violated bound is internal corruption,
/// not a query-level numeric overflow.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GroupMemo {
    by_join_key: HashMap<Vec<u8>, Contribution>,
    by_group_key: HashMap<Vec<u8>, GroupContribution>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GroupMemoStage {
    groups: BTreeMap<Vec<u8>, Option<GroupContribution>>,
    join_keys: BTreeMap<Vec<u8>, Option<Contribution>>,
}

impl GroupMemo {
    pub(crate) fn groups(&self) -> impl Iterator<Item = (&[u8], &GroupContribution)> {
        self.by_group_key
            .iter()
            .map(|(key, group)| (key.as_slice(), group))
    }

    pub(crate) fn group_after_stage<'a>(
        &'a self,
        stage: &'a GroupMemoStage,
        key: &[u8],
    ) -> Option<&'a GroupContribution> {
        match stage.groups.get(key) {
            Some(group) => group.as_ref(),
            None => self.by_group_key.get(key),
        }
    }

    pub(crate) fn group(&self, key: &[u8]) -> Option<&GroupContribution> {
        self.by_group_key.get(key)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_join_key.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.by_join_key.is_empty()
    }

    #[allow(dead_code)] // Included by the graph-owned memory report in the consumer slice.
    pub(crate) fn memory_usage(&self) -> Result<(usize, usize), EngineError> {
        let mut payload = 0usize;
        let mut capacity = self
            .by_join_key
            .capacity()
            .checked_mul(std::mem::size_of::<(Vec<u8>, Contribution)>())
            .and_then(|bytes| {
                bytes.checked_add(
                    self.by_group_key
                        .capacity()
                        .checked_mul(std::mem::size_of::<(Vec<u8>, GroupContribution)>())?,
                )
            })
            .ok_or_else(wide_error)?;
        for (key, contribution) in &self.by_join_key {
            payload = add_bytes(payload, key.len())?;
            capacity = add_bytes(capacity, key.capacity())?;
            payload = add_bytes(payload, contribution.group_key.len())?;
            capacity = add_bytes(capacity, contribution.group_key.capacity())?;
            capacity = add_bytes(
                capacity,
                contribution
                    .metrics
                    .capacity()
                    .checked_mul(std::mem::size_of::<MetricContribution>())
                    .ok_or_else(wide_error)?,
            )?;
        }
        for (key, group) in &self.by_group_key {
            payload = add_bytes(payload, key.len())?;
            capacity = add_bytes(capacity, key.capacity())?;
            capacity = add_bytes(
                capacity,
                group
                    .metrics
                    .capacity()
                    .checked_mul(std::mem::size_of::<GroupMetric>())
                    .ok_or_else(wide_error)?,
            )?;
        }
        Ok((payload, capacity))
    }

    pub(crate) fn replace(
        &mut self,
        join_key: Vec<u8>,
        next: Contribution,
    ) -> Result<(), EngineError> {
        let stage = self.stage_updates([(join_key, Some(next))])?;
        self.commit(stage);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn remove(&mut self, join_key: &[u8]) -> Result<(), EngineError> {
        let stage = self.stage_updates([(join_key.to_vec(), None)])?;
        self.commit(stage);
        Ok(())
    }

    /// Prepare a sparse transaction touching only supplied full join keys and
    /// their old/new canonical groups. No source map is cloned or published.
    pub(crate) fn stage_updates(
        &self,
        updates: impl IntoIterator<Item = (Vec<u8>, Option<Contribution>)>,
    ) -> Result<GroupMemoStage, EngineError> {
        let mut stage = GroupMemoStage::default();
        for (join_key, next) in updates {
            if stage.join_keys.contains_key(&join_key) {
                return Err(EngineError::Unsupported(
                    "duplicate full join key in group memo stage".into(),
                ));
            }
            let old = self.by_join_key.get(&join_key).cloned();
            if let (Some(old), Some(next)) = (&old, &next)
                && old.metrics.len() != next.metrics.len()
            {
                return Err(EngineError::Unsupported(
                    "group metric shape changed".into(),
                ));
            }
            if let Some(old) = old {
                let group = staged_group(self, &mut stage.groups, &old.group_key)?;
                apply(group, &old, false)?;
            }
            if let Some(next) = &next {
                let group = staged_group(self, &mut stage.groups, &next.group_key)?;
                apply(group, next, true)?;
            }
            stage.join_keys.insert(join_key, next);
        }
        Ok(stage)
    }

    pub(crate) fn commit(&mut self, stage: GroupMemoStage) {
        for (key, group) in stage.groups {
            if let Some(group) = group.filter(|group| group.physical_keys != 0) {
                self.by_group_key.insert(key, group);
            } else {
                self.by_group_key.remove(&key);
            }
        }
        for (key, contribution) in stage.join_keys {
            if let Some(contribution) = contribution {
                self.by_join_key.insert(key, contribution);
            } else {
                self.by_join_key.remove(&key);
            }
        }
    }
}

fn staged_group<'a>(
    memo: &'a GroupMemo,
    changed: &'a mut BTreeMap<Vec<u8>, Option<GroupContribution>>,
    key: &[u8],
) -> Result<&'a mut GroupContribution, EngineError> {
    let slot = changed
        .entry(key.to_vec())
        .or_insert_with(|| memo.by_group_key.get(key).cloned());
    if slot.is_none() {
        *slot = Some(GroupContribution {
            metrics: Vec::new(),
            negative_source_keys: 0,
            physical_keys: 0,
        });
    }
    Ok(slot.as_mut().expect("group slot initialized"))
}

fn apply(
    group: &mut GroupContribution,
    contribution: &Contribution,
    add: bool,
) -> Result<(), EngineError> {
    let metrics = &contribution.metrics;
    if group.metrics.is_empty() {
        group
            .metrics
            .resize_with(metrics.len(), GroupMetric::default);
    }
    if group.metrics.len() != metrics.len() {
        return Err(EngineError::Unsupported(
            "group metric shape changed".into(),
        ));
    }
    for (state, contribution) in group.metrics.iter_mut().zip(metrics) {
        if state.limit != contribution.limit && group.physical_keys != 0 {
            return Err(EngineError::Unsupported(
                "group metric limit changed".into(),
            ));
        }
        state.limit = contribution.limit;
        if contribution.over_limit {
            state.over_limit_keys = if add {
                state.over_limit_keys.checked_add(1)
            } else {
                state.over_limit_keys.checked_sub(1)
            }
            .ok_or_else(|| {
                EngineError::Infrastructure("group over-limit support underflow/overflow".into())
            })?;
        } else {
            let sign = if add { i256::ONE } else { -i256::ONE };
            state.subtotal = state
                .subtotal
                .checked_add(
                    contribution
                        .value
                        .checked_mul(sign)
                        .ok_or_else(wide_error)?,
                )
                .ok_or_else(wide_error)?;
            state.abs_subtotal = state
                .abs_subtotal
                .checked_add(
                    contribution
                        .abs_value
                        .checked_mul(sign)
                        .ok_or_else(wide_error)?,
                )
                .ok_or_else(wide_error)?;
            if state.abs_subtotal < i256::ZERO {
                return Err(EngineError::Infrastructure(
                    "group absolute subtotal underflow".into(),
                ));
            }
        }
        state.eligibility = if state.over_limit_keys != 0
            || state.subtotal > state.limit
            || state.subtotal < -state.limit
            || state.abs_subtotal > state.limit
        {
            MetricLimit::OverLimit
        } else {
            MetricLimit::Exact
        };
    }
    if contribution.has_negative_source {
        group.negative_source_keys = if add {
            group.negative_source_keys.checked_add(1)
        } else {
            group.negative_source_keys.checked_sub(1)
        }
        .ok_or_else(|| {
            EngineError::Infrastructure("group negative-source support underflow/overflow".into())
        })?;
    }
    group.physical_keys = if add {
        group.physical_keys.checked_add(1)
    } else {
        group.physical_keys.checked_sub(1)
    }
    .ok_or_else(|| {
        EngineError::Infrastructure("group physical support underflow/overflow".into())
    })?;
    Ok(())
}

fn wide_error() -> EngineError {
    EngineError::Infrastructure("group memo i256 invariant overflow".into())
}

fn add_bytes(total: usize, bytes: usize) -> Result<usize, EngineError> {
    total.checked_add(bytes).ok_or_else(wide_error)
}
