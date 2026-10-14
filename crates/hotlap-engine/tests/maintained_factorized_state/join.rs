use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{Change, Overlay, Relation, Summary};
use arrow::datatypes::i256;

#[path = "join/numeric.rs"]
mod numeric;
#[path = "join/numeric_tests.rs"]
mod numeric_tests;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Contribution {
    pub(super) count: i256,
    pub(super) left_nonnull: i256,
    pub(super) left_sum: i256,
    pub(super) right_nonnull: i256,
    pub(super) right_sum: i256,
}

pub(super) struct JoinView {
    left: Relation,
    right: Relation,
    group_for_join_key: HashMap<i64, i64>,
    by_join_key: HashMap<i64, Contribution>,
    pub(super) by_group: HashMap<i64, Contribution>,
    work: JoinWork,
    pub(super) gate: JoinGate,
    memo_valid: bool,
}

pub(super) fn set_group_mapping(mapping: impl IntoIterator<Item = (i64, i64)>) {
    // Benchmark fixtures use deterministic group ids derived from their join key.
    BENCH_GROUPS.with(|groups| groups.borrow_mut().extend(mapping));
}

thread_local! {
    static BENCH_GROUPS: std::cell::RefCell<HashMap<i64, i64>> = std::cell::RefCell::new(HashMap::new());
}

impl Default for JoinView {
    fn default() -> Self {
        Self {
            left: Relation::default(),
            right: Relation::default(),
            group_for_join_key: HashMap::new(),
            by_join_key: HashMap::new(),
            by_group: HashMap::new(),
            work: JoinWork::default(),
            gate: JoinGate::NeedsRowEvaluation,
            memo_valid: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum JoinGate {
    #[default]
    NeedsRowEvaluation,
    Certified,
    WideBoundsUnavailable,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct JoinWork {
    changed_join_keys: usize,
    memo_lookups: usize,
    summary_lookups: usize,
    group_total_copies: usize,
    contribution_replacements: usize,
    group_mapping_reads: usize,
}

impl JoinView {
    fn update(
        &mut self,
        left_changes: &[Change],
        right_changes: &[Change],
    ) -> Result<(), &'static str> {
        let left_overlay = self.left.prepare(left_changes)?;
        let right_overlay = self.right.prepare(right_changes)?;
        let touched: HashSet<_> = left_changes
            .iter()
            .map(|change| change.id.0)
            .chain(right_changes.iter().map(|change| change.id.0))
            .collect();
        let mut changes = Vec::new();
        let mut next_work = JoinWork {
            changed_join_keys: touched.len(),
            group_mapping_reads: touched.len(),
            ..JoinWork::default()
        };
        for key in &touched {
            BENCH_GROUPS.with(|groups| {
                if let Some(group) = groups.borrow().get(key).copied() {
                    self.group_for_join_key.insert(*key, group);
                }
            });
        }
        if !self.memo_valid {
            self.left.commit(left_overlay);
            self.right.commit(right_overlay);
            self.gate = JoinGate::WideBoundsUnavailable;
            return Ok(());
        }
        let gate = numeric::classify(
            self,
            left_changes,
            right_changes,
            &left_overlay,
            &right_overlay,
            &touched,
        );
        if gate == JoinGate::WideBoundsUnavailable {
            self.left.commit(left_overlay);
            self.right.commit(right_overlay);
            self.memo_valid = false;
            self.gate = gate;
            return Ok(());
        }
        let mut next_memo = HashMap::new();
        let mut next_groups = HashMap::new();

        for join_key in touched {
            let group = *self.group_for_join_key.get(&join_key).unwrap_or(&join_key);
            let old = self.by_join_key.get(&join_key).cloned();
            next_work.memo_lookups += 1;
            let left = updated_summary(&self.left, &left_overlay, join_key);
            let right = updated_summary(&self.right, &right_overlay, join_key);
            next_work.summary_lookups += 2;
            let new = match (left, right) {
                (Some(left), Some(right)) => Some(contribution(left, right)?),
                _ => None,
            };
            if let Some(old) = old.as_ref() {
                if let std::collections::hash_map::Entry::Vacant(entry) = next_groups.entry(group) {
                    next_work.group_total_copies += 1;
                    entry.insert(self.by_group.get(&group).cloned().unwrap_or_default());
                }
                let total = next_groups.get_mut(&group).unwrap();
                subtract_contribution(total, old)?;
            }
            if let Some(new) = new.as_ref() {
                if let std::collections::hash_map::Entry::Vacant(entry) = next_groups.entry(group) {
                    next_work.group_total_copies += 1;
                    entry.insert(self.by_group.get(&group).cloned().unwrap_or_default());
                }
                let total = next_groups.get_mut(&group).unwrap();
                add_contribution(total, new)?;
                next_memo.insert(join_key, new.clone());
            }
            changes.push((join_key, group, old, new));
        }

        // Both relations were fully staged and all changed-key contributions checked
        // before either side or either memo is published.
        self.left.commit(left_overlay);
        self.right.commit(right_overlay);
        for (join_key, _group, _old, new) in changes {
            if new.is_none() {
                self.by_join_key.remove(&join_key);
            }
            next_work.contribution_replacements += 1;
        }
        for (join_key, contribution) in next_memo {
            self.by_join_key.insert(join_key, contribution);
        }
        for (group, contribution) in next_groups {
            if contribution.is_zero() {
                self.by_group.remove(&group);
            } else {
                self.by_group.insert(group, contribution);
            }
        }
        self.work = next_work;
        self.gate = gate;
        Ok(())
    }

    pub(super) fn update_batch(
        &mut self,
        left: &hotlap_engine::ZSetBatch,
        right: &hotlap_engine::ZSetBatch,
    ) -> Result<(), &'static str> {
        let left_changes = decode(left)?;
        let right_changes = decode(right)?;
        self.update(&left_changes, &right_changes)
    }
}

fn decode(batch: &hotlap_engine::ZSetBatch) -> Result<Vec<Change>, &'static str> {
    use arrow::array::{Array, Int64Array};
    let keys = batch
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("key must be i64")?;
    let value_column = if batch.batch.num_columns() > 3 { 3 } else { 1 };
    let values = batch
        .batch
        .column(value_column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("value must be i64")?;
    let diffs = batch
        .diff()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("diff must be i64")?;
    let groups = if batch.batch.num_columns() > 2 {
        Some(
            batch
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("group must be i64")?,
        )
    } else {
        None
    };
    let payload_identity = if batch.batch.num_columns() > 3 {
        Some(
            batch
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("payload must be i64")?,
        )
    } else {
        None
    };
    Ok((0..batch.len())
        .map(|row| Change {
            id: (
                keys.value(row),
                payload_identity.map_or_else(
                    || groups.map_or(row as i64, |group| group.value(row)),
                    |payload| payload.value(row),
                ),
            ),
            value: (!values.is_null(row)).then(|| values.value(row)),
            delta: diffs.value(row),
        })
        .collect())
}

fn updated_summary<'a>(
    relation: &'a Relation,
    overlay: &'a Overlay,
    key: i64,
) -> Option<&'a Summary> {
    if let Some(summary) = overlay.summaries.get(&key) {
        return (overlay.support.get(&key).copied().unwrap_or_default() != 0).then_some(summary);
    }
    relation.by_key.get(&key)
}

fn contribution(left: &Summary, right: &Summary) -> Result<Contribution, &'static str> {
    let l_weight = i256::from_i128(left.weight);
    let r_weight = i256::from_i128(right.weight);
    Ok(Contribution {
        count: l_weight
            .checked_mul(r_weight)
            .ok_or("count contribution overflow")?,
        left_nonnull: i256::from_i128(left.nonnull)
            .checked_mul(r_weight)
            .ok_or("left count contribution overflow")?,
        left_sum: left
            .sum
            .checked_mul(r_weight)
            .ok_or("left sum contribution overflow")?,
        right_nonnull: i256::from_i128(right.nonnull)
            .checked_mul(l_weight)
            .ok_or("right count contribution overflow")?,
        right_sum: right
            .sum
            .checked_mul(l_weight)
            .ok_or("right sum contribution overflow")?,
    })
}

fn add_contribution(target: &mut Contribution, value: &Contribution) -> Result<(), &'static str> {
    target.count = target
        .count
        .checked_add(value.count)
        .ok_or("group count overflow")?;
    target.left_nonnull = target
        .left_nonnull
        .checked_add(value.left_nonnull)
        .ok_or("group left count overflow")?;
    target.left_sum = target
        .left_sum
        .checked_add(value.left_sum)
        .ok_or("group left sum overflow")?;
    target.right_nonnull = target
        .right_nonnull
        .checked_add(value.right_nonnull)
        .ok_or("group right count overflow")?;
    target.right_sum = target
        .right_sum
        .checked_add(value.right_sum)
        .ok_or("group right sum overflow")?;
    Ok(())
}

fn subtract_contribution(
    target: &mut Contribution,
    value: &Contribution,
) -> Result<(), &'static str> {
    target.count = target
        .count
        .checked_sub(value.count)
        .ok_or("group count underflow")?;
    target.left_nonnull = target
        .left_nonnull
        .checked_sub(value.left_nonnull)
        .ok_or("group left count underflow")?;
    target.left_sum = target
        .left_sum
        .checked_sub(value.left_sum)
        .ok_or("group left sum underflow")?;
    target.right_nonnull = target
        .right_nonnull
        .checked_sub(value.right_nonnull)
        .ok_or("group right count underflow")?;
    target.right_sum = target
        .right_sum
        .checked_sub(value.right_sum)
        .ok_or("group right sum underflow")?;
    Ok(())
}

impl Contribution {
    fn is_zero(&self) -> bool {
        self.count == i256::ZERO
            && self.left_nonnull == i256::ZERO
            && self.left_sum == i256::ZERO
            && self.right_nonnull == i256::ZERO
            && self.right_sum == i256::ZERO
    }
}

#[path = "join/tests.rs"]
mod tests;
