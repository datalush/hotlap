use std::collections::HashMap;

use arrow::datatypes::i256;

#[path = "maintained_factorized_state/join.rs"]
mod join;

pub(crate) type RowId = (i64, i64);

#[path = "maintained_factorized_state/operator_bench.rs"]
mod operator_bench;

fn magnitude(value: i256) -> i256 {
    if value < i256::ZERO {
        i256::ZERO - value
    } else {
        value
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Summary {
    weight: i128,
    abs_weight: u128,
    nonnull: i128,
    abs_nonnull: u128,
    negative_rows: i128,
    sum: i256,
    abs_sum: i256,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Relation {
    rows: HashMap<RowId, Row>,
    by_key: HashMap<i64, Summary>,
    key_support: HashMap<i64, usize>,
    work: Work,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Work {
    canonical_row_reads: usize,
    row_overlay_reads: usize,
    row_stages: usize,
    summary_copies: usize,
    affected_summary_keys: usize,
}

#[derive(Default)]
struct Overlay {
    rows: HashMap<RowId, Option<Row>>,
    summaries: HashMap<i64, Summary>,
    support: HashMap<i64, u64>,
    negative_prefix: bool,
    work: Work,
}

impl Overlay {
    fn row(&mut self, canonical: &HashMap<RowId, Row>, id: RowId) -> Option<Row> {
        if self.rows.contains_key(&id) {
            self.work.row_overlay_reads += 1;
            return self.rows.get(&id).cloned().flatten();
        }
        self.work.canonical_row_reads += 1;
        canonical.get(&id).cloned()
    }

    fn summary(
        &mut self,
        canonical: &HashMap<i64, Summary>,
        canonical_support: &HashMap<i64, usize>,
        key: i64,
    ) -> &mut Summary {
        if !self.summaries.contains_key(&key) {
            self.work.summary_copies += 1;
            let summary = canonical.get(&key).cloned().unwrap_or_default();
            self.support.insert(
                key,
                canonical_support.get(&key).copied().unwrap_or_default() as u64,
            );
            self.summaries.insert(key, summary);
        }
        self.summaries.get_mut(&key).expect("inserted summary")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    value: Option<i64>,
    weight: i64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Change {
    pub(crate) id: RowId,
    pub(crate) value: Option<i64>,
    pub(crate) delta: i64,
}

impl Relation {
    fn prepare(&self, changes: &[Change]) -> Result<Overlay, &'static str> {
        let mut staged = Overlay::default();

        for change in changes {
            staged.work.row_stages += 1;
            let old = staged.row(&self.rows, change.id);
            let old_weight = old.as_ref().map_or(0, |row| row.weight);
            if old.as_ref().is_some_and(|row| row.value != change.value) {
                return Err("row identity/value mismatch");
            }
            let new_weight = old_weight
                .checked_add(change.delta)
                .ok_or("row weight overflow")?;

            let key = change.id.0;
            {
                staged.summary(&self.by_key, &self.key_support, key);
            }
            let support_new_weight = old_weight
                .checked_add(change.delta)
                .ok_or("row weight overflow")?;
            let support = staged.support.get_mut(&key);
            let support = match support {
                Some(support) => support,
                None => return Err("summary support missing"),
            };
            if old_weight == 0 && support_new_weight != 0 {
                *support = support.checked_add(1).ok_or("support overflow")?;
            } else if old_weight != 0 && support_new_weight == 0 {
                *support = support.checked_sub(1).ok_or("support underflow")?;
            }

            let summary = staged.summaries.get_mut(&key).expect("summary staged");
            summary.weight = summary
                .weight
                .checked_add(i128::from(change.delta))
                .ok_or("summary weight overflow")?;
            summary.abs_weight = summary
                .abs_weight
                .checked_sub(u128::from(old_weight.unsigned_abs()))
                .and_then(|n| n.checked_add(u128::from(new_weight.unsigned_abs())))
                .ok_or("summary absolute weight overflow")?;

            if let Some(value) = change.value {
                summary.nonnull = summary
                    .nonnull
                    .checked_add(i128::from(change.delta))
                    .ok_or("summary nonnull overflow")?;
                summary.abs_nonnull = summary
                    .abs_nonnull
                    .checked_sub(u128::from(old_weight.unsigned_abs()))
                    .and_then(|n| n.checked_add(u128::from(new_weight.unsigned_abs())))
                    .ok_or("summary abs nonnull overflow")?;
                let old_sum = i256::from_i128(i128::from(value))
                    .checked_mul(i256::from_i128(i128::from(old_weight)))
                    .ok_or("old contribution overflow")?;
                let new_sum = i256::from_i128(i128::from(value))
                    .checked_mul(i256::from_i128(i128::from(new_weight)))
                    .ok_or("new contribution overflow")?;
                summary.sum = summary
                    .sum
                    .checked_sub(old_sum)
                    .and_then(|n| n.checked_add(new_sum))
                    .ok_or("summary sum overflow")?;
                let old_abs = magnitude(old_sum);
                let new_abs = magnitude(new_sum);
                summary.abs_sum = summary
                    .abs_sum
                    .checked_sub(old_abs)
                    .and_then(|n| n.checked_add(new_abs))
                    .ok_or("summary abs sum overflow")?;
            }

            summary.negative_rows = summary
                .negative_rows
                .checked_add(i128::from(i8::from(new_weight < 0)))
                .and_then(|n| n.checked_sub(i128::from(i8::from(old_weight < 0))))
                .ok_or("negative row count overflow")?;
            if new_weight == 0 {
                staged.rows.insert(change.id, None);
            } else {
                let next = Row {
                    value: change.value,
                    weight: new_weight,
                };
                staged.rows.insert(change.id, Some(next));
            }
        }

        staged.work.affected_summary_keys = staged.summaries.len();

        Ok(staged)
    }

    fn commit(&mut self, staged: Overlay) {
        for (id, row) in staged.rows {
            if let Some(row) = row {
                self.rows.insert(id, row);
            } else {
                self.rows.remove(&id);
            }
        }
        for (key, summary) in staged.summaries {
            if staged.support[&key] == 0 {
                self.by_key.remove(&key);
                self.key_support.remove(&key);
            } else {
                self.by_key.insert(key, summary);
                self.key_support.insert(key, staged.support[&key] as usize);
            }
        }
        self.work = staged.work;
    }

    fn apply(&mut self, changes: &[Change]) -> Result<(), &'static str> {
        let staged = self.prepare(changes)?;
        self.commit(staged);
        Ok(())
    }

    fn rebuild(rows: &HashMap<RowId, Row>) -> Self {
        let mut rebuilt = Self::default();
        for (&id, row) in rows {
            rebuilt.work.canonical_row_reads += 1;
            rebuilt.rows.insert(id, row.clone());
            let summary = rebuilt.by_key.entry(id.0).or_default();
            *rebuilt.key_support.entry(id.0).or_default() += 1;
            summary.weight += i128::from(row.weight);
            summary.abs_weight += u128::from(row.weight.unsigned_abs());
            summary.negative_rows += i128::from(i8::from(row.weight < 0));
            if let Some(value) = row.value {
                summary.nonnull += i128::from(row.weight);
                summary.abs_nonnull += u128::from(row.weight.unsigned_abs());
                let contribution = i256::from_i128(i128::from(value))
                    .checked_mul(i256::from_i128(i128::from(row.weight)))
                    .unwrap();
                summary.sum += contribution;
                summary.abs_sum += magnitude(contribution);
            }
        }
        rebuilt.by_key.retain(|_, summary| summary.abs_weight != 0);
        rebuilt.key_support.retain(|_, support| *support != 0);
        rebuilt
    }
}

impl Relation {
    fn logical_snapshot(
        &self,
    ) -> (
        HashMap<RowId, Row>,
        HashMap<i64, Summary>,
        HashMap<i64, usize>,
    ) {
        (
            self.rows.clone(),
            self.by_key.clone(),
            self.key_support.clone(),
        )
    }
}

fn full_recompute(rows: &HashMap<RowId, Row>) -> HashMap<i64, Summary> {
    let mut summaries = HashMap::<i64, Summary>::new();
    for (&(key, _), row) in rows {
        let summary = summaries.entry(key).or_default();
        summary.weight += i128::from(row.weight);
        summary.abs_weight += u128::from(row.weight.unsigned_abs());
        summary.negative_rows += i128::from(i8::from(row.weight < 0));
        if let Some(value) = row.value {
            summary.nonnull += i128::from(row.weight);
            summary.abs_nonnull += u128::from(row.weight.unsigned_abs());
            let term = i256::from_i128(i128::from(value))
                .checked_mul(i256::from_i128(i128::from(row.weight)))
                .unwrap();
            summary.sum += term;
            summary.abs_sum += magnitude(term);
        }
    }
    summaries.retain(|_, summary| summary.abs_weight != 0);
    summaries
}

#[derive(Default)]
struct Reference {
    rows: HashMap<RowId, Row>,
}

impl Reference {
    fn apply(&mut self, changes: &[Change]) -> Result<(), &'static str> {
        let mut next = self.rows.clone();
        for change in changes {
            let old = next.get(&change.id);
            if old.is_some_and(|row| row.value != change.value) {
                return Err("row identity/value mismatch");
            }
            let weight = old.map_or(0, |row| row.weight);
            let weight = weight
                .checked_add(change.delta)
                .ok_or("row weight overflow")?;
            if weight == 0 {
                next.remove(&change.id);
            } else {
                next.insert(
                    change.id,
                    Row {
                        value: change.value,
                        weight,
                    },
                );
            }
        }
        self.rows = next;
        Ok(())
    }

    fn summaries(&self) -> HashMap<i64, Summary> {
        let mut result = HashMap::<i64, Summary>::new();
        for (&(key, _payload), row) in &self.rows {
            let summary = result.entry(key).or_default();
            summary.weight += i128::from(row.weight);
            summary.abs_weight += u128::from(row.weight.unsigned_abs());
            summary.negative_rows += i128::from(i8::from(row.weight < 0));
            if let Some(value) = row.value {
                summary.nonnull += i128::from(row.weight);
                summary.abs_nonnull += u128::from(row.weight.unsigned_abs());
                let magnitude =
                    u128::from(value.unsigned_abs()) * u128::from(row.weight.unsigned_abs());
                let term =
                    i256::from_i128(i128::from(value)) * i256::from_i128(i128::from(row.weight));
                summary.sum += term;
                summary.abs_sum += i256::from_i128(magnitude as i128);
            }
        }
        result.retain(|_, summary| summary.abs_weight != 0);
        result
    }
}

#[path = "maintained_factorized_state/source_tests.rs"]
mod source_tests;

#[path = "maintained_factorized_state/numeric_tests.rs"]
mod numeric_tests;
