use std::collections::HashMap;

use arrow::row::OwnedRow;

use crate::error::EngineError;

pub(super) enum Bucket {
    One(Option<Box<OwnedRow>>, i64),
    Many(HashMap<OwnedRow, i64>),
}

pub(super) enum EitherRows<'a> {
    One(std::iter::Once<(&'a OwnedRow, &'a i64)>),
    Many(std::collections::hash_map::Iter<'a, OwnedRow, i64>),
}

impl<'a> Iterator for EitherRows<'a> {
    type Item = (&'a OwnedRow, &'a i64);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(rows) => rows.next(),
            Self::Many(rows) => rows.next(),
        }
    }
}

/// Byte-keyed `(key, payload) -> diff` state that collapses zero-sum entries.
#[derive(Default)]
pub struct RowState {
    entries: HashMap<Vec<u8>, Bucket>,
}

#[cfg(test)]
impl RowState {
    #[cfg(test)]
    pub(super) fn outer_capacity_for_test(&self) -> usize {
        self.entries.capacity()
    }

    #[cfg(test)]
    pub(super) fn memory_usage_for_test(&self) -> (usize, usize, usize) {
        self.memory_usage().unwrap()
    }

    #[cfg(test)]
    pub(super) fn many_capacity(&self, key: &[u8]) -> Option<usize> {
        match self.entries.get(key)? {
            Bucket::One(None, _) => unreachable!("singleton row temporarily moved"),
            Bucket::One(Some(_), _) => None,
            Bucket::Many(rows) => Some(rows.capacity()),
        }
    }

    pub(super) fn bucket_kind(&self, key: &[u8]) -> Option<&'static str> {
        self.entries.get(key).map(|bucket| match bucket {
            Bucket::One(None, _) => unreachable!("singleton row temporarily moved"),
            Bucket::One(Some(_), _) => "one",
            Bucket::Many(_) => "many",
        })
    }
}

impl RowState {
    pub(super) fn has_storage(&self) -> bool {
        self.entries.capacity() > 0
    }

    pub(super) fn memory_usage(&self) -> Result<(usize, usize, usize), EngineError> {
        let mut payload = 0usize;
        let mut key_capacity = 0usize;
        let mut structural = self
            .entries
            .capacity()
            .checked_mul(std::mem::size_of::<(Vec<u8>, Bucket)>())
            .ok_or_else(|| EngineError::Infrastructure("join estimate overflow".into()))?;
        for (key, bucket) in &self.entries {
            payload = checked_add(payload, key.len())?;
            key_capacity = checked_add(key_capacity, key.capacity())?;
            match bucket {
                Bucket::One(Some(row), _) => {
                    payload = checked_add(payload, row.as_ref().as_ref().len())?;
                    structural = checked_add(structural, std::mem::size_of::<OwnedRow>())?;
                }
                Bucket::One(None, _) => unreachable!("singleton row temporarily moved"),
                Bucket::Many(payloads) => {
                    structural = checked_add(
                        structural,
                        payloads
                            .capacity()
                            .checked_mul(std::mem::size_of::<(OwnedRow, i64)>())
                            .ok_or_else(|| {
                                EngineError::Infrastructure("join estimate overflow".into())
                            })?,
                    )?;
                    for row in payloads.keys() {
                        payload = checked_add(payload, row.as_ref().len())?;
                    }
                }
            }
        }
        Ok((payload, key_capacity, structural))
    }

    /// Adds `diff` to one entry, forgetting entries whose sum becomes zero.
    pub fn add(&mut self, key: OwnedRow, payload: OwnedRow, diff: i64) -> Result<(), EngineError> {
        if diff == 0 {
            return Ok(());
        }
        let key_bytes = key.as_ref();
        if let Some(bucket) = self.entries.get_mut(key_bytes) {
            match bucket {
                Bucket::One(Some(row), sum) if **row == payload => {
                    *sum = sum.checked_add(diff).ok_or(EngineError::NumericOverflow(
                        "arrangement multiplicity addition",
                    ))?;
                    if *sum == 0 {
                        self.entries.remove(key_bytes);
                    }
                }
                Bucket::One(Some(_), _) => {
                    let Bucket::One(old_row, weight) = bucket else {
                        unreachable!("bucket shape matched")
                    };
                    let mut rows = HashMap::with_capacity(2);
                    let old_row = old_row.take().expect("singleton row present");
                    rows.insert(*old_row, *weight);
                    rows.insert(payload, diff);
                    *bucket = Bucket::Many(rows);
                }
                Bucket::One(None, _) => unreachable!("singleton row temporarily moved"),
                Bucket::Many(payloads) => {
                    if let Some(sum) = payloads.get_mut(&payload) {
                        *sum = sum.checked_add(diff).ok_or(EngineError::NumericOverflow(
                            "arrangement multiplicity addition",
                        ))?;
                        if *sum == 0 {
                            payloads.remove(&payload);
                        }
                    } else {
                        payloads.insert(payload, diff);
                    }
                    if payloads.len() == 1 {
                        let (row, weight) = payloads.drain().next().expect("one remaining row");
                        *bucket = Bucket::One(Some(Box::new(row)), weight);
                    } else if payloads.is_empty() {
                        self.entries.remove(key_bytes);
                    }
                }
            }
        } else {
            self.entries.insert(
                key_bytes.to_vec(),
                Bucket::One(Some(Box::new(payload)), diff),
            );
        }
        Ok(())
    }

    /// Sets one entry to its prevalidated final weight, pruning zero weights.
    pub(super) fn set_weight(&mut self, key: OwnedRow, payload: OwnedRow, diff: i64) {
        let key_bytes = key.as_ref();
        if diff == 0 {
            if let Some(bucket) = self.entries.get_mut(key_bytes) {
                match bucket {
                    Bucket::One(Some(row), _) if **row == payload => {
                        self.entries.remove(key_bytes);
                    }
                    Bucket::One(None, _) => unreachable!("singleton row temporarily moved"),
                    Bucket::One(Some(_), _) => {}
                    Bucket::Many(payloads) => {
                        payloads.remove(&payload);
                        if payloads.len() == 1 {
                            let (row, weight) = payloads.drain().next().expect("one remaining row");
                            *bucket = Bucket::One(Some(Box::new(row)), weight);
                        } else if payloads.is_empty() {
                            self.entries.remove(key_bytes);
                        }
                    }
                }
            }
        } else if let Some(bucket) = self.entries.get_mut(key_bytes) {
            match bucket {
                Bucket::One(Some(row), weight) if **row == payload => *weight = diff,
                Bucket::One(None, _) => unreachable!("singleton row temporarily moved"),
                Bucket::One(row, weight) => {
                    let mut rows = HashMap::with_capacity(2);
                    let old_row = row.take().expect("singleton row present");
                    rows.insert(*old_row, *weight);
                    rows.insert(payload, diff);
                    *bucket = Bucket::Many(rows);
                }
                Bucket::Many(payloads) => {
                    payloads.insert(payload, diff);
                }
            }
        } else {
            self.entries.insert(
                key_bytes.to_vec(),
                Bucket::One(Some(Box::new(payload)), diff),
            );
        }
    }

    /// Returns the payload multiset for `key`, if the key is present.
    pub(super) fn get(&self, key: &[u8], payload: &OwnedRow) -> Option<&i64> {
        match self.entries.get(key)? {
            Bucket::One(Some(row), weight) if **row == *payload => Some(weight),
            Bucket::One(None, _) => unreachable!("singleton row temporarily moved"),
            Bucket::One(Some(_), _) => None,
            Bucket::Many(rows) => rows.get(payload),
        }
    }

    pub(super) fn rows(&self, key: &[u8]) -> Option<EitherRows<'_>> {
        self.entries.get(key).map(|bucket| match bucket {
            Bucket::One(Some(row), weight) => EitherRows::One(std::iter::once((row, weight))),
            Bucket::One(None, _) => unreachable!("singleton row temporarily moved"),
            Bucket::Many(rows) => EitherRows::Many(rows.iter()),
        })
    }

    /// Number of surviving `(key, payload)` entries.
    pub(super) fn len(&self) -> usize {
        self.entries
            .values()
            .map(|bucket| match bucket {
                Bucket::One(Some(_), _) => 1,
                Bucket::One(None, _) => 0,
                Bucket::Many(rows) => rows.len(),
            })
            .sum()
    }

    /// Returns `true` when no entry survives.
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns entries as `(key, payload, diff)`, ordered by key then payload.
    pub fn sorted(&self) -> Vec<(&Vec<u8>, &OwnedRow, i64)> {
        let mut entries: Vec<(&Vec<u8>, &OwnedRow, i64)> = Vec::new();
        for (key, bucket) in &self.entries {
            match bucket {
                Bucket::One(Some(row), diff) => entries.push((key, row, *diff)),
                Bucket::One(None, _) => unreachable!("singleton row temporarily moved"),
                Bucket::Many(payloads) => {
                    for (payload, &diff) in payloads {
                        entries.push((key, payload, diff));
                    }
                }
            }
        }
        entries.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
        entries
    }
}

fn checked_add(left: usize, right: usize) -> Result<usize, EngineError> {
    left.checked_add(right)
        .ok_or_else(|| EngineError::Infrastructure("join memory report overflow".into()))
}
