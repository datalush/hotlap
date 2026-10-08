use std::collections::HashMap;

use arrow::row::OwnedRow;

/// Byte-keyed `(key, payload) -> diff` state that collapses zero-sum entries.
#[derive(Default)]
pub(super) struct RowState {
    entries: HashMap<Vec<u8>, HashMap<OwnedRow, i64>>,
}

impl RowState {
    /// Adds `diff` to one entry, forgetting entries whose sum becomes zero.
    pub(super) fn add(&mut self, key: OwnedRow, payload: OwnedRow, diff: i64) {
        let payloads = self.entries.entry(key.as_ref().to_vec()).or_default();
        let sum = payloads.entry(payload.clone()).or_insert(0);
        *sum += diff;
        if *sum == 0 {
            payloads.remove(&payload);
        }
        if payloads.is_empty() {
            self.entries.remove(key.as_ref());
        }
    }

    /// Returns the payload multiset for `key`, if the key is present.
    pub(super) fn get(&self, key: &[u8]) -> Option<&HashMap<OwnedRow, i64>> {
        self.entries.get(key)
    }

    /// Number of surviving `(key, payload)` entries.
    pub(super) fn len(&self) -> usize {
        self.entries.values().map(HashMap::len).sum()
    }

    /// Returns `true` when no entry survives.
    pub(super) fn is_empty(&self) -> bool {
        self.entries.values().all(HashMap::is_empty)
    }

    /// Returns entries as `(key, payload, diff)`, ordered by key then payload.
    pub(super) fn sorted(&self) -> Vec<(&Vec<u8>, &OwnedRow, i64)> {
        let mut entries: Vec<(&Vec<u8>, &OwnedRow, i64)> = Vec::new();
        for (key, payloads) in &self.entries {
            for (payload, &diff) in payloads {
                entries.push((key, payload, diff));
            }
        }
        entries.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
        entries
    }
}
