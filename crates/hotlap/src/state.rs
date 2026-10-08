//! Pluggable state backend boundary.
//!
//! Keys and values are opaque bytes, so persistence choices (in-memory map,
//! files on disk) stay behind the interface and do not leak into the core.
//! SP4 adds prefix `scan`/`list` plus [`DurableStateBackend`], a local
//! file-backed implementation that survives a close/reopen cycle.

use std::collections::BTreeMap;

mod durable;

pub use durable::DurableStateBackend;

/// Key/value store owned by an engine.
pub trait StateBackend {
    /// Return the value stored under `key`, or `None` when absent.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Store `value` under `key`, replacing any previous value.
    fn put(&mut self, key: &[u8], value: Vec<u8>);

    /// Return every `(key, value)` whose key starts with `prefix`, sorted by
    /// key bytes ascending so iteration order is deterministic.
    fn scan(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Return every key that starts with `prefix`, sorted ascending. These are
    /// the same keys as [`StateBackend::scan`], read without the values.
    fn list(&self, prefix: &[u8]) -> Vec<Vec<u8>>;
}

impl StateBackend for BTreeMap<Vec<u8>, Vec<u8>> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get(key).cloned()
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) {
        self.insert(key.to_vec(), value);
    }

    fn scan(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        // Keys sharing a prefix are contiguous in a BTreeMap, so the range
        // starting at `prefix` yields exactly the matching keys up front.
        self.range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    fn list(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        self.range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, _)| key.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut be: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        StateBackend::put(&mut be, b"user/1", b"a".to_vec());
        StateBackend::put(&mut be, b"user/2", b"b".to_vec());
        StateBackend::put(&mut be, b"view/1", b"c".to_vec());
        be
    }

    #[test]
    fn btreemap_backend_round_trips() {
        let mut be: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        assert_eq!(StateBackend::get(&be, b"missing"), None);
        StateBackend::put(&mut be, b"k", b"v".to_vec());
        assert_eq!(StateBackend::get(&be, b"k"), Some(b"v".to_vec()));
        StateBackend::put(&mut be, b"k", b"v2".to_vec());
        assert_eq!(StateBackend::get(&be, b"k"), Some(b"v2".to_vec()));
    }

    #[test]
    fn btreemap_scan_filters_and_sorts() {
        let be = sample();
        let hits = StateBackend::scan(&be, b"user/");
        assert_eq!(
            hits,
            vec![
                (b"user/1".to_vec(), b"a".to_vec()),
                (b"user/2".to_vec(), b"b".to_vec()),
            ]
        );
        assert_eq!(StateBackend::scan(&be, b"missing"), Vec::new());
    }

    #[test]
    fn btreemap_list_matches_scan_keys() {
        let be = sample();
        let keys = StateBackend::list(&be, b"");
        let scanned: Vec<Vec<u8>> = StateBackend::scan(&be, b"")
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, scanned);
        assert_eq!(
            keys,
            vec![b"user/1".to_vec(), b"user/2".to_vec(), b"view/1".to_vec()]
        );
    }
}
