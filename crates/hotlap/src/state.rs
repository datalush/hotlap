//! Pluggable state backend boundary.
//!
//! SP1a ships only the in-memory implementation, but the trait is the seam a
//! future SP4 durable backend plugs into without touching the core.

use std::collections::BTreeMap;

/// Key/value store owned by an engine. Values are opaque bytes so persistence
/// choices do not leak into the core.
pub trait StateBackend {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn put(&mut self, key: &[u8], value: Vec<u8>);
}

impl StateBackend for BTreeMap<Vec<u8>, Vec<u8>> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get(key).cloned()
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) {
        self.insert(key.to_vec(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn btreemap_backend_round_trips() {
        let mut be: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        assert_eq!(StateBackend::get(&be, b"missing"), None);
        StateBackend::put(&mut be, b"k", b"v".to_vec());
        assert_eq!(StateBackend::get(&be, b"k"), Some(b"v".to_vec()));
        StateBackend::put(&mut be, b"k", b"v2".to_vec());
        assert_eq!(StateBackend::get(&be, b"k"), Some(b"v2".to_vec()));
    }
}
