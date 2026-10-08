//! In-memory [`StateBackend`] shared with a test, so writes stay observable.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use hotlap::state::{StateBackend, StateEntry, StateError};

/// A cloneable in-memory backend whose contents the test can inspect.
#[derive(Clone, Default)]
pub struct SharedBackend {
    map: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl StateBackend for SharedBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        self.map.lock().unwrap().insert(key.to_vec(), value);
        Ok(())
    }
    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        self.map.lock().unwrap().remove(key);
        Ok(())
    }
}
