//! Pluggable state backend boundary.
//!
//! Keys and values are opaque bytes, so persistence choices (in-memory map,
//! files on disk) stay behind the interface and do not leak into the core.
//! All operations are fallible: a real backend can hit filesystem errors and
//! must surface them instead of panicking.
//!
//! Key shape: a namespace path of non-empty segments separated by `/`, e.g.
//! `checkpoint/7/state`. The empty key and keys with empty segments are
//! rejected with [`StateError`]. Scan prefixes may be any byte string; an
//! empty prefix means "the whole store".

use std::collections::BTreeMap;
use std::fmt;
use std::io;

mod durable;
mod fsio;
#[cfg(test)]
mod fsio_tests;
mod path;

pub use durable::DurableStateBackend;

/// Error returned by [`StateBackend`] operations.
#[derive(Debug)]
pub enum StateError {
    /// The empty key was passed to `get`/`put`.
    EmptyKey,
    /// A key with an empty path segment (`a//b`, `/a`, `a/`), or a stored key
    /// that collides with an existing namespace directory.
    InvalidKey,
    /// The underlying filesystem operation failed.
    Io(io::Error),
}

impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateError::EmptyKey => write!(f, "state key must not be empty"),
            StateError::InvalidKey => write!(f, "state key has an empty or colliding segment"),
            StateError::Io(e) => write!(f, "state I/O failed: {e}"),
        }
    }
}

impl std::error::Error for StateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StateError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for StateError {
    fn from(e: io::Error) -> Self {
        StateError::Io(e)
    }
}

/// A stored key/value pair returned by [`StateBackend::scan`].
pub type StateEntry = (Vec<u8>, Vec<u8>);

/// Key/value store owned by an engine.
pub trait StateBackend {
    /// Return the value stored under `key`, or `None` when absent.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError>;

    /// Store `value` under `key`, replacing any previous value.
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError>;

    /// Return every `(key, value)` whose key starts with `prefix`, sorted by
    /// key bytes ascending so iteration order is deterministic.
    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError>;

    /// Return every key that starts with `prefix`, sorted ascending. These are
    /// the same keys as [`StateBackend::scan`], read without the values.
    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError>;

    /// Remove `key`. Deleting an absent key is not an error, so retention can
    /// retry safely after a partial cleanup.
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError>;
}

impl StateBackend for BTreeMap<Vec<u8>, Vec<u8>> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        path::validate_key(key)?;
        Ok(self.get(key).cloned())
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        path::validate_key(key)?;
        self.insert(key.to_vec(), value);
        Ok(())
    }

    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        // Keys sharing a prefix are contiguous in a BTreeMap, so the range
        // starting at `prefix` yields exactly the matching keys up front.
        Ok(self
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        Ok(self
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, _)| key.clone())
            .collect())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        path::validate_key(key)?;
        self.remove(key);
        Ok(())
    }
}
