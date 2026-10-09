//! A [`StateBackend`] wrapper that injects operational failures on demand.
//!
//! `fail(op, key, after)` arms one fault for the next exact call: `after = false`
//! makes the call fail without touching the store (the write never became
//! durable), while `after = true` performs the real call and then reports
//! failure (the effect is durable but the caller saw an error, modelling an
//! ambiguous acknowledgement). Each fault is consumed once.

use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex};

use hotlap::state::{StateBackend, StateEntry, StateError};

struct Directive {
    op: &'static str,
    key: Vec<u8>,
    after: bool,
}

/// A backend that delegates to `inner` but can fail selected calls.
pub struct FaultBackend<B> {
    inner: B,
    plan: Arc<Mutex<VecDeque<Directive>>>,
}

impl<B: Clone> Clone for FaultBackend<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            plan: Arc::clone(&self.plan),
        }
    }
}

impl<B> FaultBackend<B> {
    /// Wrap `inner` with an empty fault plan.
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            plan: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Arm a one-shot fault for `op` on `key`; `after` fails after the effect.
    pub fn fail(&self, op: &'static str, key: &[u8], after: bool) {
        self.plan.lock().unwrap().push_back(Directive {
            op,
            key: key.to_vec(),
            after,
        });
    }

    /// Consume a matching directive, returning whether to fail after the effect.
    fn fire(&self, op: &str, key: &[u8]) -> Option<bool> {
        let mut plan = self.plan.lock().unwrap();
        if plan.front().is_some_and(|d| d.op == op && d.key == key) {
            plan.pop_front().map(|d| d.after)
        } else {
            None
        }
    }
}

/// A concrete injected storage failure.
fn injected() -> StateError {
    StateError::Io(io::Error::other("injected store failure"))
}

impl<B: StateBackend> StateBackend for FaultBackend<B> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        match self.fire("get", key) {
            Some(false) => Err(injected()),
            Some(true) => {
                let _ = self.inner.get(key);
                Err(injected())
            }
            None => self.inner.get(key),
        }
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        match self.fire("put", key) {
            Some(false) => Err(injected()),
            Some(true) => {
                self.inner.put(key, value)?;
                Err(injected())
            }
            None => self.inner.put(key, value),
        }
    }

    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        match self.fire("scan", prefix) {
            Some(false) => Err(injected()),
            Some(true) => {
                let _ = self.inner.scan(prefix);
                Err(injected())
            }
            None => self.inner.scan(prefix),
        }
    }

    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        match self.fire("list", prefix) {
            Some(false) => Err(injected()),
            Some(true) => {
                let _ = self.inner.list(prefix);
                Err(injected())
            }
            None => self.inner.list(prefix),
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        match self.fire("delete", key) {
            Some(false) => Err(injected()),
            Some(true) => {
                self.inner.delete(key)?;
                Err(injected())
            }
            None => self.inner.delete(key),
        }
    }
}
