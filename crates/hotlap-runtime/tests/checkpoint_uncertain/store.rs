//! The logical durable state of a sink: staged payloads, committed ids and the
//! remote multiset, installed atomically under one guard.
//!
//! A commit computes the next remote bag and the committed set and installs
//! them in a single critical section, so an ACK lost after the commit point
//! still leaves the effect recorded and a re-driven commit is a no-op. There is
//! no separate apply-then-record window. This models the transaction outcome;
//! it does not claim a real filesystem atomic transaction.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use hotlap_connectors::ConnectorError;

/// A weighted row change.
pub type Change = (Vec<i64>, i64);

type TxKey = (&'static str, u64);

#[derive(Default)]
struct Inner {
    staged: HashMap<TxKey, Vec<Change>>,
    committed: BTreeSet<TxKey>,
    active: HashMap<&'static str, u64>,
    next: HashMap<&'static str, u64>,
    remote: BTreeMap<(&'static str, Vec<i64>), i64>,
    fail_before: HashMap<&'static str, u32>,
    fail_after: HashMap<&'static str, u32>,
}

/// The shared logical store every sink instance observes.
pub struct Store {
    inner: Mutex<Inner>,
}

impl Store {
    /// An empty store.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
        })
    }

    /// Stage `payload` as the next transaction of `view`, returning its id.
    pub fn stage(&self, view: &'static str, payload: Vec<Change>) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let next = inner.next.entry(view).or_insert(0);
        let id = *next;
        *next += 1;
        inner.staged.insert((view, id), payload);
        inner.active.insert(view, id);
        id
    }

    /// Atomically commit the active transaction of `view`.
    ///
    /// Returns whether the remote bag changed; a repeated or re-driven commit
    /// of an already committed transaction is a no-op.
    pub fn commit(&self, view: &'static str) -> Result<bool, ConnectorError> {
        let mut inner = self.inner.lock().unwrap();
        let Some(id) = inner.active.get(view).copied() else {
            return Ok(false);
        };
        if inner.committed.contains(&(view, id)) {
            return Ok(false);
        }
        if fire(&mut inner.fail_before, view) {
            return Err(rejected("commit rejected before applying"));
        }
        let changes = inner.staged.remove(&(view, id)).unwrap_or_default();
        for (row, diff) in changes {
            let entry = inner.remote.entry((view, row.clone())).or_insert(0);
            *entry += diff;
            if *entry == 0 {
                inner.remote.remove(&(view, row));
            }
        }
        inner.committed.insert((view, id));
        if fire(&mut inner.fail_after, view) {
            return Err(rejected("commit applied but not acknowledged"));
        }
        Ok(true)
    }

    /// Abort the active transaction of `view`; only an uncommitted one.
    pub fn abort(&self, view: &'static str) -> Result<(), ConnectorError> {
        let mut inner = self.inner.lock().unwrap();
        let Some(id) = inner.active.get(view).copied() else {
            return Ok(());
        };
        if inner.committed.contains(&(view, id)) {
            return Err(ConnectorError::Unsupported(
                "cannot abort a committed transaction".into(),
            ));
        }
        inner.staged.remove(&(view, id));
        inner.active.remove(view);
        Ok(())
    }

    /// Fail the next `attempts` commits before any effect.
    pub fn fail_before(&self, view: &'static str, attempts: u32) {
        self.inner
            .lock()
            .unwrap()
            .fail_before
            .insert(view, attempts);
    }

    /// Fail the next `attempts` commits after the effect is installed.
    pub fn fail_after(&self, view: &'static str, attempts: u32) {
        self.inner.lock().unwrap().fail_after.insert(view, attempts);
    }

    /// The committed remote bag of `view` as `row -> multiplicity`.
    pub fn remote_bag(&self, view: &'static str) -> BTreeMap<Vec<i64>, i64> {
        self.inner
            .lock()
            .unwrap()
            .remote
            .iter()
            .filter(|((owner, _), _)| *owner == view)
            .map(|((_, row), weight)| (row.clone(), *weight))
            .collect()
    }
}

fn fire(map: &mut HashMap<&'static str, u32>, view: &'static str) -> bool {
    match map.get_mut(view) {
        Some(remaining) if *remaining > 0 => {
            *remaining -= 1;
            true
        }
        _ => false,
    }
}

fn rejected(message: &str) -> ConnectorError {
    ConnectorError::Unsupported(message.into())
}
