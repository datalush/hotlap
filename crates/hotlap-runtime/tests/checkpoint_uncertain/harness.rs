//! Fixtures for the uncertain-commit tests: a remote multiset bag and the staged
//! transactional sink that delivers into it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

pub use crate::support::SharedBackend;
pub use crate::txn::TxnSink;

/// A multiset of weighted rows, applied by signed multiplicity.
#[derive(Clone, Default)]
pub struct RemoteBag(Arc<Mutex<BTreeMap<Vec<i64>, i64>>>);

impl RemoteBag {
    /// Apply a weighted changelog, dropping rows that reach zero.
    pub fn apply(&self, changes: &[(Vec<i64>, i64)]) {
        let mut map = self.0.lock().unwrap();
        for (row, diff) in changes {
            let entry = map.entry(row.clone()).or_insert(0);
            *entry += diff;
            if *entry == 0 {
                map.remove(row);
            }
        }
    }

    /// The current nonzero rows as `row -> multiplicity`.
    pub fn snapshot(&self) -> BTreeMap<Vec<i64>, i64> {
        self.0.lock().unwrap().clone()
    }
}
