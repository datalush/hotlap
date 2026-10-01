// SPDX-License-Identifier: Apache-2.0
//! Share one validated offset capture across physical partitions of a query.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};

use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use tokio::sync::OnceCell;

pub(super) type Offsets = Arc<HashMap<i32, i64>>;
pub(super) type Capture = Arc<OnceCell<Result<Offsets, String>>>;

#[derive(Default, Debug)]
pub(super) struct OffsetCaptures {
    entries: Mutex<HashMap<usize, Entry>>,
}

#[derive(Debug)]
struct Entry {
    context: Weak<TaskContext>,
    seen: HashSet<usize>,
    capture: Capture,
}

impl OffsetCaptures {
    /// DataFusion shares an Arc<TaskContext> between all partitions of one
    /// query. Repeated execution of the same physical plan with the *same*
    /// context starts another generation when a partition is seen again.
    pub(super) fn for_partition(&self, context: &Arc<TaskContext>, partition: usize) -> Capture {
        let key = Arc::as_ptr(context) as usize;
        let mut entries = self.entries.lock().expect("offset registry mutex poisoned");
        entries.retain(|_, entry| entry.context.upgrade().is_some());
        let entry = entries.entry(key).or_insert_with(|| Entry {
            context: Arc::downgrade(context),
            seen: HashSet::new(),
            capture: Arc::new(OnceCell::new()),
        });
        if !entry.seen.insert(partition) {
            entry.seen.clear();
            entry.seen.insert(partition);
            entry.capture = Arc::new(OnceCell::new());
        }
        Arc::clone(&entry.capture)
    }
}

pub(super) fn validate_offsets(
    offsets: HashMap<i32, i64>,
    buckets: i32,
) -> DataFusionResult<Offsets> {
    for bucket in 0..buckets {
        match offsets.get(&bucket) {
            Some(&offset) if offset >= 0 => {}
            other => {
                return Err(DataFusionError::Execution(format!(
                    "Missing or invalid latest offset for Fluss bucket {bucket}: {other:?}"
                )));
            }
        }
    }
    Ok(Arc::new(offsets))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn partitions_share_capture_but_repeated_execution_starts_fresh() {
        let registry = OffsetCaptures::default();
        let context = Arc::new(TaskContext::default());
        let first = registry.for_partition(&context, 0);
        let second = registry.for_partition(&context, 1);
        assert!(Arc::ptr_eq(&first, &second));
        let next = registry.for_partition(&context, 0);
        assert!(!Arc::ptr_eq(&first, &next));
        assert!(Arc::ptr_eq(&next, &registry.for_partition(&context, 1)));

        let other = Arc::new(TaskContext::default());
        assert!(!Arc::ptr_eq(&next, &registry.for_partition(&other, 0)));
    }

    #[tokio::test]
    async fn concurrent_partitions_initialize_one_shared_capture() {
        let registry = OffsetCaptures::default();
        let context = Arc::new(TaskContext::default());
        let first = registry.for_partition(&context, 0);
        let second = registry.for_partition(&context, 1);
        let captures = AtomicUsize::new(0);
        let init = || async {
            captures.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok(Arc::new(HashMap::from([(0, 2), (1, 3)])))
        };
        let (a, b) = tokio::join!(first.get_or_init(init), second.get_or_init(init));
        assert_eq!(captures.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(a.as_ref().unwrap(), b.as_ref().unwrap()));
    }

    #[test]
    fn missing_or_negative_bucket_offsets_never_look_like_a_complete_scan() {
        assert!(validate_offsets(HashMap::from([(0, 10)]), 2).is_err());
        assert!(validate_offsets(HashMap::from([(0, 10), (1, -1)]), 2).is_err());
        assert_eq!(
            validate_offsets(HashMap::from([(0, 0), (1, 5)]), 2)
                .unwrap()
                .len(),
            2
        );
    }
}
