//! Commit re-drive: replay idempotent writes is safe, but only a genuinely
//! durable staged state (or a true no-op) may complete an interrupted commit
//! after a restart.

use std::sync::{Arc, Mutex};

use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};

/// A sink that only declares its delivery capability.
struct Fake {
    capabilities: SinkCapabilities,
}

#[async_trait::async_trait]
impl Sink for Fake {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        self.capabilities
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// An idempotent sink that only queues writes in memory before flushing.
struct VolatileQueueSink {
    queued: Mutex<Vec<i64>>,
}

impl VolatileQueueSink {
    fn new() -> Self {
        Self {
            queued: Mutex::new(Vec::new()),
        }
    }

    fn enqueue(&self, value: i64) {
        self.queued.lock().unwrap().push(value);
    }

    fn queued(&self) -> usize {
        self.queued.lock().unwrap().len()
    }
}

#[async_trait::async_trait]
impl Sink for VolatileQueueSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.queued.lock().unwrap().clear();
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A sink whose staged writes live in a store shared across instances, so a
/// restarted instance can still complete the commit.
#[derive(Clone, Default)]
struct StagedStore(Arc<Mutex<Vec<i64>>>);

struct DurableStagedSink {
    store: StagedStore,
}

#[async_trait::async_trait]
impl Sink for DurableStagedSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }
    fn commit_redriable(&self) -> bool {
        true
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.store.0.lock().unwrap().clear();
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[test]
fn commit_is_not_redrivable_by_default_for_any_capability() {
    let sink = |capabilities| Fake { capabilities };
    assert!(!sink(SinkCapabilities::Transactional).commit_redriable());
    assert!(
        !sink(SinkCapabilities::Idempotent).commit_redriable(),
        "idempotent replay does not make an interrupted commit completable"
    );
    assert!(!sink(SinkCapabilities::AtLeastOnce).commit_redriable());
}

#[test]
fn a_new_instance_cannot_complete_a_volatile_queue_commit() {
    let before_crash = VolatileQueueSink::new();
    before_crash.enqueue(1);
    before_crash.enqueue(2);
    assert_eq!(before_crash.queued(), 2);

    // A restart hands out a new instance with an empty queue, so re-driving its
    // commit cannot deliver the writes the crashed instance accepted.
    let after_restart = VolatileQueueSink::new();
    assert_eq!(after_restart.queued(), 0, "the old queue is gone");
    assert!(
        !after_restart.commit_redriable(),
        "a volatile queue must not declare its commit re-drivable"
    );
}

#[test]
fn a_shared_durable_store_makes_a_new_instance_redrivable() {
    let store = StagedStore::default();
    let crashed = DurableStagedSink {
        store: store.clone(),
    };
    crashed.store.0.lock().unwrap().push(7);
    assert!(crashed.commit_redriable());

    // A new instance over the same durable store still sees the staged write,
    // so re-driving its commit is safe.
    let restarted = DurableStagedSink {
        store: store.clone(),
    };
    assert_eq!(restarted.store.0.lock().unwrap().as_slice(), &[7]);
    assert!(restarted.commit_redriable());
}
