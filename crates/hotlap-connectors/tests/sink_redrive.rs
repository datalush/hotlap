//! Commit re-drive: replaying idempotent writes is safe, but only a genuinely
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

/// Persistent staged input, shared across sink instances.
#[derive(Clone, Default)]
struct StagedStore(Arc<Mutex<Vec<i64>>>);

/// Persistent delivered output, distinct from the staged input.
#[derive(Clone, Default)]
struct DeliveredStore(Arc<Mutex<Vec<i64>>>);

impl DeliveredStore {
    fn values(&self) -> Vec<i64> {
        self.0.lock().unwrap().clone()
    }
}

/// A sink whose staged writes persist across instances and whose `commit`
/// transfers them into the delivered output, clearing the stage only after the
/// transfer so a repeated commit cannot duplicate.
struct DurableStagedSink {
    staged: StagedStore,
    delivered: DeliveredStore,
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
        let mut staged = self.staged.0.lock().unwrap();
        self.delivered.0.lock().unwrap().extend(staged.drain(..));
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        self.staged.0.lock().unwrap().clear();
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
fn a_new_instance_delivers_durable_staged_writes_without_duplicating() {
    let staged = StagedStore::default();
    let delivered = DeliveredStore::default();
    let crashed = DurableStagedSink {
        staged: staged.clone(),
        delivered: delivered.clone(),
    };
    staged.0.lock().unwrap().push(7);
    assert!(crashed.commit_redriable());
    assert!(delivered.values().is_empty(), "nothing delivered yet");

    // A new instance over the same durable stores still sees the staged write,
    // so re-driving its commit delivers it.
    let restarted = DurableStagedSink {
        staged: staged.clone(),
        delivered: delivered.clone(),
    };
    futures::executor::block_on(restarted.commit()).unwrap();
    assert_eq!(delivered.values(), vec![7]);
    assert!(
        staged.0.lock().unwrap().is_empty(),
        "stage cleared on delivery"
    );

    // A repeated commit drains nothing, so it cannot duplicate the value.
    futures::executor::block_on(restarted.commit()).unwrap();
    assert_eq!(delivered.values(), vec![7]);
}
