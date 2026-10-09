//! Persistent staged-payload fixtures for checkpoint abort tests.

use std::sync::{Arc, Mutex};

use futures::StreamExt;
use hotlap::InputId;
use hotlap::state::{StateBackend, StateEntry, StateError};
use hotlap_connectors::source::{Source, SourceState, SourceStream, Split};
use hotlap_connectors::{ChangeStream, ConnectorError, Sink, SinkCapabilities};
use hotlap_runtime::runtime::sources::{InputSource, Sources};

#[derive(Default)]
pub struct Remote {
    staged: Mutex<Vec<i64>>,
    committed: Mutex<Vec<i64>>,
}

impl Remote {
    pub fn staged(&self) -> Vec<i64> {
        self.staged.lock().unwrap().clone()
    }

    pub fn committed(&self) -> Vec<i64> {
        self.committed.lock().unwrap().clone()
    }
}

pub struct PersistentTxn {
    remote: Arc<Remote>,
    fail_abort: bool,
    fail_prepare: bool,
    stall_abort: bool,
    redriable: bool,
}

impl PersistentTxn {
    pub fn new(remote: Arc<Remote>, fail_abort: bool, redriable: bool) -> Arc<Self> {
        Arc::new(Self {
            remote,
            fail_abort,
            fail_prepare: false,
            stall_abort: false,
            redriable,
        })
    }

    pub fn failing_prepare(remote: Arc<Remote>, fail_abort: bool, redriable: bool) -> Arc<Self> {
        Arc::new(Self {
            remote,
            fail_abort,
            fail_prepare: true,
            stall_abort: false,
            redriable,
        })
    }

    pub fn stalled_abort(remote: Arc<Remote>) -> Arc<Self> {
        Arc::new(Self {
            remote,
            fail_abort: false,
            fail_prepare: false,
            stall_abort: true,
            redriable: false,
        })
    }
}

pub fn engine() -> hotlap::Hotlap {
    hotlap::Hotlap::open_with(Box::new(hotlap_engine::EngineCore::new()))
}

struct EmptySource;

impl Source for EmptySource {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(
            Vec::<arrow::datatypes::Field>::new(),
        ))
    }
    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        Ok(Vec::new())
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, ConnectorError> {
        Ok(Box::pin(futures::stream::empty()))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

pub fn sources() -> Sources {
    Sources::new(vec![InputSource {
        id: InputId(0),
        name: "in".into(),
        source: Arc::new(EmptySource),
        watermark: None,
    }])
    .unwrap()
}

#[derive(Clone, Default)]
pub struct FaultBackend {
    inner: crate::backend::SharedBackend,
    fail_engine: Arc<Mutex<bool>>,
    fail_clear: Arc<Mutex<bool>>,
}

impl FaultBackend {
    pub fn new(inner: crate::backend::SharedBackend, fail_clear: bool) -> Self {
        Self {
            inner,
            fail_engine: Arc::new(Mutex::new(true)),
            fail_clear: Arc::new(Mutex::new(fail_clear)),
        }
    }
}

impl StateBackend for FaultBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        self.inner.get(key)
    }
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError> {
        if key == b"checkpoint/1/engine" && std::mem::take(&mut *self.fail_engine.lock().unwrap()) {
            return Err(StateError::Io(std::io::Error::other("capture failed")));
        }
        self.inner.put(key, value)
    }
    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError> {
        self.inner.scan(prefix)
    }
    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError> {
        self.inner.list(prefix)
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        if key == b"checkpoint/1/prepare" && *self.fail_clear.lock().unwrap() {
            return Err(StateError::Io(std::io::Error::other("clear failed")));
        }
        self.inner.delete(key)
    }
}

#[async_trait::async_trait]
impl Sink for PersistentTxn {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            item?;
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }

    fn accepts_retractions(&self) -> bool {
        true
    }

    fn commit_redriable(&self) -> bool {
        self.redriable
    }

    async fn prepare(&self) -> Result<(), ConnectorError> {
        self.remote.staged.lock().unwrap().extend([7]);
        if self.fail_prepare {
            return Err(ConnectorError::Infrastructure("prepare failed".into()));
        }
        Ok(())
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        let staged = std::mem::take(&mut *self.remote.staged.lock().unwrap());
        self.remote.committed.lock().unwrap().extend(staged);
        Ok(())
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        if self.stall_abort {
            futures::future::pending().await
        }
        if self.fail_abort {
            return Err(ConnectorError::Unsupported("abort denied".into()));
        }
        self.remote.staged.lock().unwrap().clear();
        Ok(())
    }
}
