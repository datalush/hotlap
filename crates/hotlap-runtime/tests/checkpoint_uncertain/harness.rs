//! A staged, re-drivable transactional sink for the uncertain-commit tests.
//!
//! The sink keeps its prepared value in a durable `stage` store and transfers
//! it to a separate `remote` store on `commit`. `commit` is idempotent: it only
//! moves a value that is still staged, so a re-driven commit never duplicates.
//! A new instance sharing the same two stores observes the staged state.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};

pub use crate::support::SharedBackend;

/// Control calls a durable sink recorded, for value assertions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    Prepare,
    Commit,
    Abort,
}

/// A transactional sink staging to `stage` and delivering to `remote`.
pub struct DurableSink {
    token: &'static str,
    stage: SharedBackend,
    remote: SharedBackend,
    redriable: bool,
    fail_commit: AtomicU32,
    events: Mutex<Vec<Event>>,
}

impl DurableSink {
    /// A sink staging `token` and delivering it to `remote` on commit.
    pub fn new(
        token: &'static str,
        stage: SharedBackend,
        remote: SharedBackend,
        redriable: bool,
    ) -> Self {
        Self {
            token,
            stage,
            remote,
            redriable,
            fail_commit: AtomicU32::new(0),
            events: Mutex::new(Vec::new()),
        }
    }

    /// A sink whose first `attempts` commits fail before delivering.
    pub fn failing(
        token: &'static str,
        stage: SharedBackend,
        remote: SharedBackend,
        redriable: bool,
        attempts: u32,
    ) -> Self {
        let sink = Self::new(token, stage, remote, redriable);
        sink.fail_commit.store(attempts, Ordering::SeqCst);
        sink
    }

    /// Control calls recorded so far.
    pub fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    /// Value delivered to `remote` for this sink, if any.
    pub fn delivered(&self) -> Option<Vec<u8>> {
        self.remote
            .get(format!("remote/{}", self.token).as_bytes())
            .unwrap()
    }

    /// Value still staged for this sink, if any.
    pub fn staged(&self) -> Option<Vec<u8>> {
        self.stage
            .get(format!("stage/{}", self.token).as_bytes())
            .unwrap()
    }

    fn record(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }
}

fn storage(error: hotlap::state::StateError) -> ConnectorError {
    ConnectorError::Storage(error)
}

#[async_trait::async_trait]
impl Sink for DurableSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Transactional
    }

    fn commit_redriable(&self) -> bool {
        self.redriable
    }

    async fn prepare(&self) -> Result<(), ConnectorError> {
        self.record(Event::Prepare);
        self.stage
            .clone()
            .put(
                format!("stage/{}", self.token).as_bytes(),
                self.token.into(),
            )
            .map_err(storage)
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.record(Event::Commit);
        if self.fail_commit.load(Ordering::SeqCst) > 0 {
            self.fail_commit.fetch_sub(1, Ordering::SeqCst);
            return Err(ConnectorError::Infrastructure("commit failed".into()));
        }
        let mut stage = self.stage.clone();
        let Some(value) = stage
            .get(format!("stage/{}", self.token).as_bytes())
            .map_err(storage)?
        else {
            return Ok(());
        };
        self.remote
            .clone()
            .put(format!("remote/{}", self.token).as_bytes(), value)
            .map_err(storage)?;
        stage
            .delete(format!("stage/{}", self.token).as_bytes())
            .map_err(storage)
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        self.record(Event::Abort);
        self.stage
            .clone()
            .delete(format!("stage/{}", self.token).as_bytes())
            .map_err(storage)
    }
}

/// Wrap a durable sink so the barrier coordinates it.
pub fn coord(sink: Arc<DurableSink>) -> hotlap_runtime::runtime::sink::SinkSync {
    hotlap_runtime::runtime::sink::SinkSync::sink_only(
        hotlap_runtime::runtime::sink::SharedSink::new(sink),
    )
}
