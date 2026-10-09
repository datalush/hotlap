//! A sink with a volatile writer queue cannot complete an interrupted commit
//! after a crash; recovery must replay from the fallback and re-deliver rows.

#[path = "common/recovery.rs"]
mod recovery;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::executor::block_on;
use hotlap::state::StateBackend;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_engine::MetricsRegistry;
use hotlap_runtime::runtime::checkpoint::{Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline;
use hotlap_runtime::runtime::recovery::{Recovery, RecoveryDecision};
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use recovery::{Dataset, ResumableSource, SharedBackend, drain, engine_with, rows, take};

/// Durable output shared by every sink instance, surviving a "process".
#[derive(Clone, Default)]
struct RemoteStore(Arc<Mutex<Vec<Vec<i64>>>>);

impl RemoteStore {
    fn write(&self, rows: Vec<Vec<i64>>) {
        *self.0.lock().unwrap() = rows;
    }

    fn rows(&self) -> Vec<Vec<i64>> {
        self.0.lock().unwrap().clone()
    }
}

/// An idempotent sink that only stages the latest snapshot in memory and writes
/// it to the remote store on commit. Its staged queue is volatile: a fresh
/// instance (a restart) starts empty.
struct StagedSink {
    remote: RemoteStore,
    staged: Mutex<Option<Vec<Vec<i64>>>>,
    commits: Arc<AtomicU32>,
}

impl StagedSink {
    fn new(remote: RemoteStore) -> (Arc<Self>, Arc<AtomicU32>) {
        let commits = Arc::new(AtomicU32::new(0));
        let sink = Arc::new(Self {
            remote,
            staged: Mutex::new(None),
            commits: Arc::clone(&commits),
        });
        (sink, commits)
    }

    fn stage(&self, rows: Vec<Vec<i64>>) {
        *self.staged.lock().unwrap() = Some(rows);
    }
}

#[async_trait::async_trait]
impl Sink for StagedSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }
    fn accepts_retractions(&self) -> bool {
        true
    }
    // `commit_redriable` is left at the default: idempotent replay does not make
    // an interrupted commit completable from a fresh, empty queue.
    async fn commit(&self) -> Result<(), ConnectorError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        if let Some(rows) = self.staged.lock().unwrap().take() {
            self.remote.write(rows);
        }
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        *self.staged.lock().unwrap() = None;
        Ok(())
    }
}

/// A sink that never writes to the shared remote store, used only to capture
/// checkpoint bodies.
struct NoopSink;

#[async_trait::async_trait]
impl Sink for NoopSink {
    async fn write(&self, _changes: ChangeStream) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::Idempotent
    }
    async fn commit(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A real log of four single-key events.
fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1], vec![2], vec![3]]).with_retention(0)
}

#[test]
fn a_fresh_volatile_sink_cannot_promote_and_replay_redelivers() {
    let backend = SharedBackend::default();
    let remote = RemoteStore::default();

    // Capture a valid checkpoint C1 after three events and a valid C2 after the
    // fourth, with a sink that does not touch the remote store.
    let (mut engine, pipe) = engine_with(ResumableSource::new(log()));
    let mut ckpt = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
        SinkSync::sink_only(SharedSink::new(Arc::new(NoopSink))),
    ]);
    let mut stream = pipe.sources.stream().unwrap();
    drain(&mut engine, &pipe.sources, &mut stream, 3);
    let c1 = take(&mut ckpt, &engine, &pipe.sources);
    drain(&mut engine, &pipe.sources, &mut stream, 1);
    let c2 = take(&mut ckpt, &engine, &pipe.sources);

    // Simulate the process dying between C2's durable commit marker and its
    // `valid` marker: the fourth event's writes were only in memory.
    let mut writer = backend.clone();
    writer
        .delete(format!("checkpoint/{c2}/valid").as_bytes())
        .unwrap();
    writer
        .put(format!("checkpoint/{c2}/commit").as_bytes(), b"1".to_vec())
        .unwrap();
    assert!(
        remote.rows().is_empty(),
        "nothing was delivered before replay"
    );

    // Restart: a fresh sink shares the durable remote but has an empty queue.
    let (mut engine_b, pipe_b) = engine_with(ResumableSource::new(log()));
    let (sink_b, commits) = StagedSink::new(remote.clone());
    let mut ckpt_b = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![SinkSync::sink_only(SharedSink::new(sink_b.clone()))]);

    match Recovery::inspect(&ckpt_b, &pipe_b.sources).unwrap() {
        RecoveryDecision::Discard {
            pending, fallback, ..
        } => {
            assert_eq!(pending, c2);
            assert_eq!(fallback.expect("fallback").id, c1);
        }
        other => panic!("a fresh volatile sink must not promote: {other:?}"),
    }

    let mut resumed = block_on(Recovery::start(
        &mut engine_b,
        &pipe_b.sources,
        &mut ckpt_b,
        &Mutex::new(None),
        &MetricsRegistry::new(),
    ))
    .unwrap();
    while let Some(item) = block_on(resumed.next()) {
        let event = item.unwrap();
        pipeline::ingest_event(&mut engine_b, &pipe_b.sources, &event).unwrap();
        sink_b.stage(rows(&engine_b.snapshot("c").unwrap()));
        pipe_b
            .sources
            .get(event.input)
            .unwrap()
            .source
            .commit(event.batch.split, event.batch.next_offset)
            .unwrap();
        block_on(sink_b.commit()).unwrap();
    }

    assert!(commits.load(Ordering::SeqCst) >= 1, "replay must deliver");
    let delivered: BTreeSet<Vec<i64>> = remote.rows().into_iter().collect();
    assert!(
        delivered.contains(&vec![3, 1]),
        "the fourth event's row must be redelivered, got {delivered:?}"
    );
}
