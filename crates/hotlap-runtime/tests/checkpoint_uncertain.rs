//! A checkpoint that fails after the sinks were touched blocks further attempts.
//!
//! The engine and source offsets are not rolled back with the sinks, so the
//! runtime must stop ingesting until a restart resolves the attempt. A commit
//! failure keeps the marker, the body and the staged transaction, and the
//! interleaved deliveries match the engine views exactly.

#[path = "checkpoint_uncertain/codec.rs"]
pub mod codec;
#[path = "common/spy.rs"]
mod spy;
#[path = "checkpoint_uncertain/store.rs"]
pub mod store;
#[path = "checkpoint_uncertain/support.rs"]
pub mod support;
#[path = "checkpoint_uncertain/txn.rs"]
pub mod txn;

use std::sync::Arc;
use std::sync::Mutex;

use futures::StreamExt;
use hotlap::state::StateBackend;
use hotlap::{Hotlap, ZSetBatch};
use hotlap_connectors::ConnectorError;
use hotlap_runtime::runtime::checkpoint::{CheckpointState, Checkpointer, DEFAULT_RETAIN};
use hotlap_runtime::runtime::pipeline::Pipeline;
use hotlap_runtime::runtime::recovery::Recovery;
use hotlap_runtime::runtime::sink::{SharedSink, SinkSync};
use hotlap_runtime::runtime::sources::{InputStream, SourceEvent};

use store::Store;
use support::{
    COUNT, Dataset, ROWS, ResumableSource, SharedBackend, bag, engine_for, engine_with, ingest,
    sources_of,
};
use txn::TxnSink;

fn log() -> Dataset {
    Dataset::new(vec![vec![1], vec![1, 2], vec![2], vec![3]])
        .with_retention(0)
        .with_physical_identity("test/checkpoint-uncertain/source")
}

/// One running engine wired to the two distinct sinks.
struct Run {
    backend: SharedBackend,
    store: Arc<Store>,
    shared_a: Arc<SharedSink>,
    shared_b: Arc<SharedSink>,
    checkpointer: Checkpointer,
    engine: Hotlap,
    pipe: Pipeline,
    stream: InputStream,
}

impl Run {
    fn new() -> Self {
        let backend = SharedBackend::default();
        let store = Store::new();
        let shared_a = SharedSink::new(Arc::new(TxnSink::new(ROWS, store.clone())));
        let shared_b = SharedSink::new(Arc::new(TxnSink::new(COUNT, store.clone())));
        let checkpointer =
            Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN).with_sinks(vec![
                SinkSync::sink_only_named(shared_a.clone(), "rows".into(), ROWS.into()),
                SinkSync::sink_only_named(shared_b.clone(), "count".into(), COUNT.into()),
            ]);
        let (engine, pipe) = engine_with(ResumableSource::new(log()));
        let stream = pipe.sources.stream().unwrap();
        Self {
            backend,
            store,
            shared_a,
            shared_b,
            checkpointer,
            engine,
            pipe,
            stream,
        }
    }

    /// Ingest one event and push each view's changelog into its sink.
    fn step(&mut self) {
        let event: SourceEvent = futures::executor::block_on(self.stream.next())
            .expect("an event")
            .expect("ok");
        ingest(&mut self.engine, &self.pipe.sources, &event);
        for (view, sink) in [(ROWS, &self.shared_a), (COUNT, &self.shared_b)] {
            let changes: ZSetBatch = self.engine.take_changes(view).unwrap();
            if !changes.is_empty() {
                futures::executor::block_on(sink.write_batch(changes)).unwrap();
            }
        }
    }

    fn take(&mut self) -> Result<u64, ConnectorError> {
        futures::executor::block_on(self.checkpointer.take(&self.engine, &self.pipe.sources))
    }

    /// Commit three events as checkpoint 1 and check both remote bags.
    fn valid_checkpoint(&mut self) {
        for _ in 0..3 {
            self.step();
        }
        assert_eq!(self.take().unwrap(), 1);
        assert_eq!(
            self.store.remote_bag(ROWS),
            bag(&self.engine.snapshot(ROWS).unwrap())
        );
        assert_eq!(
            self.store.remote_bag(COUNT),
            bag(&self.engine.snapshot(COUNT).unwrap())
        );
    }
}

#[test]
fn a_commit_failure_is_commit_uncertain_and_blocks() {
    let mut run = Run::new();
    run.valid_checkpoint();

    // The fourth event reaches both sinks, but the second commit fails with no
    // effect, so the engine view and its remote bag diverge.
    run.step();
    run.store.fail_before(COUNT, 1);
    let error = run.take().expect_err("the failed commit must surface");

    assert!(
        matches!(error, ConnectorError::Unsupported(_)),
        "got {error:?}"
    );
    assert_eq!(run.checkpointer.state(), CheckpointState::CommitUncertain);
    assert_eq!(
        run.backend.get(b"checkpoint/2/commit").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(run.backend.get(b"checkpoint/2/valid").unwrap(), None);
    assert_eq!(
        run.store.remote_bag(ROWS),
        bag(&run.engine.snapshot(ROWS).unwrap())
    );
    assert_ne!(
        run.store.remote_bag(COUNT),
        bag(&run.engine.snapshot(COUNT).unwrap())
    );
    assert!(
        run.take().is_err(),
        "a commit failure must block further attempts on this runtime"
    );
}

#[test]
fn public_recovery_rejects_corrupt_published_transaction_evidence_before_effects() {
    let mut failures = Vec::new();
    for (damage, body_damage, manifest_damage) in [
        ("missing-body", "missing", "valid"),
        ("corrupt-body", "corrupt", "valid"),
        ("missing-body-missing-manifest", "missing", "missing"),
        ("corrupt-body-corrupt-manifest", "corrupt", "corrupt"),
    ] {
        let backend = SharedBackend::default();
        let store = Store::new();
        let (mut engine, pipe) = engine_with(ResumableSource::new(log()));

        let mut no_sink_checkpointer = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
        assert_eq!(
            futures::executor::block_on(no_sink_checkpointer.take(&engine, &pipe.sources)).unwrap(),
            1
        );

        let rows_sink = SharedSink::new(Arc::new(TxnSink::new(ROWS, store.clone())));
        let count_sink = SharedSink::new(Arc::new(TxnSink::new(COUNT, store.clone())));
        let mut transaction = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN)
            .with_sinks(vec![
                SinkSync::sink_only_named(rows_sink.clone(), "rows".into(), ROWS.into()),
                SinkSync::sink_only_named(count_sink.clone(), "count".into(), COUNT.into()),
            ]);
        let mut stream = pipe.sources.stream().unwrap();
        for _ in 0..3 {
            let event = futures::executor::block_on(stream.next()).unwrap().unwrap();
            ingest(&mut engine, &pipe.sources, &event);
            for (view, sink) in [(ROWS, &rows_sink), (COUNT, &count_sink)] {
                let changes = engine.take_changes(view).unwrap();
                if !changes.is_empty() {
                    futures::executor::block_on(sink.write_batch(changes)).unwrap();
                }
            }
        }
        assert_eq!(
            futures::executor::block_on(transaction.take(&engine, &pipe.sources)).unwrap(),
            2
        );
        assert_eq!(store.remote_bag(ROWS), bag(&engine.snapshot(ROWS).unwrap()));

        let mut damaged = backend.clone();
        damaged.put(b"checkpoint/2/commit", b"1".to_vec()).unwrap();
        damaged
            .put(b"checkpoint/2/prepare", b"prepare-v1".to_vec())
            .unwrap();
        match body_damage {
            "missing" => damaged.delete(b"checkpoint/2/engine").unwrap(),
            "corrupt" => damaged
                .put(b"checkpoint/2/engine", b"bad".to_vec())
                .unwrap(),
            _ => unreachable!(),
        }
        match manifest_damage {
            "valid" => {}
            "missing" => damaged.delete(b"checkpoint/2/participants").unwrap(),
            "corrupt" => damaged
                .put(b"checkpoint/2/participants", b"bad".to_vec())
                .unwrap(),
            _ => unreachable!(),
        }
        let preserved: Vec<_> = [
            b"checkpoint/1/engine".as_slice(),
            b"checkpoint/1/sources",
            b"checkpoint/1/participants",
            b"checkpoint/2/engine",
            b"checkpoint/2/sources",
            b"checkpoint/2/participants",
            b"checkpoint/2/valid",
            b"checkpoint/2/prepare",
            b"checkpoint/2/commit",
            b"checkpoint/latest",
        ]
        .iter()
        .map(|key| (key.to_vec(), damaged.get(key).unwrap()))
        .collect();

        let mut direct = Checkpointer::new(Box::new(damaged.clone()), DEFAULT_RETAIN);
        assert!(matches!(
            direct.newest_valid(),
            Err(ConnectorError::Unsupported(_))
        ));
        assert!(matches!(
            direct.read(2),
            Err(ConnectorError::Unsupported(_))
        ));
        assert!(matches!(
            futures::executor::block_on(direct.promote(2)),
            Err(ConnectorError::Unsupported(_))
        ));

        let spy = Arc::new(spy::SpySource::new(Arc::new(ResumableSource::new(log()))));
        let sources = sources_of(spy.clone());
        let (mut restart, restart_pipe) = engine_for(sources);
        let mut current = Checkpointer::new(Box::new(backend.clone()), DEFAULT_RETAIN);
        let signal = Mutex::new(None);
        let result = futures::executor::block_on(Recovery::start(
            &mut restart,
            &restart_pipe.sources,
            &mut current,
            &signal,
            &hotlap_engine::MetricsRegistry::new(),
        ));

        if !matches!(result, Err(ConnectorError::Unsupported(_))) {
            failures.push(format!("{damage} returned without Unsupported"));
        }
        if spy.reads() != 0 {
            failures.push(format!("{damage} read the source {} times", spy.reads()));
        }
        if spy.resumed() != 0 {
            failures.push(format!(
                "{damage} resumed the source {} times",
                spy.resumed()
            ));
        }
        assert_eq!(spy.offset(), None);
        assert_eq!(store.remote_bag(ROWS), bag(&engine.snapshot(ROWS).unwrap()));
        for (key, value) in preserved {
            assert_eq!(damaged.get(&key).unwrap(), value, "{damage}: {key:?}");
        }
    }
    assert!(failures.is_empty(), "recovery effects: {failures:?}");
}

#[test]
fn a_replacement_store_with_the_same_view_is_rejected_before_transaction_redrive() {
    let mut run = Run::new();
    run.valid_checkpoint();
    run.step();
    run.store.fail_after(ROWS, 1);
    assert!(run.take().is_err());
    assert_eq!(
        run.backend.get(b"checkpoint/2/commit").unwrap(),
        Some(b"1".to_vec())
    );
    let original_bag = run.store.remote_bag(ROWS);
    assert_eq!(original_bag.get(&vec![3]), Some(&1));

    let replacement = Store::new();
    let replacement_rows = SharedSink::new(Arc::new(TxnSink::new(ROWS, replacement.clone())));
    let replacement_count = SharedSink::new(Arc::new(TxnSink::new(COUNT, replacement.clone())));
    let spy = Arc::new(spy::SpySource::new(Arc::new(ResumableSource::new(log()))));
    let sources = sources_of(spy.clone());
    let (mut restart, pipeline) = engine_for(sources);
    let mut checkpointer = Checkpointer::new(Box::new(run.backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![
            SinkSync::sink_only_named(replacement_rows, "rows".into(), ROWS.into()),
            SinkSync::sink_only_named(replacement_count, "count".into(), COUNT.into()),
        ]);
    let before = run.backend.get(b"checkpoint/2/commit").unwrap();
    let result = futures::executor::block_on(Recovery::start(
        &mut restart,
        &pipeline.sources,
        &mut checkpointer,
        &Mutex::new(None),
        &hotlap_engine::MetricsRegistry::new(),
    ));

    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_eq!(spy.reads(), 0);
    assert_eq!(spy.resumed(), 0);
    assert_eq!(replacement.remote_bag(ROWS), Default::default());
    assert_eq!(run.store.remote_bag(ROWS), original_bag);
    assert_eq!(run.backend.get(b"checkpoint/2/commit").unwrap(), before);
    assert!(run.backend.get(b"checkpoint/2/valid").unwrap().is_none());
}

#[test]
fn recovery_rejects_a_saved_untapped_sink_view_before_redrive() {
    let mut run = Run::new();
    run.valid_checkpoint();
    run.step();
    run.store.fail_after(ROWS, 1);
    assert!(run.take().is_err());
    let engine_key = b"checkpoint/2/engine";
    let encoded = run.backend.get(engine_key).unwrap().unwrap();
    let mut snapshot = hotlap_engine::decode_snapshot(&encoded).unwrap();
    let rows_id = run
        .engine
        .view_registry()
        .into_iter()
        .find(|(name, _, _)| name == ROWS)
        .unwrap()
        .1;
    snapshot
        .views
        .iter_mut()
        .find(|view| view.id == rows_id)
        .unwrap()
        .tapped = false;
    let corrupted = hotlap_engine::encode_snapshot(&snapshot).unwrap();
    run.backend.clone().put(engine_key, corrupted).unwrap();
    let commit_before = run.backend.get(b"checkpoint/2/commit").unwrap();
    let store_before = run.store.remote_bag(ROWS);

    let spy = Arc::new(spy::SpySource::new(Arc::new(ResumableSource::new(log()))));
    let sources = sources_of(spy.clone());
    let (mut restart, pipeline) = engine_for(sources);
    let rows_sink = SharedSink::new(Arc::new(TxnSink::new(ROWS, run.store.clone())));
    let count_sink = SharedSink::new(Arc::new(TxnSink::new(COUNT, run.store.clone())));
    let mut checkpointer = Checkpointer::new(Box::new(run.backend.clone()), DEFAULT_RETAIN)
        .with_sinks(vec![
            SinkSync::sink_only_named(rows_sink, "rows".into(), ROWS.into()),
            SinkSync::sink_only_named(count_sink, "count".into(), COUNT.into()),
        ]);
    let result = futures::executor::block_on(Recovery::start(
        &mut restart,
        &pipeline.sources,
        &mut checkpointer,
        &Mutex::new(None),
        &hotlap_engine::MetricsRegistry::new(),
    ));

    assert!(matches!(result, Err(ConnectorError::Unsupported(_))));
    assert_eq!(spy.reads(), 0);
    assert_eq!(spy.resumed(), 0);
    assert_eq!(run.store.remote_bag(ROWS), store_before);
    assert_eq!(
        run.backend.get(b"checkpoint/2/commit").unwrap(),
        commit_before
    );
    assert!(run.backend.get(b"checkpoint/2/valid").unwrap().is_none());
}
