// SPDX-License-Identifier: Apache-2.0
//! Real writer saturation/ACK/metadata faults in an owned Docker fixture.

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use fluss::client::{FlussAdmin, FlussConnection};
use fluss::config::Config;
use fluss::metadata::{
    AddColumn, AlterTableChanges, ColumnPositionType, DataTypes, JsonSerde, PartitionSpec,
    Schema as FlussSchema, TableDescriptor, TablePath,
};
use fluss::rpc::message::OffsetSpec;
use fluss_datafusion::{
    FlussKvTable, FlussLogTable, FlussWriteBatchOutcome, FlussWriteOptions, FlussWritePhase,
    FlussWriteProgress, FlussWriteSummary, FlussWriteTermination, FlussWriteTimeout,
};
use fluss_test_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
use futures::FutureExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type Feed = mpsc::Sender<Result<RecordBatch>>;

/// Observe a real pool admission after metadata completed, then pause the
/// owned server before releasing the blocking enqueue. This delegates every
/// allocation decision to DataFusion's GreedyMemoryPool; it is a test probe.
#[derive(Debug)]
struct EncodingGate {
    armed: AtomicBool,
    skip: AtomicUsize,
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    wake: std::sync::Condvar,
}
impl EncodingGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicBool::new(false),
            skip: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(true),
            wake: std::sync::Condvar::new(),
        })
    }
    fn arm(self: &Arc<Self>) -> ReleaseGate {
        self.arm_after(0)
    }
    fn arm_after(self: &Arc<Self>, skip: usize) -> ReleaseGate {
        *self.released.lock().unwrap() = false;
        self.skip.store(skip, Ordering::Release);
        self.armed.store(true, Ordering::Release);
        ReleaseGate(Arc::clone(self))
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
    fn observe(&self, reservation: &MemoryReservation) {
        if reservation.consumer().name() == "FlussWriteEncoded"
            && self.armed.load(Ordering::Acquire)
            && self
                .skip
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |skip| {
                    skip.checked_sub(1)
                })
                .is_ok()
        {
            return;
        }
        if reservation.consumer().name() == "FlussWriteEncoded"
            && self.armed.swap(false, Ordering::AcqRel)
        {
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
        }
    }
}
struct ReleaseGate(Arc<EncodingGate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}
#[derive(Debug)]
struct ObservedPool {
    inner: GreedyMemoryPool,
    gate: Arc<EncodingGate>,
}
impl std::fmt::Display for ObservedPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}
impl MemoryPool for ObservedPool {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(&self, reservation: &MemoryReservation, bytes: usize) {
        self.inner.grow(reservation, bytes);
    }
    fn shrink(&self, reservation: &MemoryReservation, bytes: usize) {
        self.inner.shrink(reservation, bytes);
    }
    fn try_grow(&self, reservation: &MemoryReservation, bytes: usize) -> Result<()> {
        self.inner.try_grow(reservation, bytes)?;
        self.gate.observe(reservation);
        Ok(())
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

#[derive(Debug)]
struct FeedPartition {
    schema: SchemaRef,
    receiver: Mutex<Option<mpsc::Receiver<Result<RecordBatch>>>>,
}
impl PartitionStream for FeedPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .expect("one execution per fixture feed");
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::unfold(receiver, |mut receiver| async move {
                receiver.recv().await.map(|item| (item, receiver))
            }),
        ))
    }
}

fn batch(start: i32, rows: usize, width: usize) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(
                (start..start + rows as i32).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from(vec!["x".repeat(width); rows])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn one_value(id: i32, value: &str) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(vec![value])) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn ready_partition(
    admin: &FlussAdmin,
    path: &TablePath,
    name: &str,
    minimum: i64,
) -> TestResult<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if admin
                .list_partition_offsets(path, name, &[0], OffsetSpec::Latest)
                .await
                .is_ok_and(|offsets| offsets[&0] >= minimum)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    Ok(())
}

async fn context(
    connection: Arc<FlussConnection>,
    path: &TablePath,
    kv: bool,
    options: FlussWriteOptions,
) -> TestResult<(SessionContext, Feed, Arc<dyn MemoryPool>, Arc<EncodingGate>)> {
    let gate = EncodingGate::new();
    let pool: Arc<dyn MemoryPool> = Arc::new(ObservedPool {
        inner: GreedyMemoryPool::new(16 * 1024 * 1024),
        gate: Arc::clone(&gate),
    });
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(1),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        ),
    );
    if kv {
        ctx.register_table(
            "sink",
            Arc::new(
                FlussKvTable::open(connection, path.clone(), Duration::from_secs(20))
                    .await?
                    .with_write_options(options)?,
            ),
        )?;
    } else {
        ctx.register_table(
            "sink",
            Arc::new(
                FlussLogTable::open(connection, path.clone(), Duration::from_secs(20))
                    .await?
                    .with_write_options(options)?,
            ),
        )?;
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("value", DataType::Utf8, true),
    ]));
    let (sender, receiver) = mpsc::channel(2);
    let partition = Arc::new(FeedPartition {
        schema: Arc::clone(&schema),
        receiver: Mutex::new(Some(receiver)),
    });
    ctx.register_table(
        "feed",
        Arc::new(StreamingTable::try_new(schema, vec![partition])?.with_infinite_table(true)),
    )?;
    Ok((ctx, sender, pool, gate))
}

async fn start(
    ctx: &SessionContext,
) -> TestResult<tokio::task::JoinHandle<Result<Vec<RecordBatch>>>> {
    let query = ctx
        .sql("INSERT INTO sink SELECT id, value FROM feed")
        .await?;
    Ok(tokio::spawn(async move { query.collect().await }))
}

async fn observe(
    ctx: &SessionContext,
    kv: bool,
) -> TestResult<tokio::sync::broadcast::Receiver<FlussWriteProgress>> {
    let provider = ctx.table_provider("sink").await?;
    let provider = provider.as_ref() as &dyn std::any::Any;
    Ok(if kv {
        provider
            .downcast_ref::<FlussKvTable>()
            .unwrap()
            .subscribe_writes()
    } else {
        provider
            .downcast_ref::<FlussLogTable>()
            .unwrap()
            .subscribe_writes()
    })
}

async fn confirmation(
    receiver: &mut tokio::sync::broadcast::Receiver<FlussWriteProgress>,
    total: u64,
) -> TestResult<u64> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match receiver.recv().await? {
                FlussWriteProgress::BatchOutcome {
                    execution_id,
                    outcome: FlussWriteBatchOutcome::Confirmed,
                    counts,
                    ..
                } if counts.confirmed == total => {
                    break Ok::<_, Box<dyn std::error::Error>>(execution_id);
                }
                FlussWriteProgress::Terminated(summary) => {
                    panic!("terminated before expected ACK: {summary:?}")
                }
                _ => {}
            }
        }
    })
    .await?
}

async fn summary(
    receiver: &mut tokio::sync::broadcast::Receiver<FlussWriteProgress>,
) -> TestResult<FlussWriteSummary> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let FlussWriteProgress::Terminated(summary) = receiver.recv().await? {
                break Ok::<_, Box<dyn std::error::Error>>(summary);
            }
        }
    })
    .await?
}

fn metric(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, name: &str) -> usize {
    let metrics = plan.metrics().expect("native sink metrics");
    metrics
        .iter()
        .find(|metric| metric.value().name() == name)
        .unwrap_or_else(|| panic!("missing {name}"))
        .value()
        .as_usize()
}

async fn start_with_plan(
    ctx: &SessionContext,
) -> TestResult<(
    tokio::task::JoinHandle<Result<Vec<RecordBatch>>>,
    Arc<dyn datafusion::physical_plan::ExecutionPlan>,
)> {
    let query = ctx
        .sql("INSERT INTO sink SELECT id, value FROM feed")
        .await?;
    let task = Arc::new(query.task_ctx());
    let plan = query.create_physical_plan().await?;
    let running_plan = Arc::clone(&plan);
    Ok((
        tokio::spawn(async move { datafusion::physical_plan::collect(running_plan, task).await }),
        plan,
    ))
}

async fn latest(admin: &FlussAdmin, path: &TablePath, minimum: i64) -> TestResult<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if admin
                .list_offsets(path, &[0], OffsetSpec::Latest)
                .await
                .is_ok_and(|offsets| offsets[&0] >= minimum)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| format!("warm-up ACK did not reach offset {minimum}"))?;
    Ok(())
}

async fn released(pool: &Arc<dyn MemoryPool>) -> TestResult<()> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while pool.reserved() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| format!("pool cleanup retained {} bytes", pool.reserved()))?;
    Ok(())
}

async fn idle(pool: &Arc<dyn MemoryPool>, kv: bool) -> TestResult<()> {
    // Routing entries, and KV encoder scratch, legitimately remain while the
    // writer is idle. Queued builder reservations exceed this small bound.
    tokio::time::timeout(Duration::from_secs(3), async {
        while pool.reserved() >= 32 * 1024 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(
        pool.reserved() > 0,
        "idle writer metadata/scratch remains accounted (kv={kv})"
    );
    Ok(())
}

fn assert_timeout(error: DataFusionError, phase: FlussWritePhase) {
    match error {
        DataFusionError::External(cause) => assert_eq!(
            cause
                .downcast_ref::<FlussWriteTimeout>()
                .expect("typed write timeout")
                .phase,
            phase
        ),
        other => panic!("unexpected error: {other}"),
    }
}

async fn cases(
    cluster: &FlussTestingCluster,
    snapshotter: &metrics_util::debugging::Snapshotter,
) -> TestResult<()> {
    let ready = cluster.get_fluss_connection().await;
    ready.close(Duration::from_secs(1)).await?;
    let connection = Arc::new(
        FlussConnection::new(Config {
            bootstrap_servers: cluster.plaintext_bootstrap_servers().into(),
            writer_buffer_memory_size: 64 * 1024,
            writer_batch_size: 32 * 1024,
            writer_dynamic_batch_size_min: 16 * 1024,
            writer_dynamic_batch_size_enabled: false,
            writer_request_max_size: 64 * 1024,
            writer_buffer_wait_timeout_ms: 10_000,
            writer_retries: 3,
            ..Default::default()
        })
        .await?,
    );
    wait_for_tablet(&connection).await?;
    let admin = connection.get_admin()?;
    admin.create_database("writer_pressure", None, true).await?;
    for kv in [false, true] {
        eprintln!("writer fault cases: kv={kv}");
        let path = TablePath::new("writer_pressure", if kv { "kv" } else { "log" });
        let schema = FlussSchema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string());
        let schema = if kv {
            schema.primary_key(vec!["id"])?
        } else {
            schema
        };
        admin
            .create_table(
                &path,
                &TableDescriptor::builder()
                    .schema(schema.build()?)
                    .distributed_by(Some(1), vec!["id".into()])
                    .build()?,
                false,
            )
            .await?;

        // Warm this execution's native writer ID and ACK before pausing the
        // server. Later faults exercise writes, not only writer initialization.
        let options = FlussWriteOptions {
            ack_timeout: Duration::from_secs(2),
            preparation_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let (ctx, feed, pool, gate) = context(Arc::clone(&connection), &path, kv, options).await?;
        let mut writes = observe(&ctx, kv).await?;
        let (ongoing, write_plan) = start_with_plan(&ctx).await?;
        feed.send(Ok(batch(1, 1, 1))).await?;
        let write_id = confirmation(&mut writes, 1).await?;
        latest(&admin, &path, 1).await?;
        eprintln!("warm ACK kv={kv}, reserved={}", pool.reserved());
        idle(&pool, kv).await?;
        tokio::time::sleep(Duration::from_millis(2300)).await;
        assert!(!ongoing.is_finished(), "idle after ACK must not expire");
        let (survivor, survivor_feed, survivor_pool, _) =
            context(Arc::clone(&connection), &path, kv, options).await?;
        let release = gate.arm();
        feed.send(Ok(batch(100, 256, 4096))).await?;
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()).await?;
        eprintln!("kv={kv}: entered encoder gate");
        cluster.pause_tablet_server(0).await?;
        drop(release);
        eprintln!("kv={kv}: tablet paused, encoder released");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let exhausted = snapshotter.snapshot().into_vec().into_iter().any(|(key, _, _, value)|
                    key.key().name() == "fluss.client.writer.buffer_available_bytes" && matches!(value, metrics_util::debugging::DebugValue::Gauge(value) if value.into_inner() == 0.0)
                );
                if exhausted && pool.reserved() >= 1024 * 1024 { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.map_err(|_| "writer did not reach full client buffer")?;
        eprintln!("kv={kv}: buffer full");
        assert_eq!(metric(&write_plan, "fluss_write_active_executions"), 1);
        assert_eq!(metric(&write_plan, "fluss_write_pending_operations"), 256);
        assert!(metric(&write_plan, "fluss_write_encoded_bytes") > 0);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!ongoing.is_finished());
        let surviving = start(&survivor).await?;
        survivor_feed.send(Ok(batch(9001, 1, 1))).await?;
        drop(survivor_feed);
        ongoing.abort();
        eprintln!("kv={kv}: awaiting cancellation");
        assert!(ongoing.await.unwrap_err().is_cancelled());
        released(&pool).await?;
        let cancelled = summary(&mut writes).await?;
        assert_eq!(cancelled.execution_id, write_id);
        assert_eq!(cancelled.status, FlussWriteTermination::Cancelled);
        assert_eq!(cancelled.counts.confirmed, 1);
        assert_eq!(cancelled.counts.received, 257);
        assert_eq!(cancelled.counts.uncertain, 256);
        assert_eq!(cancelled.counts.pending, 0);
        assert_eq!(metric(&write_plan, "fluss_write_confirmed_operations"), 1);
        assert_eq!(metric(&write_plan, "fluss_write_uncertain_operations"), 256);
        assert_eq!(metric(&write_plan, "fluss_write_pending_operations"), 0);
        assert_eq!(metric(&write_plan, "fluss_write_active_executions"), 0);
        for name in [
            "fluss_write_retained_arrow_bytes",
            "fluss_write_encoded_bytes",
            "fluss_write_transport_bytes",
            "fluss_write_routing_metadata_bytes",
            "fluss_write_routing_scratch_bytes",
            "fluss_write_kv_scratch_bytes",
        ] {
            assert_eq!(metric(&write_plan, name), 0, "released owner: {name}");
        }
        eprintln!("kv={kv}: cancelled writer resources released");
        assert!(
            feed.send(Ok(batch(9000, 1, 1))).await.is_err(),
            "cancel closes input"
        );
        assert!(
            !surviving.is_finished(),
            "cancelling one private writer must not fail its concurrent peer"
        );
        cluster.resume_tablet_server(0).await?;
        eprintln!("kv={kv}: tablet resumed, awaiting peer");
        assert!(
            !tokio::time::timeout(Duration::from_secs(10), surviving)
                .await???
                .is_empty()
        );
        released(&survivor_pool).await?;
        let survived = survivor
            .sql("SELECT id FROM sink WHERE id = 9001")
            .await?
            .collect()
            .await?;
        assert_eq!(survived.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        drop(survived);

        // A fresh private execution remains usable after the cancelled one.
        let (fresh, input, fresh_pool, _) =
            context(Arc::clone(&connection), &path, kv, options).await?;
        let mut fresh_writes = observe(&fresh, kv).await?;
        let completed = start(&fresh).await?;
        input.send(Ok(batch(9002, 1, 1))).await?;
        drop(input);
        assert!(
            !tokio::time::timeout(Duration::from_secs(10), completed)
                .await???
                .is_empty()
        );
        released(&fresh_pool).await?;
        let completed_summary = summary(&mut fresh_writes).await?;
        assert_eq!(completed_summary.status, FlussWriteTermination::Completed);
        assert_eq!(completed_summary.counts.confirmed, 1);
        assert_eq!(completed_summary.counts.uncertain, 0);
        assert!(completed_summary.input_exhausted);
        assert_ne!(completed_summary.execution_id, write_id);
        eprintln!("kv={kv}: concurrent and restarted writes passed");
        let found = fresh
            .sql("SELECT id FROM sink WHERE id = 9002")
            .await?
            .collect()
            .await?;
        assert_eq!(found.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        drop(found);

        // ACK timeout, with a singleton fully enqueued; no final SQL success.
        let ack_options = FlussWriteOptions {
            ack_timeout: Duration::from_millis(500),
            ..options
        };
        let (ack_ctx, ack_feed, ack_pool, ack_gate) =
            context(Arc::clone(&connection), &path, kv, ack_options).await?;
        let mut ack_writes = observe(&ack_ctx, kv).await?;
        let ack = start(&ack_ctx).await?;
        ack_feed.send(Ok(batch(9100, 1, 1))).await?;
        confirmation(&mut ack_writes, 1).await?;
        tokio::time::sleep(Duration::from_millis(250)).await;
        idle(&ack_pool, kv).await?;
        let release = ack_gate.arm();
        ack_feed.send(Ok(batch(9101, 1, 1))).await?;
        tokio::time::timeout(Duration::from_secs(1), ack_gate.entered.notified()).await?;
        cluster.pause_tablet_server(0).await?;
        drop(release);
        let error = tokio::time::timeout(Duration::from_secs(3), ack)
            .await??
            .unwrap_err();
        assert_timeout(error, FlussWritePhase::EnqueueAndAck);
        released(&ack_pool).await?;
        let uncertain = summary(&mut ack_writes).await?;
        assert_eq!(uncertain.status, FlussWriteTermination::Failed);
        assert_eq!(uncertain.counts.confirmed, 1);
        assert_eq!(uncertain.counts.uncertain, 1);
        assert_eq!(uncertain.counts.rejected_before_enqueue, 0);
        cluster.resume_tablet_server(0).await?;

        eprintln!("kv={kv}: ACK timeout passed");

        // Explicit cancellation of an enqueued singleton awaiting ACK, as
        // distinct from the producer blocked on buffer admission above.
        let (cancel_ctx, cancel_feed, cancel_pool, cancel_gate) =
            context(Arc::clone(&connection), &path, kv, options).await?;
        let mut cancel_writes = observe(&cancel_ctx, kv).await?;
        let cancel_ack = start(&cancel_ctx).await?;
        cancel_feed.send(Ok(batch(9150, 1, 1))).await?;
        confirmation(&mut cancel_writes, 1).await?;
        let release = cancel_gate.arm();
        cancel_feed.send(Ok(batch(9151, 1, 1))).await?;
        tokio::time::timeout(Duration::from_secs(1), cancel_gate.entered.notified()).await?;
        cluster.pause_tablet_server(0).await?;
        drop(release);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!cancel_ack.is_finished());
        cancel_ack.abort();
        assert!(cancel_ack.await.unwrap_err().is_cancelled());
        let cancelled_ack = summary(&mut cancel_writes).await?;
        assert_eq!(cancelled_ack.status, FlussWriteTermination::Cancelled);
        assert_eq!(cancelled_ack.stage, fluss_datafusion::FlussWriteStage::Ack);
        assert_eq!(cancelled_ack.counts.confirmed, 1);
        assert_eq!(cancelled_ack.counts.uncertain, 1);
        released(&cancel_pool).await?;
        cluster.resume_tablet_server(0).await?;
        eprintln!("kv={kv}: explicit cancellation waiting for ACK passed");

        // A stalled metadata RPC gets a distinct finite between-batch scope.
        let (metadata_ctx, metadata_feed, metadata_pool, _) = context(
            Arc::clone(&connection),
            &path,
            kv,
            FlussWriteOptions {
                preparation_timeout: Duration::from_millis(500),
                ..options
            },
        )
        .await?;
        let metadata = start(&metadata_ctx).await?;
        metadata_feed.send(Ok(batch(9200, 1, 1))).await?;
        tokio::time::sleep(Duration::from_millis(400)).await;
        idle(&metadata_pool, kv).await?;
        cluster.pause_tablet_server(0).await?;
        metadata_feed.send(Ok(batch(9201, 1, 1))).await?;
        let error = tokio::time::timeout(Duration::from_secs(3), metadata)
            .await??
            .unwrap_err();
        assert_timeout(error, FlussWritePhase::Metadata);
        released(&metadata_pool).await?;
        cluster.resume_tablet_server(0).await?;
        eprintln!("kv={kv}: metadata timeout passed");

        // Preparation fails before a writer or any input reservation exists.
        let (prepare_ctx, prepare_feed, prepare_pool, _) = context(
            Arc::clone(&connection),
            &path,
            kv,
            FlussWriteOptions {
                preparation_timeout: Duration::from_millis(500),
                ..options
            },
        )
        .await?;
        cluster.pause_coordinator().await?;
        let preparation = start(&prepare_ctx).await?;
        prepare_feed.send(Ok(batch(9300, 1, 1))).await?;
        let error = tokio::time::timeout(Duration::from_secs(3), preparation)
            .await??
            .unwrap_err();
        assert_timeout(error, FlussWritePhase::Preparation);
        released(&prepare_pool).await?;
        cluster.resume_coordinator().await?;
        eprintln!("kv={kv}: preparation timeout passed");

        let (quota_ctx, quota_feed, quota_pool, _) = context(
            Arc::clone(&connection),
            &path,
            kv,
            FlussWriteOptions {
                max_retained_batch_bytes: 1,
                ..options
            },
        )
        .await?;
        let mut quota_writes = observe(&quota_ctx, kv).await?;
        let quota = start(&quota_ctx).await?;
        quota_feed.send(Ok(batch(9400, 1, 1))).await?;
        let error = tokio::time::timeout(Duration::from_secs(3), quota)
            .await??
            .unwrap_err();
        assert!(
            matches!(error, DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
        released(&quota_pool).await?;
        let rejected = summary(&mut quota_writes).await?;
        assert_eq!(rejected.status, FlussWriteTermination::Failed);
        assert_eq!(rejected.counts.rejected_before_enqueue, 1);
        assert_eq!(rejected.counts.uncertain, 0);
        let absent = quota_ctx
            .sql("SELECT id FROM sink WHERE id = 9400")
            .await?
            .collect()
            .await?;
        assert_eq!(absent.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        drop(absent);

        // Source failure after a real ACK fails the statement and keeps the
        // previously visible operation; no successful partial count is emitted.
        let (error_ctx, error_feed, error_pool, _) =
            context(Arc::clone(&connection), &path, kv, options).await?;
        let mut error_writes = observe(&error_ctx, kv).await?;
        let source_error = start(&error_ctx).await?;
        error_feed.send(Ok(batch(9500, 1, 1))).await?;
        confirmation(&mut error_writes, 1).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        idle(&error_pool, kv).await?;
        error_feed
            .send(Err(DataFusionError::Execution(
                "fixture input failed after ACK".into(),
            )))
            .await?;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), source_error)
                .await??
                .unwrap_err()
                .to_string()
                .contains("fixture input failed")
        );
        released(&error_pool).await?;
        let failed = summary(&mut error_writes).await?;
        assert_eq!(failed.status, FlussWriteTermination::Failed);
        assert_eq!(failed.counts.confirmed, 1);
        assert_eq!(failed.counts.uncertain, 0);
        assert!(!failed.input_exhausted);
        let confirmed = error_ctx
            .sql("SELECT id FROM sink WHERE id = 9500")
            .await?
            .collect()
            .await?;
        assert_eq!(
            confirmed.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1
        );
        drop(confirmed);

        // Dropping/recreating the same path while INSERT is open invalidates
        // its identity. The next source batch must not enter the new table.
        let (identity_ctx, identity_feed, identity_pool, _) =
            context(Arc::clone(&connection), &path, kv, options).await?;
        let identity = start(&identity_ctx).await?;
        identity_feed.send(Ok(batch(9600, 1, 1))).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        idle(&identity_pool, kv).await?;
        admin.drop_table(&path, false).await?;
        let schema = FlussSchema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string());
        let schema = if kv {
            schema.primary_key(vec!["id"])?
        } else {
            schema
        };
        admin
            .create_table(
                &path,
                &TableDescriptor::builder()
                    .schema(schema.build()?)
                    .distributed_by(Some(1), vec!["id".into()])
                    .build()?,
                false,
            )
            .await?;
        identity_feed.send(Ok(batch(9601, 1, 1))).await?;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), identity)
                .await??
                .unwrap_err()
                .to_string()
                .contains("identity or schema")
        );
        released(&identity_pool).await?;
        latest(&admin, &path, 0).await?;
        let (new_table, _, _, _) = context(Arc::clone(&connection), &path, kv, options).await?;
        let empty = new_table
            .sql("SELECT id FROM sink")
            .await?
            .collect()
            .await?;
        assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        drop(empty);
        eprintln!("kv={kv}: quota, input error, recreation passed");

        let mut unsupported = connection.config().clone();
        unsupported.writer_enable_idempotence = false;
        unsupported.writer_acks = "0".into();
        let (no_ack_ctx, no_ack_feed, no_ack_pool, _) = context(
            Arc::new(FlussConnection::new(unsupported).await?),
            &path,
            kv,
            options,
        )
        .await?;
        let no_ack = start(&no_ack_ctx).await?;
        no_ack_feed.send(Ok(batch(9700, 1, 1))).await?;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), no_ack)
                .await??
                .unwrap_err()
                .to_string()
                .contains("writer_acks")
        );
        released(&no_ack_pool).await?;

        let mut ack_one = connection.config().clone();
        ack_one.writer_enable_idempotence = false;
        ack_one.writer_acks = "1".into();
        let (one_ctx, one_feed, one_pool, _) = context(
            Arc::new(FlussConnection::new(ack_one).await?),
            &path,
            kv,
            options,
        )
        .await?;
        let one = start(&one_ctx).await?;
        one_feed.send(Ok(batch(9701, 1, 1))).await?;
        drop(one_feed);
        assert!(
            !tokio::time::timeout(Duration::from_secs(3), one)
                .await???
                .is_empty()
        );
        released(&one_pool).await?;
        let visible = one_ctx.sql("SELECT id FROM sink").await?.collect().await?;
        assert_eq!(
            visible.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1,
            "ACK0 rejected without writing; ACK1 confirmed one operation"
        );
        drop(visible);
        eprintln!("kv={kv}: ACK policies passed");

        let partitioned = TablePath::new(
            "writer_pressure",
            if kv {
                "partitioned_kv"
            } else {
                "partitioned_log"
            },
        );
        let fields = FlussSchema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string());
        let fields = if kv {
            fields.primary_key(vec!["value", "id"])?
        } else {
            fields
        };
        admin
            .create_table(
                &partitioned,
                &TableDescriptor::builder()
                    .schema(fields.build()?)
                    .partitioned_by(vec!["value"])
                    .distributed_by(Some(1), vec!["id".into()])
                    .build()?,
                false,
            )
            .await?;
        let spec =
            |name: &str| PartitionSpec::new(std::collections::HashMap::from([("value", name)]));
        admin
            .create_partition(&partitioned, &spec("x"), false)
            .await?;
        ready_partition(&admin, &partitioned, "x", 0).await?;
        let (part_ctx, part_feed, part_pool, _) =
            context(Arc::clone(&connection), &partitioned, kv, options).await?;
        let part = start(&part_ctx).await?;
        part_feed.send(Ok(one_value(1, "x"))).await?;
        ready_partition(&admin, &partitioned, "x", 1).await?;
        idle(&part_pool, kv).await?;
        admin
            .create_partition(&partitioned, &spec("y"), false)
            .await?;
        ready_partition(&admin, &partitioned, "y", 0).await?;
        part_feed.send(Ok(one_value(2, "y"))).await?;
        ready_partition(&admin, &partitioned, "y", 1).await?;
        idle(&part_pool, kv).await?;
        admin
            .drop_partition(&partitioned, &spec("x"), false)
            .await?;
        admin
            .create_partition(&partitioned, &spec("x"), false)
            .await?;
        ready_partition(&admin, &partitioned, "x", 0).await?;
        part_feed.send(Ok(one_value(3, "x"))).await?;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), part)
                .await??
                .unwrap_err()
                .to_string()
                .contains("partition identity/layout changed")
        );
        released(&part_pool).await?;
        let recreated = admin
            .list_partition_offsets(&partitioned, "x", &[0], OffsetSpec::Latest)
            .await?;
        assert_eq!(
            recreated[&0], 0,
            "old execution did not write into the recreated partition"
        );
        eprintln!("kv={kv}: partition topology passed");
        let (schema_ctx, schema_feed, schema_pool, _) =
            context(Arc::clone(&connection), &path, kv, options).await?;
        let schema_write = start(&schema_ctx).await?;
        schema_feed.send(Ok(batch(9800, 1, 1))).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        idle(&schema_pool, kv).await?;
        admin
            .alter_table(
                &path,
                false,
                AlterTableChanges {
                    add_columns: vec![AddColumn {
                        column_name: "extra".into(),
                        data_type_json: serde_json::to_vec(&DataTypes::string().serialize_json()?)?,
                        comment: None,
                        position: ColumnPositionType::Last,
                    }],
                    ..Default::default()
                },
            )
            .await?;
        schema_feed.send(Ok(batch(9801, 1, 1))).await?;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), schema_write)
                .await??
                .unwrap_err()
                .to_string()
                .contains("identity or schema")
        );
        released(&schema_pool).await?;
        let (schema_new, _, _, _) = context(Arc::clone(&connection), &path, kv, options).await?;
        let checked = schema_new
            .sql("SELECT id FROM sink WHERE id = 9801")
            .await?
            .collect()
            .await?;
        assert_eq!(checked.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        eprintln!("kv={kv}: live schema invalidation passed");
    }
    delete_cases(&connection, cluster).await?;
    merge_cases(&connection, cluster).await?;
    connection.close(Duration::from_secs(2)).await?;
    Ok(())
}

async fn delete_cases(
    connection: &Arc<FlussConnection>,
    cluster: &FlussTestingCluster,
) -> TestResult<()> {
    let path = TablePath::new("writer_pressure", "delete_contract");
    let admin = connection.get_admin()?;
    admin
        .create_table(
            &path,
            &TableDescriptor::builder()
                .schema(
                    FlussSchema::builder()
                        .column("id", DataTypes::int())
                        .column("value", DataTypes::string())
                        .primary_key(vec!["id"])?
                        .build()?,
                )
                .distributed_by(Some(2), vec!["id".into()])
                .build()?,
            false,
        )
        .await?;
    let gate = EncodingGate::new();
    let pool: Arc<dyn MemoryPool> = Arc::new(ObservedPool {
        inner: GreedyMemoryPool::new(16 * 1024 * 1024),
        gate: Arc::clone(&gate),
    });
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(1),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        ),
    );
    let provider = FlussKvTable::open(
        Arc::clone(connection),
        path.clone(),
        Duration::from_secs(20),
    )
    .await?
    .with_write_options(FlussWriteOptions {
        ack_timeout: Duration::from_millis(800),
        ..Default::default()
    })?;
    ctx.register_table("sink", Arc::new(provider))?;
    ctx.sql("INSERT INTO sink VALUES (1, 'old'), (2, 'gone')")
        .await?
        .collect()
        .await?;

    // The selected snapshot matches 'old'. A concurrent upsert is not a
    // predicate-CAS: native key deletion removes the replacement value too.
    let release = gate.arm();
    let query = ctx
        .sql("DELETE FROM sink WHERE id = 1 AND value = 'old'")
        .await?;
    let selected = tokio::spawn(async move { query.collect().await });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()).await?;
    ctx.sql("INSERT INTO sink VALUES (1, 'replacement')")
        .await?
        .collect()
        .await?;
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(3), selected).await???;
    assert_eq!(
        result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap()
            .value(0),
        1
    );
    let absent = ctx
        .sql("SELECT id FROM sink WHERE id = 1")
        .await?
        .collect()
        .await?;
    assert_eq!(absent.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    drop(absent);

    // Count includes a selected key already deleted by a concurrent statement.
    let release = gate.arm();
    let query = ctx.sql("DELETE FROM sink WHERE id = 2").await?;
    let selected = tokio::spawn(async move { query.collect().await });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()).await?;
    ctx.sql("DELETE FROM sink WHERE id = 2")
        .await?
        .collect()
        .await?;
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(3), selected).await???;
    assert_eq!(
        result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap()
            .value(0),
        1
    );
    released(&pool).await?;

    // Two native snapshot bucket batches: the first ACK is known before a
    // blocked subsequent delete. No successful partial SQL count is emitted.
    let values = (100..164)
        .map(|id| format!("({id}, 'delete')"))
        .collect::<Vec<_>>()
        .join(",");
    ctx.sql(&format!("INSERT INTO sink VALUES {values}"))
        .await?
        .collect()
        .await?;
    let offsets = admin
        .list_offsets(&path, &[0, 1], OffsetSpec::Latest)
        .await?;
    assert!(
        offsets[&0] > 0 && offsets[&1] > 0,
        "exercise both snapshot buckets"
    );
    let mut writes = observe(&ctx, true).await?;
    let release = gate.arm_after(1);
    let query = ctx.sql("DELETE FROM sink WHERE id >= 100").await?;
    let deleting = tokio::spawn(async move { query.collect().await });
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified()).await?;
    let confirmed = loop {
        if let FlussWriteProgress::BatchOutcome {
            outcome: FlussWriteBatchOutcome::Confirmed,
            counts,
            ..
        } = writes.try_recv()?
        {
            break counts.confirmed;
        }
    };
    assert!(confirmed > 0 && confirmed < 64);
    cluster.pause_tablet_server(0).await?;
    drop(release);
    let error = tokio::time::timeout(Duration::from_secs(3), deleting)
        .await??
        .unwrap_err();
    assert_timeout(error, FlussWritePhase::EnqueueAndAck);
    let partial = summary(&mut writes).await?;
    assert_eq!(
        partial.operation,
        fluss_datafusion::FlussWriteOperation::Delete
    );
    assert_eq!(partial.status, FlussWriteTermination::Failed);
    assert_eq!(partial.counts.confirmed, confirmed);
    assert!(partial.counts.uncertain > 0);
    assert_eq!(
        partial.counts.received,
        partial.counts.confirmed + partial.counts.uncertain
    );
    released(&pool).await?;
    cluster.resume_tablet_server(0).await?;
    let remaining = ctx.sql("SELECT id FROM sink").await?.collect().await?;
    assert!(remaining.iter().map(RecordBatch::num_rows).sum::<usize>() <= 64 - confirmed as usize);
    eprintln!("DELETE selection/update/delete races and partial ACK failure passed");
    Ok(())
}

async fn merge_cases(
    connection: &Arc<FlussConnection>,
    cluster: &FlussTestingCluster,
) -> TestResult<()> {
    let path = TablePath::new("writer_pressure", "merge_contract");
    let admin = connection.get_admin()?;
    admin
        .create_table(
            &path,
            &TableDescriptor::builder()
                .schema(
                    FlussSchema::builder()
                        .column("id", DataTypes::int())
                        .column("value", DataTypes::string())
                        .primary_key(vec!["id"])?
                        .build()?,
                )
                .distributed_by(Some(1), vec!["id".into()])
                .build()?,
            false,
        )
        .await?;
    let gate = EncodingGate::new();
    let pool: Arc<dyn MemoryPool> = Arc::new(ObservedPool {
        inner: GreedyMemoryPool::new(16 * 1024 * 1024),
        gate: Arc::clone(&gate),
    });
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(1),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        ),
    );
    ctx.register_table(
        "sink",
        Arc::new(
            FlussKvTable::open(
                Arc::clone(connection),
                path.clone(),
                Duration::from_secs(20),
            )
            .await?
            .with_write_options(FlussWriteOptions {
                ack_timeout: Duration::from_millis(800),
                ..Default::default()
            })?,
        ),
    )?;
    ctx.sql("INSERT INTO sink VALUES (1, 'old')")
        .await?
        .collect()
        .await?;

    let release = gate.arm();
    let query = ctx.sql("MERGE INTO sink AS t USING (VALUES(1,'merge')) AS s(id,value) ON t.id=s.id WHEN MATCHED AND t.value='old' THEN UPDATE SET value=s.value").await?;
    let merging = tokio::spawn(async move { query.collect().await });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()).await?;
    ctx.sql("INSERT INTO sink VALUES (1,'concurrent')")
        .await?
        .collect()
        .await?;
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(3), merging).await???;
    assert_eq!(
        result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap()
            .value(0),
        1
    );
    let actual = ctx
        .sql("SELECT value FROM sink WHERE id=1")
        .await?
        .collect()
        .await?;
    assert_eq!(
        actual[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "merge",
        "snapshot predicate is not compare-and-set"
    );
    drop(actual);

    let release = gate.arm();
    let query = ctx.sql("MERGE INTO sink AS t USING (VALUES(2,'insert')) AS s(id,value) ON t.id=s.id WHEN NOT MATCHED THEN INSERT(id,value) VALUES(s.id,s.value)").await?;
    let merging = tokio::spawn(async move { query.collect().await });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()).await?;
    ctx.sql("INSERT INTO sink VALUES (2,'concurrent')")
        .await?
        .collect()
        .await?;
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(3), merging).await???;
    assert_eq!(
        result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap()
            .value(0),
        1
    );
    let actual = ctx
        .sql("SELECT value FROM sink WHERE id=2")
        .await?
        .collect()
        .await?;
    assert_eq!(
        actual[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "insert",
        "native upsert is not conditional INSERT"
    );
    drop(actual);
    released(&pool).await?;

    let values = (100..164)
        .map(|id| format!("({id},'partial')"))
        .collect::<Vec<_>>()
        .join(",");
    let query = ctx.sql(&format!("MERGE INTO sink AS t USING (VALUES {values}) AS s(id,value) ON t.id=s.id WHEN NOT MATCHED THEN INSERT(id,value) VALUES(s.id,s.value)")).await?;
    let task = Arc::new(query.task_ctx());
    let plan = query.create_physical_plan().await?;
    let running = Arc::clone(&plan);
    let mut writes = observe(&ctx, true).await?;
    let release = gate.arm_after(1);
    let merging =
        tokio::spawn(async move { datafusion::physical_plan::collect(running, task).await });
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified()).await?;
    let confirmed = loop {
        if let FlussWriteProgress::BatchOutcome {
            outcome: FlussWriteBatchOutcome::Confirmed,
            counts,
            ..
        } = writes.try_recv()?
        {
            break counts.confirmed;
        }
    };
    assert!(confirmed > 0 && confirmed < 64);
    assert!(
        metric(&plan, "fluss_write_merge_key_bytes") > 0,
        "persistent duplicate-key state is admitted and visible"
    );
    cluster.pause_tablet_server(0).await?;
    drop(release);
    assert_timeout(
        tokio::time::timeout(Duration::from_secs(3), merging)
            .await??
            .unwrap_err(),
        FlussWritePhase::EnqueueAndAck,
    );
    let partial = summary(&mut writes).await?;
    assert_eq!(
        partial.operation,
        fluss_datafusion::FlussWriteOperation::Merge
    );
    assert_eq!(partial.status, FlussWriteTermination::Failed);
    assert_eq!(partial.counts.confirmed, confirmed);
    assert!(partial.counts.uncertain > 0);
    assert_eq!(metric(&plan, "fluss_write_merge_key_bytes"), 0);
    assert_eq!(metric(&plan, "fluss_write_kv_scratch_bytes"), 0);
    // DataFusion's retained HashJoin build-side state may still own the target
    // snapshot buffers while this physical plan is held for metric inspection.
    // Those source leases remain charged; dropping only the query future is
    // not permission to free externally retained plan/operator buffers.
    drop(plan);
    released(&pool).await?;
    cluster.resume_tablet_server(0).await?;
    let actual = ctx
        .sql("SELECT id FROM sink WHERE id>=100")
        .await?
        .collect()
        .await?;
    let stored = actual.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert!(
        stored >= confirmed as usize && stored <= partial.counts.received as usize,
        "only submitted prefix can apply: {partial:?}, stored={stored}"
    );
    eprintln!("MERGE snapshot/update/insert races and partial ACK failure passed");
    Ok(())
}

async fn wait_for_tablet(connection: &FlussConnection) -> TestResult<()> {
    let metadata = connection.get_metadata();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            metadata
                .update_tables_metadata(
                    &std::collections::HashSet::new(),
                    &std::collections::HashSet::new(),
                    vec![],
                )
                .await?;
            if metadata.get_cluster().get_tablet_server(0).is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok::<_, fluss::error::Error>(())
    })
    .await??;
    Ok(())
}

async fn connection_loss_cases(
    connection: &Arc<FlussConnection>,
    cluster: &FlussTestingCluster,
) -> TestResult<()> {
    let admin = connection.get_admin()?;
    for kv in [false, true] {
        let path = TablePath::new(
            "writer_pressure",
            if kv {
                "connection_loss_kv"
            } else {
                "connection_loss_log"
            },
        );
        let fields = FlussSchema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string());
        let fields = if kv {
            fields.primary_key(vec!["id"])?
        } else {
            fields
        };
        admin
            .create_table(
                &path,
                &TableDescriptor::builder()
                    .schema(fields.build()?)
                    .distributed_by(Some(1), vec!["id".into()])
                    .build()?,
                false,
            )
            .await?;
        let options = FlussWriteOptions {
            ack_timeout: Duration::from_secs(2),
            preparation_timeout: Duration::from_secs(2),
            max_retries: 1,
            ..Default::default()
        };
        let (ctx, input, pool, gate) = context(Arc::clone(connection), &path, kv, options).await?;
        let mut writes = observe(&ctx, kv).await?;
        let ongoing = start(&ctx).await?;
        input.send(Ok(batch(9900, 1, 1))).await?;
        confirmation(&mut writes, 1).await?;
        eprintln!("kv={kv}: connection-loss warm ACK received");
        // The single-replica server checkpoints its high watermark every 5s.
        // ACK alone does not imply that checkpoint has reached disk. Give the
        // warm prefix a checkpoint window before crashing the process; the
        // next batch is still stopped before it can enter native encoding.
        tokio::time::sleep(Duration::from_secs(6)).await;
        let release = gate.arm();
        input.send(Ok(batch(9901, 1, 1))).await?;
        tokio::time::timeout(Duration::from_secs(1), gate.entered.notified()).await?;
        cluster.stop_tablet_server(0).await?;
        eprintln!("kv={kv}: tablet stopped and sockets closed");
        drop(release);
        let error = tokio::time::timeout(Duration::from_secs(4), ongoing)
            .await??
            .unwrap_err();
        // Native retry exhaustion can return its protocol/connection error; if
        // metadata loses the leader before another drain, the batch ACK bound
        // expires instead. Neither is a successful/complete SQL result.
        assert!(
            matches!(error, DataFusionError::External(_)),
            "native cause lost: {error}"
        );
        let partial = summary(&mut writes).await?;
        assert_eq!(partial.status, FlussWriteTermination::Failed);
        assert_eq!(partial.counts.confirmed, 1);
        assert_eq!(partial.counts.uncertain, 1);
        released(&pool).await?;
        cluster.start_tablet_server(0).await?;
        eprintln!("kv={kv}: tablet restarted, awaiting leader recovery");
        let fresh_connection = Arc::new(cluster.get_fluss_connection().await);
        let fresh_admin = fresh_connection.get_admin()?;
        let mut last = String::new();
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                match fresh_admin
                    .list_offsets(&path, &[0], OffsetSpec::Latest)
                    .await
                {
                    Ok(offsets) if offsets.get(&0).is_some_and(|offset| *offset >= 1) => break,
                    Ok(offsets) => last = format!("recovered offsets {offsets:?}"),
                    Err(error) => last = error.to_string(),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| format!("tablet/leader did not recover: {last}"))?;
        let (fresh, new_input, fresh_pool, _) =
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    match context(Arc::clone(&fresh_connection), &path, kv, options).await {
                        Ok(context) => break context,
                        Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                    }
                }
            })
            .await?;
        // The first ACKed row persists after real connection/leader loss. A
        // new execution succeeds; the failed one is never transparently resumed.
        let old = fresh
            .sql("SELECT id FROM sink WHERE id=9900")
            .await?
            .collect()
            .await?;
        assert_eq!(old.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        drop(old);
        let restarted = start(&fresh).await?;
        new_input.send(Ok(batch(9902, 1, 1))).await?;
        drop(new_input);
        assert!(
            !tokio::time::timeout(Duration::from_secs(5), restarted)
                .await???
                .is_empty()
        );
        let actual = fresh
            .sql("SELECT id FROM sink ORDER BY id")
            .await?
            .collect()
            .await?;
        let ids = actual
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![9900, 9902],
            "checkpointed prefix and new execution must be stored; the gated batch never reached the stopped server"
        );
        drop(actual);
        released(&fresh_pool).await?;
        eprintln!(
            "kv={kv}: real socket/leader loss preserves ACK prefix and new execution recovers"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Docker and compatible Fluss image; crashes only this owned fixture"]
async fn real_writer_connection_loss_and_recovery() -> TestResult<()> {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let mut builder = FlussTestingClusterBuilder::new(format!("df-write-recovery-{suffix}"))
        .with_port(20000 + (suffix % 20000) as u16);
    let cluster = builder.build().await;
    let result = std::panic::AssertUnwindSafe(async {
        let connection = Arc::new(cluster.get_fluss_connection().await);
        wait_for_tablet(&connection).await?;
        connection
            .get_admin()?
            .create_database("writer_pressure", None, true)
            .await?;
        tokio::time::timeout(
            Duration::from_secs(180),
            connection_loss_cases(&connection, &cluster),
        )
        .await
        .map_err(|_| "connection-loss matrix exceeded its 180s fixture deadline")?
    })
    .catch_unwind()
    .await;
    cluster.stop();
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Docker and compatible Fluss image; pauses only this owned fixture"]
async fn real_writer_saturation_ack_and_preparation_faults() -> TestResult<()> {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::set_global_recorder(recorder)?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let mut builder = FlussTestingClusterBuilder::new(format!("df-write-pressure-{suffix}"))
        .with_port(20000 + (suffix % 20000) as u16);
    let cluster = builder.build().await;
    let result = std::panic::AssertUnwindSafe(async {
        tokio::time::timeout(Duration::from_secs(90), cases(&cluster, &snapshotter))
            .await
            .map_err(|_| "writer fault matrix exceeded its 90s fixture deadline")?
    })
    .catch_unwind()
    .await;
    // A failed assertion must not leave a paused container behind. This only
    // affects the three containers created by this unique RAII fixture.
    let _ = cluster.resume_tablet_server(0).await;
    let _ = cluster.resume_coordinator().await;
    cluster.stop();
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
