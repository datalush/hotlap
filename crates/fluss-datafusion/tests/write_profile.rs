// SPDX-License-Identifier: Apache-2.0
//! Owned native continuous INSERT and projection profiles. Run alone.
#[path = "support/profile.rs"]
mod profile;
#[path = "support/profile_metrics.rs"]
mod profile_metrics;

use arrow::array::{Array, ArrayRef, Int32Array, StringArray, UInt64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use fluss::client::{FlussAdmin, FlussConnection};
use fluss::config::Config;
use fluss::metadata::{
    AlterTableChanges, DataTypes, PartitionSpec, Schema, TableDescriptor, TablePath,
};
use fluss::rpc::message::OffsetSpec;
use fluss_datafusion::LogReadOptions;
use fluss_datafusion::{
    FlussKvTable, FlussLogTable, FlussWriteBatchOutcome, FlussWriteOptions, FlussWriteProgress,
    FlussWriteTermination,
};
use fluss_test_cluster::FlussTestingClusterBuilder;
use futures::{FutureExt, StreamExt};
use profile_metrics::ProfileMetrics as Snapshotter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
const ROWS: usize = 128;
const WIDTH: usize = 4096;
const POOL: usize = 64 * 1024 * 1024;
const RSS_LIMIT: usize = 1536 * 1024 * 1024;

#[derive(Debug)]
struct Feed {
    schema: SchemaRef,
    receiver: Mutex<Option<mpsc::Receiver<DfResult<RecordBatch>>>>,
}
impl PartitionStream for Feed {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .expect("one profile execution");
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::unfold(receiver, |mut r| async move {
                r.recv().await.map(|batch| (batch, r))
            }),
        ))
    }
}
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn rss(field: &str) -> TestResult<usize> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    Ok(status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .ok_or("missing process memory field")?
        .split_whitespace()
        .next()
        .ok_or("missing memory value")?
        .parse::<usize>()?
        * 1024)
}
fn cpu_seconds() -> TestResult<f64> {
    static TICKS: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    let ticks = *TICKS.get_or_init(|| {
        let output = std::process::Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .expect("getconf CLK_TCK");
        String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    });
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields = stat
        .rsplit_once(')')
        .ok_or("invalid process stat")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    Ok((fields[11].parse::<u64>()? + fields[12].parse::<u64>()?) as f64 / ticks)
}
fn metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    plan.metrics().map_or(0, |set| {
        set.iter()
            .filter(|m| m.value().name() == name)
            .map(|m| m.value().as_usize())
            .sum::<usize>()
    }) + plan
        .children()
        .iter()
        .map(|child| metric(child, name))
        .sum::<usize>()
}
fn counter(snapshot: &Snapshotter, name: &str) -> u64 {
    snapshot.counter(name)
}
fn available(snapshot: &Snapshotter) -> usize {
    snapshot.gauge("fluss.client.writer.buffer_available_bytes")
}
fn batch(start: usize, interleaved: bool) -> RecordBatch {
    let ids = (start..start + ROWS)
        .map(|id| id as i32)
        .collect::<Vec<_>>();
    let regions = (0..ROWS)
        .map(|i| {
            if if interleaved {
                i % 2 == 0
            } else {
                i < ROWS / 2
            } {
                "north"
            } else {
                "west"
            }
        })
        .collect::<Vec<_>>();
    let values = ids
        .iter()
        .map(|id| format!("row-{id:010}-{}", "x".repeat(WIDTH - 15)))
        .collect::<Vec<_>>();
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        ("region", Arc::new(StringArray::from(regions)) as ArrayRef),
        ("value", Arc::new(StringArray::from(values)) as ArrayRef),
    ])
    .unwrap()
}
async fn partition(
    admin: &FlussAdmin,
    path: &TablePath,
    name: &str,
    buckets: i32,
) -> TestResult<()> {
    admin
        .create_partition(
            path,
            &PartitionSpec::new([("region", name)].into_iter().collect()),
            false,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if admin
                .list_partition_offsets(
                    path,
                    name,
                    &(0..buckets).collect::<Vec<_>>(),
                    OffsetSpec::Latest,
                )
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    Ok(())
}
async fn empty(pool: &Arc<dyn MemoryPool>) -> TestResult<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.reserved() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| format!("pool retains {} bytes", pool.reserved()))?;
    Ok(())
}
#[derive(Default)]
struct Peaks {
    rss: usize,
    pool: usize,
    retained: usize,
    encoded: usize,
    transport: usize,
    buffer_min: usize,
    waiting_threads: usize,
}

async fn stage(
    sender: &mpsc::Sender<DfResult<RecordBatch>>,
    observer: &mut tokio::sync::broadcast::Receiver<FlussWriteProgress>,
    next: &mut usize,
    interleaved: bool,
    seconds: u64,
) -> TestResult<(profile::Latencies, Duration)> {
    let began = Instant::now();
    let mut ticks = tokio::time::interval(Duration::from_millis(250));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut latencies = profile::Latencies::new(30_000);
    while began.elapsed() < Duration::from_secs(seconds) {
        ticks.tick().await;
        let started = Instant::now();
        sender.send(Ok(batch(*next, interleaved))).await?;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                match observer.recv().await? {
                    FlussWriteProgress::BatchOutcome {
                        outcome: FlussWriteBatchOutcome::Confirmed,
                        counts,
                        ..
                    } => {
                        assert_eq!(counts.confirmed, (*next + ROWS) as u64);
                        break;
                    }
                    FlussWriteProgress::Terminated(summary) => {
                        return Err(format!("unexpected terminal {summary:?}").into());
                    }
                    _ => {}
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await??;
        *next += ROWS;
        latencies.record(started.elapsed());
    }
    Ok((latencies, began.elapsed()))
}

async fn verify(
    ctx: &SessionContext,
    expected: usize,
    interleaved: bool,
    columns: &str,
    snapshot: &Snapshotter,
) -> TestResult<(usize, u64)> {
    let before = counter(snapshot, "fluss.client.bytes_received.total");
    let start = Instant::now();
    let cpu_start = cpu_seconds()?;
    let query = ctx.sql(&format!("SELECT {columns} FROM sink")).await?;
    let plan = query.create_physical_plan().await?;
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), Arc::new(query.task_ctx()))?;
    let mut seen = vec![false; expected];
    let mut counted = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if columns == "COUNT(*)" {
            counted = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0) as usize;
            continue;
        }
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let id = usize::try_from(ids.value(row))?;
            assert!(id < expected && !seen[id], "duplicate/out-of-range id {id}");
            seen[id] = true;
            counted += 1;
            if columns == "*" {
                let region = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row);
                let north = if interleaved {
                    id % 2 == 0
                } else {
                    id % ROWS < ROWS / 2
                };
                assert_eq!(region, if north { "north" } else { "west" });
                let value = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row);
                assert_eq!(value.len(), WIDTH);
                assert!(
                    value.starts_with(&format!("row-{id:010}-"))
                        && value[15..].bytes().all(|b| b == b'x')
                );
            }
        }
    }
    assert_eq!(counted, expected);
    if columns != "COUNT(*)" {
        assert!(seen.iter().all(|n| *n));
    }
    let decoded = metric(&plan, "arrow_decoded_bytes");
    if columns == "COUNT(*)" {
        assert_eq!(decoded, 0);
    }
    eprintln!(
        "projection columns={columns} rows={expected} elapsed_ms={} arrow_decoded_bytes={decoded} arrow_output_bytes={} rpc_body_received_bytes={}",
        start.elapsed().as_millis(),
        metric(&plan, "arrow_output_bytes"),
        counter(snapshot, "fluss.client.bytes_received.total") - before
    );
    eprintln!("projection cpu_seconds={:.3}", cpu_seconds()? - cpu_start);
    Ok((
        decoded,
        counter(snapshot, "fluss.client.bytes_received.total") - before,
    ))
}

async fn workload(
    connection: &Arc<FlussConnection>,
    kv: bool,
    interleaved: bool,
    warmup: u64,
    measured: u64,
    snapshot: &Snapshotter,
) -> TestResult<()> {
    let admin = connection.get_admin()?;
    let single_bucket_control =
        std::env::var("FLUSS_WRITE_SINGLE_BUCKET_CONTROL").is_ok_and(|value| value == "1");
    let old_buckets = if single_bucket_control { 1 } else { 2 };
    let new_buckets = if single_bucket_control { 1 } else { 3 };
    let path = TablePath::new("native_profile", format!("sink_{kv}_{interleaved}"));
    let fields = Schema::builder()
        .column("id", DataTypes::int())
        .column("region", DataTypes::string())
        .column("value", DataTypes::string());
    let fields = if kv {
        fields.primary_key(vec!["region", "id"])?
    } else {
        fields
    };
    admin
        .create_table(
            &path,
            &TableDescriptor::builder()
                .schema(fields.build()?)
                .partitioned_by(vec!["region"])
                .distributed_by(Some(old_buckets), vec!["id".into()])
                .build()?,
            false,
        )
        .await?;
    let result=async {
        partition(&admin,&path,"north",old_buckets).await?;
        if !single_bucket_control {admin.alter_table(&path,false,AlterTableChanges {modify_bucket_count:Some(new_buckets),..Default::default()}).await?;}
        partition(&admin,&path,"west",new_buckets).await?;
        eprintln!("routing profile old_buckets={old_buckets} new_buckets={new_buckets}");
        let pool:Arc<dyn MemoryPool>=Arc::new(GreedyMemoryPool::new(POOL));
        let ctx=SessionContext::new_with_config_rt(SessionConfig::new().with_target_partitions(1),Arc::new(RuntimeEnvBuilder::new().with_memory_pool(Arc::clone(&pool)).build()?));
        let options=FlussWriteOptions {ack_timeout:Duration::from_secs(10),..Default::default()};
        let mut observer=if kv {
            let provider=FlussKvTable::open(Arc::clone(connection),path.clone(),Duration::from_secs(180)).await?.with_write_options(options)?;
            let events=provider.subscribe_writes();ctx.register_table("sink",Arc::new(provider))?;events
        } else {
            let provider=FlussLogTable::open(Arc::clone(connection),path.clone(),Duration::from_secs(180)).await?.with_write_options(options)?;
            let events=provider.subscribe_writes();ctx.register_table("sink",Arc::new(provider))?;events
        };
        let schema=batch(0,interleaved).schema();
        let (sender,receiver)=mpsc::channel(1);
        ctx.register_table("feed",Arc::new(StreamingTable::try_new(Arc::clone(&schema),vec![Arc::new(Feed {schema,receiver:Mutex::new(Some(receiver))})])?.with_infinite_table(true)))?;
        let query=ctx.sql("INSERT INTO sink SELECT * FROM feed").await?;
        let plan=query.create_physical_plan().await?;
        let task=Arc::new(query.task_ctx()); let writing=Arc::clone(&plan);
        let mut running=AbortOnDrop(tokio::spawn(async move {collect(writing,task).await}));
        let live_read=std::env::var("FLUSS_PROFILE_LIVE_READ").is_ok_and(|value|value=="1");
        if live_read && kv {return Err("live-read profile requires a log destination".into());}
        let live_seen=Arc::new(Mutex::new(vec![false;if live_read {200_000} else {0}]));
        let live_rows=Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut reader=if live_read {
            ctx.register_table("live",Arc::new(FlussLogTable::open_with_options(Arc::clone(connection),path.clone(),LogReadOptions::default()).await?))?;
            let read_query=ctx.sql("SELECT id,region,value FROM live").await?;
            let mut stream=read_query.execute_stream().await?;
            let seen=Arc::clone(&live_seen);let rows=Arc::clone(&live_rows);
            Some(AbortOnDrop(tokio::spawn(async move {
                while let Some(batch)=stream.next().await {
                    let batch=batch?;
                    let ids=batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                    let regions=batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                    let values=batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
                    {let mut seen=seen.lock().unwrap();for row in 0..batch.num_rows() {
                        let id=ids.value(row) as usize;assert!(id<seen.len() && !seen[id]);seen[id]=true;
                        let north=if interleaved {id.is_multiple_of(2)} else {id%ROWS<ROWS/2};assert_eq!(regions.value(row),if north {"north"} else {"west"});
                        let value=values.value(row);assert_eq!(value.len(),WIDTH);assert!(value.starts_with(&format!("row-{id:010}-")) && value[15..].bytes().all(|b|b==b'x'));
                    }}
                    rows.fetch_add(batch.num_rows(),Ordering::Release);
                    drop(batch);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Ok::<_,datafusion::common::DataFusionError>(())
            })))
        } else {None};
        eprintln!("write profile kv={kv} interleaved={interleaved}: warmup {warmup}s");
        let mut next=0;
        stage(&sender,&mut observer,&mut next,interleaved,warmup).await?;
        let warm_rss=rss("VmRSS:")?;
        let live_warm_rows=live_rows.load(Ordering::Acquire);
        let peaks=Arc::new(Mutex::new(Peaks {buffer_min:2*1024*1024,..Default::default()}));
        let stop=Arc::new(AtomicBool::new(false));
        let observing=Arc::clone(&peaks);let observed_pool=Arc::clone(&pool);let observed_plan=Arc::clone(&plan);let stopping=Arc::clone(&stop);let snap=snapshot.clone();
        let mut sampler=AbortOnDrop(tokio::spawn(async move {
            while !stopping.load(Ordering::Acquire) {
                { let mut state=observing.lock().unwrap();
                  state.rss=state.rss.max(rss("VmRSS:").expect("RSS"));state.pool=state.pool.max(observed_pool.reserved());
                  state.retained=state.retained.max(metric(&observed_plan,"fluss_write_retained_arrow_bytes"));
                  state.encoded=state.encoded.max(metric(&observed_plan,"fluss_write_encoded_bytes"));
                  state.transport=state.transport.max(metric(&observed_plan,"fluss_write_transport_bytes"));
                  state.buffer_min=state.buffer_min.min(available(&snap)); }
                { let mut state=observing.lock().unwrap();state.waiting_threads=state.waiting_threads.max(snap.gauge("fluss.client.writer.buffer_waiting_threads")); }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
        let ack_before=metric(&plan,"fluss_write_ack_time");let enqueue_before=metric(&plan,"fluss_write_enqueue_time");
        eprintln!("write profile kv={kv} interleaved={interleaved}: measuring {measured}s");
        let cpu_before=cpu_seconds()?;
        let sent_before=counter(snapshot,"fluss.client.bytes_sent.total");let received_before=counter(snapshot,"fluss.client.bytes_received.total");
        let (latencies,elapsed)=stage(&sender,&mut observer,&mut next,interleaved,measured).await?;
        assert!(latencies.count() as f64/elapsed.as_secs_f64()>=3.6,"less than 90% of the offered four batches/s");
        assert!(latencies.percentile(99)<=1000 && latencies.overflow==0,"confirmation p99 exceeded the fixed 1s profile criterion");
        let cpu=cpu_seconds()?-cpu_before;
        let live_measured_rows=live_rows.load(Ordering::Acquire).saturating_sub(live_warm_rows);
        if live_read {assert!(live_measured_rows as f64/elapsed.as_secs_f64()>=460.8,"continuous reader fell behind the fixed paced workload");}
        let sent=counter(snapshot,"fluss.client.bytes_sent.total")-sent_before;let received=counter(snapshot,"fluss.client.bytes_received.total")-received_before;
        let ack_ns=metric(&plan,"fluss_write_ack_time")-ack_before;let enqueue_ns=metric(&plan,"fluss_write_enqueue_time")-enqueue_before;
        stop.store(true,Ordering::Release);(&mut sampler.0).await?;
        if interleaved {
            running.0.abort();
            assert!(tokio::time::timeout(Duration::from_secs(5),&mut running.0).await?.unwrap_err().is_cancelled());
        } else {
            drop(sender);
            let result=tokio::time::timeout(Duration::from_secs(15),&mut running.0).await???;
            assert_eq!(result[0].column(0).as_any().downcast_ref::<UInt64Array>().unwrap().value(0),next as u64);
        }
        let summary=loop {if let FlussWriteProgress::Terminated(summary)=observer.recv().await? {break summary;}};
        assert_eq!(summary.status,if interleaved {FlussWriteTermination::Cancelled} else {FlussWriteTermination::Completed});assert_eq!(summary.counts.confirmed,next as u64);assert_eq!(summary.counts.uncertain,0);
        if let Some(reader)=reader.as_mut() {
            tokio::time::timeout(Duration::from_secs(15),async {
                while live_rows.load(Ordering::Acquire)<next {
                    if reader.0.is_finished() {panic!("live reader stopped before the confirmed prefix");}
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await?;
            assert_eq!(live_rows.load(Ordering::Acquire),next);
            assert!(live_seen.lock().unwrap()[..next].iter().all(|seen|*seen));
            reader.0.abort();assert!((&mut reader.0).await.unwrap_err().is_cancelled());
            eprintln!("continuous read verified_rows={next} rows_observed_during_measurement={live_measured_rows} measured_seconds={:.3} consumer_pause_ms=1 cancellation=verified",elapsed.as_secs_f64());
        }
        let final_rss=rss("VmRSS:")?;let hwm=rss("VmHWM:")?;
        { let p=peaks.lock().unwrap();assert!(p.rss<=RSS_LIMIT && hwm<=RSS_LIMIT);assert!(p.pool<=POOL);assert!(final_rss<=warm_rss+256*1024*1024);
          eprintln!("write result kv={kv} interleaved={interleaved} batches={} measured_seconds={:.3} measured_rows={} confirmation_p50_ms={} confirmation_p95_ms={} confirmation_p99_ms={} overflow={} ack_ns={ack_ns} enqueue_ns={enqueue_ns} warm_rss={} final_rss={final_rss} hwm={hwm} peak_pool={} peak_retained={} peak_encoded={} peak_transport={} native_buffer_min={}",latencies.count(),elapsed.as_secs_f64(),latencies.count()*ROWS as u64,latencies.percentile(50),latencies.percentile(95),latencies.percentile(99),latencies.overflow,warm_rss,p.pool,p.retained,p.encoded,p.transport,p.buffer_min); }
        eprintln!("write cpu_seconds={cpu:.3} rpc_body_sent={sent} rpc_body_received={received} sampled_waiting_threads={}",peaks.lock().unwrap().waiting_threads);
        drop(plan);drop(query);empty(&pool).await?;
        let mut projections=Vec::new();
        for columns in if kv {vec!["*","id","COUNT(*)"]} else {vec!["*"]} {projections.push(verify(&ctx,next,interleaved,columns,snapshot).await?);empty(&pool).await?;}
        if kv {assert!(projections[1].0<projections[0].0);assert_eq!(projections[2].0,0);assert!(projections.iter().all(|(_,wire)|*wire>0));}
        Ok::<_,Box<dyn std::error::Error>>(())
    }.await;
    let cleanup = admin.drop_table(&path, true).await;
    result?;
    cleanup?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Owned Docker release continuous writer/routing/projection profile; run alone"]
async fn native_continuous_writer_profile() -> TestResult<()> {
    let duration = |name: &str, default: u64| max_duration(name, default);
    let warmup = duration("FLUSS_WRITE_WARMUP_SECS", 60)?;
    let measured = duration("FLUSS_WRITE_MEASURE_SECS", 300)?;
    let selected = std::env::var("FLUSS_WRITE_CASE").ok();
    if selected.as_deref().is_some_and(|name| {
        ![
            "log-contiguous",
            "log-interleaved",
            "kv-contiguous",
            "kv-interleaved",
        ]
        .contains(&name)
    }) {
        return Err("invalid FLUSS_WRITE_CASE".into());
    }
    let snapshot = Snapshotter::default();
    metrics::set_global_recorder(snapshot.clone())?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let mut builder = FlussTestingClusterBuilder::new(format!("native-write-profile-{suffix}"))
        .with_port(20000 + (suffix % 20000) as u16);
    let cluster = builder.build().await;
    let result = std::panic::AssertUnwindSafe(async {
        let ready = cluster.get_fluss_connection().await;
        ready.close(Duration::from_secs(1)).await?;
        let connection = Arc::new(
            FlussConnection::new(Config {
                bootstrap_servers: cluster.plaintext_bootstrap_servers().into(),
                writer_acks: "all".into(),
                writer_buffer_memory_size: 2 * 1024 * 1024,
                writer_batch_size: 256 * 1024,
                ..Default::default()
            })
            .await?,
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let metadata = connection.get_metadata();
                metadata
                    .update_tables_metadata(&Default::default(), &Default::default(), vec![])
                    .await?;
                if metadata.get_cluster().get_tablet_server(0).is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok::<_, fluss::error::Error>(())
        })
        .await??;
        connection
            .get_admin()?
            .create_database("native_profile", None, true)
            .await?;
        for kv in [false, true] {
            for mixed in [false, true] {
                let name = format!(
                    "{}-{}",
                    if kv { "kv" } else { "log" },
                    if mixed { "interleaved" } else { "contiguous" }
                );
                if selected.as_ref().is_some_and(|selected| selected != &name) {
                    continue;
                }
                workload(&connection, kv, mixed, warmup, measured, &snapshot).await?;
            }
        }
        connection.close(Duration::from_secs(5)).await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .catch_unwind()
    .await;
    cluster.stop();
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
fn max_duration(name: &str, default: u64) -> TestResult<u64> {
    let n = std::env::var(name).map_or(Ok(default), |value| value.parse::<u64>())?;
    if n == 0 || n > default {
        return Err(format!("{name} must be 1..={default}").into());
    }
    Ok(n)
}
