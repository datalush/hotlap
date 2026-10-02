// SPDX-License-Identifier: Apache-2.0
//! Isolated Docker profiles: tiny segments, filesystem or RustFS S3 tiering, short TTL.
//! Run alone with FLUSS_IMAGE and FLUSS_VERSION set to a matching Fluss server.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{
    AlterConfig, AlterConfigOpType, AlterTableChanges, DataTypes, Schema, TableDescriptor,
    TablePath,
};
use fluss::metrics::SCANNER_REMOTE_FETCH_BYTES_TOTAL;
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;
use fluss_datafusion::FlussLogTable;
use fluss_test_cluster::FlussTestingClusterBuilder;
use futures::StreamExt;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[test]
#[ignore = "requires Docker plus a compatible FLUSS_IMAGE and FLUSS_VERSION"]
fn datafusion_reads_remote_and_rejects_lost_retention() -> TestResult<()> {
    std::env::var("FLUSS_IMAGE")?;
    std::env::var("FLUSS_VERSION")?;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let mut builder = FlussTestingClusterBuilder::new_with_cluster_conf(
                    "datafusion-remote",
                    &tiering_conf(),
                );
                let remote = tempfile::tempdir()?;
                builder = builder.with_remote_data_dir(remote.path().canonicalize()?);
                let cluster = builder.build().await;
                let result = check_remote_scan(&cluster, &snapshotter).await;
                drop(cluster);
                result
            })
    })
}

fn tiering_conf() -> HashMap<String, String> {
    HashMap::from([
        ("log.segment.file-size".into(), "120b".into()),
        ("remote.log.task-interval-duration".into(), "1s".into()),
        ("log.retention.check-interval".into(), "1s".into()),
    ])
}

#[test]
#[ignore = "requires Docker, AWS CLI, a RustFS env file, FLUSS_IMAGE and FLUSS_VERSION"]
fn datafusion_reads_and_expires_rustfs_s3() -> TestResult<()> {
    std::env::var("FLUSS_IMAGE")?;
    std::env::var("FLUSS_VERSION")?;
    let (conf, mut test_objects) = rustfs_profile()?;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let mut builder =
                    FlussTestingClusterBuilder::new_with_cluster_conf("datafusion-s3", &conf)
                        .with_port(9323);
                let cluster = builder.build().await;
                let result = check_remote_scan(&cluster, &snapshotter).await;
                drop(cluster);
                let cleanup = test_objects.cleanup();
                result?;
                cleanup
            })
    })
}

fn rustfs_profile() -> TestResult<(HashMap<String, String>, S3TestPrefix)> {
    let endpoint = std::env::var("RUSTFS_ENDPOINT")?;
    let bucket = std::env::var("RUSTFS_BUCKET")?;
    let access = std::env::var("RUSTFS_ACCESS_KEY")?;
    let secret = std::env::var("RUSTFS_SECRET_KEY")?;
    let check = s3_command(
        &endpoint,
        &access,
        &secret,
        &["s3api", "head-bucket", "--bucket", &bucket],
    )?;
    if !check.status.success() {
        return Err(format!(
            "RustFS bucket unavailable: {}",
            String::from_utf8_lossy(&check.stderr)
        )
        .into());
    }
    // Test-scoped prefix only. Never touch other objects in the shared bucket.
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let prefix = format!("datafusion-remote-tests/{}-{suffix}", std::process::id());
    let test_objects = S3TestPrefix {
        endpoint: endpoint.clone(),
        access: access.clone(),
        secret: secret.clone(),
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cleaned: false,
    };
    let mut conf = tiering_conf();
    conf.extend([
        ("remote.data.dir".into(), format!("s3://{bucket}/{prefix}")),
        ("s3.region".into(), "us-east-1".into()),
        ("s3.endpoint".into(), endpoint.clone()),
        ("s3.path-style-access".into(), "true".into()),
        ("s3.access-key".into(), access),
        ("s3.secret-key".into(), secret),
        (
            "s3.assumed.role.arn".into(),
            "arn:aws:iam::rustfs:role/fluss-read".into(),
        ),
        ("s3.assumed.role.sts.endpoint".into(), endpoint),
    ]);
    Ok((conf, test_objects))
}

#[test]
#[ignore = "35-minute Docker/RustFS load profile; requires AWS CLI, RUSTFS_*, FLUSS_IMAGE and FLUSS_VERSION"]
fn datafusion_resource_pressure_rustfs() -> TestResult<()> {
    std::env::var("FLUSS_IMAGE")?;
    std::env::var("FLUSS_VERSION")?;
    let profile = PressureProfile::from_env()?;
    let (mut conf, mut test_objects) = rustfs_profile()?;
    // Large enough to offload a compressible dataset without creating a
    // separate object for every record.
    conf.insert("log.segment.file-size".into(), "64kb".into());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let mut builder =
                    FlussTestingClusterBuilder::new_with_cluster_conf("datafusion-pressure", &conf)
                        .with_port(9523);
                let cluster = builder.build().await;
                let result = check_resource_pressure(&cluster, profile, &snapshotter).await;
                drop(cluster);
                let cleanup = test_objects.cleanup();
                result?;
                cleanup
            })
    })
}

const PRESSURE_VALUE_BYTES: usize = 128 * 1024;
const PRESSURE_POOL_BYTES: usize = 512 * 1024 * 1024;
const PRESSURE_RSS_LIMIT: usize = 1536 * 1024 * 1024;
const PRESSURE_TEMP_LIMIT: u64 = 2 * 1024 * 1024 * 1024;

fn pressure_value(id: i32) -> String {
    // 4 KiB of distinct but deterministic data repeated to 128 KiB: Arrow
    // must decode a large row, while the remote object is ~32x smaller.
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";
    let mut block = format!("row-{id:08}-");
    let mut seed = (id as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    while block.len() < 4 * 1024 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        block.push(ALPHABET[(seed & 63) as usize] as char);
    }
    block.repeat(PRESSURE_VALUE_BYTES / block.len())
}

#[derive(Clone, Copy)]
struct PressureProfile {
    rows: usize,
    warmup: Duration,
    measure: Duration,
    full: bool,
}

impl PressureProfile {
    fn from_env() -> TestResult<Self> {
        let value = |key: &str, default| -> TestResult<usize> {
            Ok(std::env::var(key).map_or(Ok(default), |v| v.parse::<usize>())?)
        };
        let rows = value("FLUSS_PRESSURE_ROWS", 4_800)?;
        let warmup = value("FLUSS_PRESSURE_WARMUP_SECS", 300)?;
        let measure = value("FLUSS_PRESSURE_MEASURE_SECS", 1_800)?;
        if rows == 0 || rows > 4_800 || measure == 0 || measure > 1_800 || warmup > 300 {
            return Err("invalid pressure profile size/duration".into());
        }
        let full = rows == 4_800 && warmup == 300 && measure == 1_800;
        assert!(!full || rows * PRESSURE_VALUE_BYTES > PRESSURE_POOL_BYTES);
        Ok(Self {
            rows,
            warmup: Duration::from_secs(warmup as u64),
            measure: Duration::from_secs(measure as u64),
            full,
        })
    }
}

#[derive(Default)]
struct PressureSamples {
    peak_rss: usize,
    peak_pool: usize,
    peak_temp: u64,
    last_rss: usize,
    failure: Option<String>,
}

fn process_memory_bytes(field: &str) -> TestResult<usize> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or(format!("/proc/self/status contains no {field}"))?;
    let kb: usize = line
        .split_whitespace()
        .nth(1)
        .ok_or(format!("missing {field}"))?
        .parse()?;
    Ok(kb * 1024)
}

fn rss_bytes() -> TestResult<usize> {
    process_memory_bytes("VmRSS:")
}

fn remote_temp_bytes() -> std::io::Result<u64> {
    let mut total = 0_u64;
    for directory in std::fs::read_dir(std::env::temp_dir())? {
        let directory = directory?;
        if directory
            .file_name()
            .to_string_lossy()
            .starts_with("fluss-remote-logs")
        {
            for file in std::fs::read_dir(directory.path())? {
                total += file?.metadata()?.len();
            }
        }
    }
    Ok(total)
}

async fn observe_pressure(
    pool: Arc<dyn MemoryPool>,
    samples: Arc<Mutex<PressureSamples>>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        let failed = {
            let observed = (|| -> TestResult<(usize, usize, u64)> {
                Ok((rss_bytes()?, pool.reserved(), remote_temp_bytes()?))
            })();
            let mut state = samples.lock().unwrap();
            match observed {
                Ok((rss, reserved, temporary)) => {
                    state.last_rss = rss;
                    state.peak_rss = state.peak_rss.max(rss);
                    state.peak_pool = state.peak_pool.max(reserved);
                    state.peak_temp = state.peak_temp.max(temporary);
                    if rss > PRESSURE_RSS_LIMIT
                        || temporary > PRESSURE_TEMP_LIMIT
                        || reserved > PRESSURE_POOL_BYTES
                    {
                        state.failure = Some(format!(
                            "resource budget exceeded: rss={rss}, pool={reserved}, temporary={temporary}"
                        ));
                    }
                }
                Err(error) => state.failure = Some(error.to_string()),
            }
            state.failure.is_some()
        };
        if failed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn check_resource_pressure(
    cluster: &fluss_test_cluster::FlussTestingCluster,
    profile: PressureProfile,
    snapshotter: &Snapshotter,
) -> TestResult<()> {
    let config = Config {
        bootstrap_servers: cluster.plaintext_bootstrap_servers().to_string(),
        scanner_log_max_poll_records: 128,
        scanner_remote_log_read_chunk_bytes: 1024 * 1024,
        scanner_remote_log_read_concurrency: 2,
        remote_file_download_thread_num: 2,
        scanner_remote_log_prefetch_num: 2,
        scanner_remote_log_max_prefetch_bytes: 64 * 1024 * 1024,
        ..Config::default()
    };
    let connection = Arc::new(
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                match FlussConnection::new(config.clone()).await {
                    Ok(connection) => break connection,
                    Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                }
            }
        })
        .await
        .map_err(|_| "pressure coordinator did not accept a connection within 60s")?,
    );
    let admin = connection.get_admin()?;
    let metadata = connection.get_metadata();
    wait_for(Duration::from_secs(60), || async {
        let updated = metadata
            .update_tables_metadata(
                &std::collections::HashSet::new(),
                &std::collections::HashSet::new(),
                vec![],
            )
            .await
            .is_ok();
        Ok::<bool, std::convert::Infallible>(
            updated && metadata.get_cluster().get_tablet_server(0).is_some(),
        )
    })
    .await?;
    let suffix = std::process::id();
    let log = TablePath::new("fluss", format!("pressure_log_{suffix}"));
    let kv = TablePath::new("fluss", format!("pressure_kv_{suffix}"));
    let schema = || {
        Schema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string())
    };
    admin
        .create_table(
            &log,
            &TableDescriptor::builder()
                .schema(schema().build()?)
                .distributed_by(Some(2), vec!["id".into()])
                .property("table.log.ttl", "0ms")
                .property("table.log.tiered.local-segments", "1")
                .build()?,
            false,
        )
        .await
        .map_err(|error| format!("create pressure log: {error}"))?;
    let result: TestResult<()> = async {
        admin
            .create_table(
                &kv,
                &TableDescriptor::builder()
                    .schema(schema().primary_key(vec!["id"])?.build()?)
                    .distributed_by(Some(2), vec!["id".into()])
                    .build()?,
                false,
            )
            .await
            .map_err(|error| format!("create pressure KV: {error}"))?;
        run_pressure_profile(&connection, &log, &kv, profile, snapshotter)
            .await
            .map_err(|error| format!("run pressure profile: {error}").into())
    }
    .await;
    let log_cleanup = admin.drop_table(&log, true).await;
    let kv_cleanup = admin.drop_table(&kv, true).await;
    result?;
    log_cleanup?;
    kv_cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn run_pressure_profile(
    connection: &Arc<FlussConnection>,
    log: &TablePath,
    kv: &TablePath,
    profile: PressureProfile,
    snapshotter: &Snapshotter,
) -> TestResult<()> {
    let log_writer = connection
        .get_table(log)
        .await?
        .new_append()?
        .create_writer()?;
    for id in 0..profile.rows {
        let mut row = GenericRow::new(2);
        row.set_field(0, id as i32);
        row.set_field(1, pressure_value(id as i32));
        log_writer
            .append(&row)
            .map_err(|error| format!("append log row {id}: {error}"))?;
        if id % 64 == 63 {
            log_writer
                .flush()
                .await
                .map_err(|error| format!("flush log after row {id}: {error}"))?;
        }
    }
    log_writer
        .flush()
        .await
        .map_err(|error| format!("final log flush: {error}"))?;
    drop(log_writer);
    let kv_writer = connection
        .get_table(kv)
        .await?
        .new_upsert()?
        .create_writer()?;
    for id in 0..64_i32 {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, pressure_value(id));
        kv_writer
            .upsert(&row)
            .map_err(|error| format!("upsert KV row {id}: {error}"))?;
        if id % 32 == 31 {
            kv_writer
                .flush()
                .await
                .map_err(|error| format!("flush KV after row {id}: {error}"))?;
        }
    }
    kv_writer
        .flush()
        .await
        .map_err(|error| format!("final KV flush: {error}"))?;
    drop(kv_writer);
    let admin = connection.get_admin()?;
    let table_id = connection.get_table(log).await?.get_table_info().table_id;
    if profile.full {
        wait_for(Duration::from_secs(90), || async {
            admin
                .list_remote_log_manifests(table_id, None)
                .await
                .map(|entries| entries.iter().any(|entry| entry.remote_log_end_offset > 0))
        })
        .await?;
    }

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(PRESSURE_POOL_BYTES));
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(2),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        ),
    );
    ctx.register_table(
        "pressure_log",
        Arc::new(
            FlussLogTable::open(
                Arc::clone(connection),
                log.clone(),
                Duration::from_secs(180),
            )
            .await?,
        ),
    )?;
    ctx.register_table(
        "pressure_kv",
        Arc::new(
            fluss_datafusion::FlussKvTable::open(
                Arc::clone(connection),
                kv.clone(),
                Duration::from_secs(180),
            )
            .await?,
        ),
    )?;

    // Deliberately provoke a pool rejection, then verify the normal context
    // can continue. The result is an error, not a truncated successful query.
    let tiny_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
    let tiny = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(2),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&tiny_pool))
                .build()?,
        ),
    );
    tiny.register_table(
        "log",
        Arc::new(
            FlussLogTable::open(
                Arc::clone(connection),
                log.clone(),
                Duration::from_secs(180),
            )
            .await?,
        ),
    )?;
    let error = tiny
        .sql("SELECT * FROM log")
        .await?
        .collect()
        .await
        .expect_err("memory pressure must reject the scan");
    assert!(
        error.to_string().to_lowercase().contains("memory"),
        "{error}"
    );
    assert_eq!(tiny_pool.reserved(), 0);
    drop(tiny);

    let samples = Arc::new(Mutex::new(PressureSamples::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = tokio::spawn(observe_pressure(
        Arc::clone(&pool),
        Arc::clone(&samples),
        Arc::clone(&stop),
    ));
    let result = async {
        let warmup_runs = run_pressure_stage(&ctx, &pool, &samples, profile.rows, profile.warmup).await?;
        if profile.full && remote_bytes(snapshotter) == 0 {
            return Err("no remote log bytes fetched during pressure warmup".into());
        }
        let warm_rss = rss_bytes()?;
        let start = Instant::now();
        let measured_runs = run_pressure_stage(&ctx, &pool, &samples, profile.rows, profile.measure).await?;
        let elapsed = start.elapsed();
        wait_for(Duration::from_secs(10), || async { remote_temp_bytes().map(|bytes| bytes == 0) }).await?;
        assert_eq!(pool.reserved(), 0);
        let state = samples.lock().unwrap();
        if let Some(error) = &state.failure {
            return Err(error.clone().into());
        }
        let final_rss = rss_bytes()?;
        let high_water_rss = process_memory_bytes("VmHWM:")?;
        let downloaded = remote_bytes(snapshotter);
        eprintln!(
            "DataFusion pressure profile full={} rows={} warmup_scans={} measured_scans={} measured_seconds={:.1} warmup_rss_mib={} sampled_peak_rss_mib={} process_hwm_rss_mib={} final_rss_mib={} peak_pool_mib={} sampled_peak_remote_temp_bytes={} remote_download_bytes={}",
            profile.full, profile.rows, warmup_runs, measured_runs, elapsed.as_secs_f64(),
            warm_rss / (1024 * 1024), state.peak_rss / (1024 * 1024), high_water_rss / (1024 * 1024),
            final_rss / (1024 * 1024), state.peak_pool / (1024 * 1024), state.peak_temp, downloaded
        );
        if high_water_rss > PRESSURE_RSS_LIMIT {
            return Err(format!("process RSS high-water mark exceeded budget: {high_water_rss}").into());
        }
        if profile.full && final_rss > warm_rss + 256 * 1024 * 1024 {
            return Err(format!("RSS did not stabilize after warmup: {warm_rss} -> {final_rss}").into());
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }.await;
    stop.store(true, Ordering::Relaxed);
    sampler.await?;
    result
}

async fn run_pressure_stage(
    ctx: &SessionContext,
    pool: &Arc<dyn MemoryPool>,
    samples: &Arc<Mutex<PressureSamples>>,
    log_rows: usize,
    duration: Duration,
) -> TestResult<usize> {
    let until = Instant::now() + duration;
    let mut scans = 0;
    loop {
        let reads = (0..4).map(|index| {
            let (table, expected) = if index % 2 == 0 {
                ("pressure_log", log_rows)
            } else {
                ("pressure_kv", 64)
            };
            pressure_read(ctx, table, expected, samples)
        });
        futures::future::try_join_all(reads).await?;
        scans += 4;
        wait_for(Duration::from_secs(5), || async {
            Ok::<bool, std::convert::Infallible>(pool.reserved() == 0)
        })
        .await?;
        // Exercise early termination after each complete wave. Temporary
        // downloads and pool reservations must disappear before the next wave.
        for table in ["pressure_log", "pressure_kv"] {
            let query = ctx.sql(&format!("SELECT id, value FROM {table}")).await?;
            let source = source_plan(&query.create_physical_plan().await?);
            let mut stream = source.execute(0, Arc::new(query.task_ctx()))?;
            let first = stream.next().await.expect("first pressure batch")?;
            let held = pool.reserved();
            assert!(first.num_rows() > 0 && held > 0);
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(pool.reserved(), held, "paused source pulled more data");
            drop(first);
            drop(stream);
            wait_for(Duration::from_secs(5), || async {
                Ok::<bool, std::convert::Infallible>(pool.reserved() == 0)
            })
            .await?;
        }
        wait_for(Duration::from_secs(5), || async {
            remote_temp_bytes().map(|bytes| bytes == 0)
        })
        .await?;
        if let Some(error) = samples.lock().unwrap().failure.clone() {
            return Err(error.into());
        }
        if Instant::now() >= until {
            break;
        }
    }
    Ok(scans)
}

async fn pressure_read(
    ctx: &SessionContext,
    table: &str,
    expected: usize,
    samples: &Arc<Mutex<PressureSamples>>,
) -> TestResult<()> {
    let mut seen = vec![false; expected];
    let mut stream = ctx
        .sql(&format!("SELECT id, value FROM {table}"))
        .await?
        .execute_stream()
        .await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .ok_or("invalid id column")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("invalid value column")?;
        for index in 0..batch.num_rows() {
            let id = usize::try_from(ids.value(index))?;
            if id >= expected
                || seen[id]
                || values.value(index).len() != PRESSURE_VALUE_BYTES
                || !values.value(index).starts_with(&format!("row-{id:08}-"))
            {
                return Err(format!("invalid or repeated {table} row {id}").into());
            }
            seen[id] = true;
        }
        if let Some(error) = samples.lock().unwrap().failure.clone() {
            return Err(error.into());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    if seen.iter().any(|present| !present) {
        return Err(format!("{table}: scan omitted rows").into());
    }
    Ok(())
}

fn s3_command(
    endpoint: &str,
    access: &str,
    secret: &str,
    args: &[&str],
) -> std::io::Result<std::process::Output> {
    std::process::Command::new("aws")
        .env("AWS_ACCESS_KEY_ID", access)
        .env("AWS_SECRET_ACCESS_KEY", secret)
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .args(["--endpoint-url", endpoint])
        .args(args)
        .output()
}

struct S3TestPrefix {
    endpoint: String,
    access: String,
    secret: String,
    bucket: String,
    prefix: String,
    cleaned: bool,
}

impl S3TestPrefix {
    fn cleanup(&mut self) -> TestResult<()> {
        if self.cleaned {
            return Ok(());
        }
        assert!(self.prefix.starts_with("datafusion-remote-tests/") && self.prefix.contains('-'));
        let uri = format!("s3://{}/{}", self.bucket, self.prefix);
        let output = s3_command(
            &self.endpoint,
            &self.access,
            &self.secret,
            &["s3", "rm", &uri, "--recursive", "--quiet"],
        )?;
        if !output.status.success() {
            return Err(format!(
                "Could not remove test S3 prefix: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for S3TestPrefix {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

async fn check_remote_scan(
    cluster: &fluss_test_cluster::FlussTestingCluster,
    snapshotter: &Snapshotter,
) -> TestResult<()> {
    let connection = Arc::new(cluster.get_fluss_connection().await);
    let path = TablePath::new("fluss", format!("df_remote_{}", std::process::id()));
    let admin = connection.get_admin()?;
    let metadata = connection.get_metadata();
    // The coordinator can accept connections before its tabletserver has
    // registered. Wait for metadata to report the server before creating a
    // replicated table; relying on image-pull timing made cold runs flaky.
    wait_for(Duration::from_secs(60), || async {
        let updated = metadata
            .update_tables_metadata(
                &std::collections::HashSet::new(),
                &std::collections::HashSet::new(),
                vec![],
            )
            .await
            .is_ok();
        Ok::<bool, std::convert::Infallible>(
            updated && metadata.get_cluster().get_tablet_server(0).is_some(),
        )
    })
    .await?;
    admin
        .create_table(
            &path,
            &TableDescriptor::builder()
                .schema(
                    Schema::builder()
                        .column("id", DataTypes::int())
                        .column("value", DataTypes::string())
                        .build()?,
                )
                .distributed_by(Some(1), vec!["id".into()])
                .property("table.log.ttl", "0ms")
                .property("table.log.tiered.local-segments", "1")
                .build()?,
            false,
        )
        .await?;
    let result = async {
        read_tiered_table(&connection, &path, snapshotter).await?;
        check_pending_limit(cluster, &path).await
    }
    .await;
    let cleanup = admin.drop_table(&path, true).await;
    result?;
    cleanup?;
    check_retention_scan(&connection).await?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn check_pending_limit(
    cluster: &fluss_test_cluster::FlussTestingCluster,
    path: &TablePath,
) -> TestResult<()> {
    check_source_limit(
        cluster,
        path,
        Config {
            scanner_remote_log_max_pending_segments: 1,
            ..Config::default()
        },
        "pending segments",
    )
    .await?;
    check_source_limit(
        cluster,
        path,
        Config {
            scanner_remote_log_max_prefetch_bytes: 1,
            ..Config::default()
        },
        "prefetch limit",
    )
    .await
}

async fn check_source_limit(
    cluster: &fluss_test_cluster::FlussTestingCluster,
    path: &TablePath,
    mut config: Config,
    expected: &str,
) -> TestResult<()> {
    config.bootstrap_servers = cluster.plaintext_bootstrap_servers().to_string();
    let connection = Arc::new(FlussConnection::new(config).await?);
    let ctx = SessionContext::new();
    ctx.register_table(
        "remote",
        Arc::new(
            FlussLogTable::open(
                Arc::clone(&connection),
                path.clone(),
                Duration::from_secs(45),
            )
            .await?,
        ),
    )?;
    let error = ctx
        .sql("SELECT * FROM remote")
        .await?
        .collect()
        .await
        .expect_err("the remote request budget must reject excess segments");
    assert!(error.to_string().contains(expected), "{error}");
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn read_tiered_table(
    connection: &Arc<FlussConnection>,
    path: &TablePath,
    snapshotter: &Snapshotter,
) -> TestResult<()> {
    let table = connection.get_table(path).await?;
    let writer = table.new_append()?.create_writer()?;
    for id in 0..80_i32 {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, format!("value-{id}"));
        writer.append(&row)?.await?;
    }
    writer.flush().await?;
    let admin = connection.get_admin()?;
    let table_id = table.get_table_info().table_id;
    wait_for(Duration::from_secs(45), || async {
        admin
            .list_remote_log_manifests(table_id, None)
            .await
            .map(|manifests| {
                manifests
                    .iter()
                    .any(|manifest| manifest.remote_log_end_offset > 0)
            })
    })
    .await?;
    let manifests = admin.list_remote_log_manifests(table_id, None).await?;
    assert!(
        manifests
            .iter()
            .any(|manifest| manifest.remote_log_end_offset > 0)
    );
    let ctx = SessionContext::new();
    ctx.register_table(
        "logs",
        Arc::new(
            FlussLogTable::open(
                Arc::clone(connection),
                path.clone(),
                Duration::from_secs(45),
            )
            .await?,
        ),
    )?;
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let batches = ctx
            .sql("SELECT id FROM logs WHERE id >= 20 AND id < 60 ORDER BY id")
            .await?
            .collect()
            .await?;
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap();
                (0..values.len())
                    .map(|row| values.value(row))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, (20..60).collect::<Vec<_>>());
        if remote_bytes(snapshotter) > 0 {
            break;
        }
        if Instant::now() >= deadline {
            return Err(
                "Remote manifest exists but the client never fetched remote log bytes".into(),
            );
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    // A fresh append is still local while the earlier IDs require a remote
    // download. One SQL query must cross both tiers without duplication.
    let mut local = GenericRow::new(2);
    local.set_field(0, 80_i32);
    local.set_field(1, "fresh-local");
    writer.append(&local)?.await?;
    let full = ctx
        .sql("SELECT id FROM logs ORDER BY id")
        .await?
        .collect()
        .await?;
    let ids: Vec<_> = full
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(ids, (0..=80).collect::<Vec<_>>());
    let current = admin.list_offsets(path, &[0], OffsetSpec::Earliest).await?;
    assert_eq!(
        current[&0], 0,
        "TTL-disabled remote log must retain the earliest offset"
    );
    check_slow_remote_consumer(&ctx).await?;
    Ok(())
}

async fn check_slow_remote_consumer(ctx: &SessionContext) -> TestResult<()> {
    let query = ctx.sql("SELECT id FROM logs").await?;
    let source = source_plan(&query.create_physical_plan().await?);
    let task_ctx = Arc::new(query.task_ctx());
    let pool = Arc::clone(&task_ctx.runtime_env().memory_pool);
    let mut stream = source.execute(0, task_ctx)?;
    assert!(stream.next().await.expect("first remote batch")?.num_rows() > 0);
    let reserved = pool.reserved();
    assert!(reserved > 0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        pool.reserved(),
        reserved,
        "a paused consumer must not pull more Arrow batches"
    );
    assert!(
        remote_temp_files()? <= 4,
        "remote prefetch must honor the default four-file limit"
    );
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    wait_for(Duration::from_secs(5), || async {
        remote_temp_files().map(|files| files == 0)
    })
    .await?;
    Ok(())
}

fn remote_temp_files() -> std::io::Result<usize> {
    let mut files = 0;
    for entry in std::fs::read_dir(std::env::temp_dir())? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("fluss-remote-logs")
        {
            files += std::fs::read_dir(entry.path())?.count();
        }
    }
    Ok(files)
}

fn remote_bytes(snapshotter: &Snapshotter) -> u64 {
    snapshotter
        .snapshot()
        .into_vec()
        .iter()
        .filter_map(|(key, _, _, value)| {
            (key.key().name() == SCANNER_REMOTE_FETCH_BYTES_TOTAL)
                .then_some(value)
                .and_then(|value| match value {
                    DebugValue::Counter(bytes) => Some(*bytes),
                    _ => None,
                })
        })
        .sum()
}

async fn check_retention_scan(connection: &Arc<FlussConnection>) -> TestResult<()> {
    let path = TablePath::new("fluss", format!("df_ttl_{}", std::process::id()));
    let admin = connection.get_admin()?;
    admin
        .create_table(
            &path,
            &TableDescriptor::builder()
                .schema(
                    Schema::builder()
                        .column("id", DataTypes::int())
                        .column("value", DataTypes::string())
                        .build()?,
                )
                .distributed_by(Some(1), vec!["id".into()])
                .property("table.log.ttl", "0ms")
                .property("table.log.tiered.local-segments", "1")
                .build()?,
            false,
        )
        .await?;
    let result = read_during_retention(connection, &path).await;
    let cleanup = admin.drop_table(&path, true).await;
    result?;
    cleanup?;
    Ok(())
}

async fn read_during_retention(
    connection: &Arc<FlussConnection>,
    path: &TablePath,
) -> TestResult<()> {
    let table = connection.get_table(path).await?;
    let writer = table.new_append()?.create_writer()?;
    let payload = "x".repeat(400_000);
    const ROWS: i32 = 50;
    for id in 0..ROWS {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, format!("{id}-{payload}"));
        writer.append(&row)?.await?;
    }
    writer.flush().await?;
    let admin = connection.get_admin()?;
    let table_id = table.get_table_info().table_id;
    wait_for(Duration::from_secs(60), || async {
        admin
            .list_remote_log_manifests(table_id, None)
            .await
            .map(|entries| {
                entries
                    .iter()
                    .any(|entry| entry.remote_log_end_offset >= 45)
            })
    })
    .await?;

    // Keep only one remote segment in the scanner and yield one record per
    // pull. The next segment cannot all be prefetched while this query waits
    // for TTL, so losing the retained range must surface as a scan error.
    let mut scan_config = connection.config().clone();
    scan_config.scanner_remote_log_prefetch_num = 1;
    scan_config.scanner_log_max_poll_records = 1;
    scan_config.scanner_remote_log_read_chunk_bytes = 256;
    scan_config.scanner_remote_log_operation_timeout_ms = 15_000;
    let scan_connection = Arc::new(FlussConnection::new(scan_config).await?);
    let ctx = SessionContext::new();
    ctx.register_table(
        "ttl",
        Arc::new(
            FlussLogTable::open(
                Arc::clone(&scan_connection),
                path.clone(),
                Duration::from_secs(60),
            )
            .await?,
        ),
    )?;
    let query = ctx.sql("SELECT id FROM ttl").await?;
    let plan = query.create_physical_plan().await?;
    let source = source_plan(&plan);
    let mut stream = source.execute(0, Arc::new(query.task_ctx()))?;
    let first = stream.next().await.expect("first log batch")?;
    assert_eq!(
        first.num_rows(),
        1,
        "the paused scan must have read just one row"
    );

    admin
        .alter_table(
            path,
            false,
            AlterTableChanges {
                config_changes: vec![AlterConfig::new(
                    "table.log.ttl",
                    Some("2s".into()),
                    AlterConfigOpType::Set,
                )],
                ..Default::default()
            },
        )
        .await?;
    wait_for(Duration::from_secs(40), || async {
        admin
            .list_offsets(path, &[0], OffsetSpec::Earliest)
            .await
            .map(|offsets| offsets.get(&0).is_some_and(|&earliest| earliest >= 45))
    })
    .await?;

    let mut scan_error = None;
    while let Some(next) = stream.next().await {
        match next {
            Ok(_) => {}
            Err(error) => {
                scan_error = Some(error.to_string());
                break;
            }
        }
    }
    assert!(
        scan_error.as_ref().is_some_and(|message| {
            let message = message.to_ascii_lowercase();
            message.contains("out of range")
                || message.contains("lost retained log offsets")
                || message.contains("notfound")
        }),
        "expected a lost-offset or missing-segment error after retention, got {scan_error:?}"
    );
    drop(stream);

    let earliest = admin.list_offsets(path, &[0], OffsetSpec::Earliest).await?[&0];
    let rows = ctx
        .sql("SELECT id FROM ttl ORDER BY id")
        .await?
        .collect()
        .await?;
    let ids: Vec<i32> = rows
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(ids, (earliest as i32..ROWS).collect::<Vec<_>>());
    Ok(())
}

fn source_plan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if plan.name() == "FlussScanExec" {
        return Arc::clone(plan);
    }
    plan.children()
        .into_iter()
        .find_map(|child| (child.name() == "FlussScanExec").then(|| Arc::clone(child)))
        .expect("SQL must plan a Fluss source")
}

async fn wait_for<F, Fut, E>(timeout: Duration, mut check: F) -> TestResult<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool, E>>,
    E: std::error::Error + 'static,
{
    let deadline = Instant::now() + timeout;
    loop {
        if check().await? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("Fluss remote condition did not become true".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
