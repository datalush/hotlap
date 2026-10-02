// SPDX-License-Identifier: Apache-2.0
//! Isolated Docker profiles: tiny segments, filesystem or RustFS S3 tiering, short TTL.
//! Run alone with FLUSS_IMAGE and FLUSS_VERSION set to a matching Fluss server.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
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
    let endpoint = std::env::var("RUSTFS_ENDPOINT")?;
    let bucket = std::env::var("RUSTFS_BUCKET")?;
    let access = std::env::var("RUSTFS_ACCESS_KEY")?;
    let secret = std::env::var("RUSTFS_SECRET_KEY")?;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
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
                // Test-scoped prefix only. Never touch other objects in the
                // existing RustFS bucket shared with the laboratory.
                let suffix = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_nanos();
                let prefix = format!("datafusion-remote-tests/{}-{suffix}", std::process::id());
                let mut test_objects = S3TestPrefix {
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
                    ("s3.access-key".into(), access.clone()),
                    ("s3.secret-key".into(), secret.clone()),
                    (
                        "s3.assumed.role.arn".into(),
                        "arn:aws:iam::rustfs:role/fluss-read".into(),
                    ),
                    ("s3.assumed.role.sts.endpoint".into(), endpoint.clone()),
                ]);
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
