// SPDX-License-Identifier: Apache-2.0
//! Isolated Docker profile: tiny segments, local filesystem tiering, short TTL.
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
                    &HashMap::from([
                        ("log.segment.file-size".into(), "120b".into()),
                        ("remote.log.task-interval-duration".into(), "1s".into()),
                        ("log.retention.check-interval".into(), "1s".into()),
                    ]),
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

async fn check_remote_scan(
    cluster: &fluss_test_cluster::FlussTestingCluster,
    snapshotter: &Snapshotter,
) -> TestResult<()> {
    let connection = Arc::new(cluster.get_fluss_connection().await);
    let path = TablePath::new("fluss", format!("df_remote_{}", std::process::id()));
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
    let connection = Arc::new(
        FlussConnection::new(Config {
            bootstrap_servers: cluster.plaintext_bootstrap_servers().to_string(),
            scanner_remote_log_max_pending_segments: 1,
            ..Config::default()
        })
        .await?,
    );
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
    assert!(error.to_string().contains("pending segments"), "{error}");
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

    let ctx = SessionContext::new();
    ctx.register_table(
        "ttl",
        Arc::new(
            FlussLogTable::open(
                Arc::clone(connection),
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
    assert!(first.num_rows() > 0 && first.num_rows() < ROWS as usize);

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

    let mut saw_error = false;
    while let Some(next) = stream.next().await {
        match next {
            Ok(_) => {}
            Err(_) => {
                saw_error = true;
                break;
            }
        }
    }
    assert!(
        saw_error,
        "retention must fail an unfinished scan, not finish with missing rows"
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
