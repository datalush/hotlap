// SPDX-License-Identifier: Apache-2.0
//! Optional isolated-lab integration: run with FLUSS_* from ../lab/.env.

use std::collections::{HashMap, HashSet};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::collect as collect_plan;
use datafusion::physical_plan::common::collect as collect_stream;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{
    AddColumn, AlterTableChanges, ClusterHealthStatus, ColumnPositionType, DataTypes, JsonSerde,
    PartitionSpec, Schema, TableBucket, TableDescriptor, TablePath,
};
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;
use fluss_datafusion::{FlussCatalog, FlussKvTable, FlussLogTable, LogReadOptions, LogStart};
use futures::StreamExt;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
const DATABASE: &str = "datafusion_tests";
const SCAN_TIMEOUT: Duration = Duration::from_secs(45);

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; creates one isolated log"]
async fn streaming_log_waits_for_appends_and_releases_on_cancel() -> TestResult<()> {
    let connection = connect().await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let path = TablePath::new(DATABASE, format!("stream_{suffix}"));
    let admin = connection.get_admin()?;
    admin
        .create_table(
            &path,
            &TableDescriptor::builder()
                .schema(Schema::builder().column("id", DataTypes::int()).build()?)
                .distributed_by(Some(1), vec!["id".into()])
                .build()?,
            false,
        )
        .await?;
    let result: TestResult<()> = async {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
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
        let source = FlussLogTable::open_with_options(
            Arc::clone(&connection),
            path.clone(),
            LogReadOptions::default(),
        )
        .await?;
        let mut delivered = source.subscribe_deliveries();
        let mut observed = source.subscribe_progress();
        ctx.register_table("live", Arc::new(source))?;
        let query = ctx.sql("SELECT id FROM live").await?;
        let plan = query.create_physical_plan().await?;
        let source = source_plan(&plan);
        assert!(source.boundedness().is_unbounded());
        let mut stream = source.execute(0, Arc::new(query.task_ctx()))?;
        assert!(
            tokio::time::timeout(Duration::from_millis(900), stream.next())
                .await
                .is_err(),
            "no new records must not end the stream"
        );
        let fluss_datafusion::LogProgress::Initialized { positions, .. } = observed.try_recv()?
        else {
            panic!("idle streaming execution must publish its initial assignment");
        };
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].start_offset, 0);
        assert!(positions[0].stop_offset.is_none());
        let table = connection.get_table(&path).await?;
        let writer = table.new_append()?.create_writer()?;
        let mut execution_id = None;
        let mut retained_batches = Vec::new();
        for id in [11_i32, 12_i32] {
            if id == 12 {
                let held = pool.reserved();
                assert!(held > 0);
                tokio::time::sleep(Duration::from_secs(1)).await;
                assert_eq!(
                    pool.reserved(),
                    held,
                    "slow consumers must not grow Arrow reservations"
                );
            }
            let mut row = GenericRow::new(1);
            row.set_field(0, id);
            writer.append(&row)?;
            writer.flush().await?;
            let batch = tokio::time::timeout(Duration::from_secs(12), stream.next())
                .await?
                .ok_or("stream ended after a late append")??;
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(values.len(), 1);
            assert_eq!(values.value(0), id);
            let progress = delivered.try_recv()?;
            assert_eq!(
                progress.bucket,
                TableBucket::new(table.get_table_info().table_id, 0)
            );
            assert_eq!(progress.base_offset, (id - 11) as i64);
            assert_eq!(progress.next_offset, (id - 10) as i64);
            assert_eq!(progress.rows, 1);
            if let Some(previous) = execution_id {
                assert_eq!(progress.execution_id, previous);
            }
            execution_id = Some(progress.execution_id);
            retained_batches.push(batch);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(900), stream.next())
                .await
                .is_err()
        );
        drop(stream);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            pool.reserved() > 0,
            "retained streaming buffers must stay charged after cancellation"
        );
        retained_batches.clear();
        assert_eq!(pool.reserved(), 0);
        let mut cancelled = false;
        while let Ok(event) = observed.try_recv() {
            if let fluss_datafusion::LogProgress::Terminated { status, .. } = event {
                assert_eq!(status, fluss_datafusion::LogTermination::Cancelled);
                cancelled = true;
            }
        }
        assert!(cancelled);
        assert_eq!(
            metric(&source.metrics().unwrap(), "fluss_active_partition_streams"),
            0
        );

        let table_id = table.get_table_info().table_id;
        let resumed = FlussLogTable::open_with_options(
            Arc::clone(&connection),
            path.clone(),
            LogReadOptions {
                start: LogStart::Offsets(HashMap::from([(TableBucket::new(table_id, 0), 1)])),
                ..LogReadOptions::batch(SCAN_TIMEOUT)
            },
        )
        .await?;
        ctx.register_table("resumed", Arc::new(resumed))?;
        let rows = ctx.sql("SELECT id FROM resumed").await?.collect().await?;
        assert_eq!(rows.len(), 1);
        let ids = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[12]);

        let latest = FlussLogTable::open_with_options(
            Arc::clone(&connection),
            path.clone(),
            LogReadOptions {
                start: LogStart::Latest,
                ..LogReadOptions::batch(SCAN_TIMEOUT)
            },
        )
        .await?;
        ctx.register_table("latest", Arc::new(latest))?;
        assert!(
            ctx.sql("SELECT id FROM latest")
                .await?
                .collect()
                .await?
                .is_empty()
        );

        let invalid = FlussLogTable::open_with_options(
            Arc::clone(&connection),
            path.clone(),
            LogReadOptions {
                start: LogStart::Offsets(HashMap::from([(TableBucket::new(table_id, 0), -1)])),
                ..LogReadOptions::batch(SCAN_TIMEOUT)
            },
        )
        .await?;
        ctx.register_table("invalid", Arc::new(invalid))?;
        let error = ctx
            .sql("SELECT id FROM invalid")
            .await?
            .collect()
            .await
            .expect_err("expired/invalid offsets cannot become successful scans");
        assert!(
            error.to_string().contains("outside retained range"),
            "{error}"
        );

        let stale = FlussLogTable::open_with_options(
            Arc::clone(&connection),
            path.clone(),
            LogReadOptions {
                start: LogStart::Offsets(HashMap::from([(TableBucket::new(table_id + 1, 0), 1)])),
                ..LogReadOptions::batch(SCAN_TIMEOUT)
            },
        )
        .await?;
        ctx.register_table("stale", Arc::new(stale))?;
        let error = ctx
            .sql("SELECT id FROM stale")
            .await?
            .collect()
            .await
            .expect_err("a checkpoint from another table identity must not resume here");
        assert!(error.to_string().contains("cover exactly"), "{error}");

        let continuous_resume = FlussLogTable::open_with_options(
            Arc::clone(&connection),
            path.clone(),
            LogReadOptions {
                start: LogStart::Offsets(HashMap::from([(TableBucket::new(table_id, 0), 2)])),
                ..LogReadOptions::default()
            },
        )
        .await?;
        ctx.register_table("continuous_resume", Arc::new(continuous_resume))?;
        let next = ctx.sql("SELECT id FROM continuous_resume").await?;
        let mut resumed_stream = source_plan(&next.create_physical_plan().await?)
            .execute(0, Arc::new(next.task_ctx()))?;
        assert!(
            tokio::time::timeout(Duration::from_millis(800), resumed_stream.next())
                .await
                .is_err()
        );
        let mut row = GenericRow::new(1);
        row.set_field(0, 13_i32);
        writer.append(&row)?;
        writer.flush().await?;
        let batch = tokio::time::timeout(Duration::from_secs(12), resumed_stream.next())
            .await?
            .expect("continuous resume must see later data")?;
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            13
        );
        drop(resumed_stream);

        ctx.register_table(
            "filtered_live",
            Arc::new(
                FlussLogTable::open_with_options(
                    Arc::clone(&connection),
                    path.clone(),
                    LogReadOptions {
                        start: LogStart::Latest,
                        ..LogReadOptions::default()
                    },
                )
                .await?,
            ),
        )?;
        let filtered = ctx
            .sql("SELECT id FROM filtered_live WHERE id >= 14")
            .await?;
        let plan = filtered.create_physical_plan().await?;
        let mut filtered_stream = plan.execute(0, Arc::new(filtered.task_ctx()))?;
        assert!(
            tokio::time::timeout(Duration::from_millis(850), filtered_stream.next())
                .await
                .is_err()
        );
        row.set_field(0, 14_i32);
        writer.append(&row)?;
        writer.flush().await?;
        let batch = tokio::time::timeout(Duration::from_secs(10), filtered_stream.next())
            .await?
            .expect("filtered streaming query unexpectedly ended")?;
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            14
        );
        drop(filtered_stream);
        check_continuous_pressure(&connection, &path).await?;
        Ok(())
    }
    .await;
    let cleanup = admin.drop_table(&path, true).await;
    result?;
    cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn check_continuous_pressure(
    connection: &Arc<FlussConnection>,
    path: &TablePath,
) -> TestResult<()> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(1),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        ),
    );
    let provider = FlussLogTable::open_with_options(
        Arc::clone(connection),
        path.clone(),
        LogReadOptions {
            start: LogStart::Latest,
            ..LogReadOptions::default()
        },
    )
    .await?;
    ctx.register_table("pressure", Arc::new(provider))?;
    let query = ctx.sql("SELECT id FROM pressure").await?;
    let source = source_plan(&query.create_physical_plan().await?);
    let mut stream = source.execute(0, Arc::new(query.task_ctx()))?;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), stream.next())
            .await
            .is_err()
    );
    let producer_connection = Arc::clone(connection);
    let producer_path = path.clone();
    let producer = tokio::spawn(async move {
        let table = producer_connection.get_table(&producer_path).await?;
        let writer = table.new_append()?.create_writer()?;
        for group in 0..20 {
            for offset in 0..5 {
                let mut row = GenericRow::new(1);
                row.set_field(0, 1000_i32 + group * 5 + offset);
                writer.append(&row)?;
            }
            writer.flush().await?;
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        Ok::<_, fluss::error::Error>(())
    });
    let consumed: TestResult<()> = async {
        let mut ids = HashSet::new();
        while ids.len() < 100 {
            let batch = tokio::time::timeout(Duration::from_secs(12), stream.next())
                .await?
                .ok_or("continuous source ended under pressure")??;
            let held = pool.reserved();
            assert!(held > 0 && held <= 1024 * 1024);
            tokio::time::sleep(Duration::from_millis(70)).await;
            assert_eq!(
                pool.reserved(),
                held,
                "producer activity must not decode more Arrow behind a paused consumer"
            );
            for id in batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
            {
                assert!(ids.insert(*id), "pressure scan duplicated {id}");
            }
            drop(batch);
            assert_eq!(
                pool.reserved(),
                0,
                "single-batch pull must not retain decoded queued batches"
            );
        }
        assert_eq!(ids, (1000..1100).collect());
        Ok(())
    }
    .await;
    drop(stream);
    if consumed.is_err() {
        producer.abort();
    }
    let produced = producer.await;
    consumed?;
    produced??;
    assert_eq!(pool.reserved(), 0);
    assert_eq!(
        metric(&source.metrics().unwrap(), "fluss_active_partition_streams"),
        0
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; creates isolated partitions"]
async fn streaming_partition_topology_change_fails_explicitly() -> TestResult<()> {
    let connection = connect().await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let path = TablePath::new(DATABASE, format!("stream_part_{suffix}"));
    let admin = connection.get_admin()?;
    admin
        .create_table(
            &path,
            &TableDescriptor::builder()
                .schema(
                    Schema::builder()
                        .column("id", DataTypes::int())
                        .column("region", DataTypes::string())
                        .build()?,
                )
                .partitioned_by(vec!["region"])
                .distributed_by(Some(2), vec!["id".to_string()])
                .build()?,
            false,
        )
        .await?;
    let result: TestResult<()> = async {
        create_ready_partition(&admin, &path, "north").await?;
        admin
            .alter_table(
                &path,
                false,
                AlterTableChanges {
                    modify_bucket_count: Some(3),
                    ..Default::default()
                },
            )
            .await?;
        create_ready_partition(&admin, &path, "south").await?;
        assert_eq!(partition_bucket_count(&admin, &path, "north").await?, 2);
        assert_eq!(partition_bucket_count(&admin, &path, "south").await?, 3);
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        ctx.register_table(
            "live",
            Arc::new(
                FlussLogTable::open_with_options(
                    Arc::clone(&connection),
                    path.clone(),
                    LogReadOptions::default(),
                )
                .await?,
            ),
        )?;
        let query = ctx.sql("SELECT id FROM live").await?;
        let plan = query.create_physical_plan().await?;
        let mut stream = source_plan(&plan).execute(0, Arc::new(query.task_ctx()))?;
        assert!(
            tokio::time::timeout(Duration::from_millis(900), stream.next())
                .await
                .is_err()
        );
        let table = connection.get_table(&path).await?;
        let writer = table.new_append()?.create_writer()?;
        for (id, region) in [(1_i32, "north"), (2_i32, "south")] {
            let mut row = GenericRow::new(2);
            row.set_field(0, id);
            row.set_field(1, region);
            writer.append(&row)?;
            writer.flush().await?;
            let batch = tokio::time::timeout(Duration::from_secs(12), stream.next())
                .await?
                .ok_or("partitioned stream unexpectedly completed")??;
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                id,
            );
        }
        create_ready_partition(&admin, &path, "west").await?;
        let error = tokio::time::timeout(Duration::from_secs(12), stream.next())
            .await?
            .expect("stream must explicitly fail when a new partition appears")
            .expect_err("stream must not skip a new partition");
        assert!(
            error.to_string().contains("partition topology changed"),
            "{error}"
        );
        drop(stream);
        Ok(())
    }
    .await;
    let cleanup = admin.drop_table(&path, true).await;
    result?;
    cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn connect() -> TestResult<Arc<FlussConnection>> {
    let connection = Arc::new(
        FlussConnection::new(Config {
            bootstrap_servers: std::env::var("FLUSS_BOOTSTRAP")?,
            security_ssl_enabled: true,
            security_ssl_ca_file: Some(std::env::var("FLUSS_CA_FILE")?),
            security_protocol: "sasl".into(),
            security_sasl_username: std::env::var("FLUSS_USER")?,
            security_sasl_password: std::env::var("FLUSS_PASSWORD")?,
            ..Config::default()
        })
        .await?,
    );
    connection
        .get_admin()?
        .create_database(DATABASE, None, true)
        .await?;
    Ok(connection)
}

async fn register_log(
    ctx: &SessionContext,
    connection: &Arc<FlussConnection>,
    name: &str,
    timeout: Duration,
) -> TestResult<()> {
    let table = FlussLogTable::open(
        Arc::clone(connection),
        TablePath::new(DATABASE, name),
        timeout,
    )
    .await?;
    ctx.register_table("log", Arc::new(table))?;
    Ok(())
}

fn source_plan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    fn find(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.name() == "FlussScanExec" {
            return Some(Arc::clone(plan));
        }
        plan.children().into_iter().find_map(find)
    }
    find(plan).expect("SQL must plan a Fluss stream")
}

fn single_count(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) returns Int64")
        .value(0)
}

async fn count_sql(ctx: &SessionContext, sql: &str) -> TestResult<i64> {
    Ok(single_count(&ctx.sql(sql).await?.collect().await?))
}

async fn explain_text(ctx: &SessionContext, sql: &str) -> TestResult<String> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches
        .iter()
        .map(|batch| {
            let plans = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("EXPLAIN plan is a string");
            (0..plans.len())
                .map(|i| plans.value(i))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

fn metric(metrics: &MetricsSet, name: &str) -> usize {
    metrics
        .sum_by_name(name)
        .expect("metric must be registered")
        .as_usize()
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; run with --ignored"]
async fn bounded_log_and_kv_sql_against_native_sni() -> TestResult<()> {
    let connection = connect().await?;
    let (log_path, kv_path) = create_known_tables(&connection).await?;
    let ctx = SessionContext::new();
    let result = async {
        register_log(&ctx, &connection, log_path.table(), SCAN_TIMEOUT).await?;
        let total = check_log_sql(&ctx).await?;
        check_source_projection(&ctx).await?;
        check_parallelism(&connection, log_path.table(), total).await?;
        check_explain_metrics(&ctx).await?;
        check_catalog(&ctx, &connection, &log_path, total).await?;
        check_timeout_and_kv(&connection, &log_path, &kv_path).await?;
        check_kv_sql(&ctx, &connection, &log_path, &kv_path).await?;
        check_log_progress(&connection, &log_path).await
    }
    .await;

    drop(ctx);
    let admin = connection.get_admin()?;
    let log_cleanup = admin.drop_table(&log_path, true).await;
    let kv_cleanup = admin.drop_table(&kv_path, true).await;
    result?;
    log_cleanup?;
    kv_cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn create_known_tables(
    connection: &Arc<FlussConnection>,
) -> TestResult<(TablePath, TablePath)> {
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let log = TablePath::new(DATABASE, format!("log_{suffix}"));
    let kv = TablePath::new(DATABASE, format!("kv_{suffix}"));
    let admin = connection.get_admin()?;
    let log_schema = Schema::builder()
        .column("run_id", DataTypes::string())
        .column("id", DataTypes::int())
        .column("value", DataTypes::string())
        .build()?;
    let log_descriptor = TableDescriptor::builder()
        .schema(log_schema)
        .distributed_by(Some(2), vec!["id".to_string()])
        .property("table.statistics.columns", "id")
        .build()?;
    admin.create_table(&log, &log_descriptor, false).await?;
    let kv_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("value", DataTypes::string())
                .primary_key(vec!["id"])?
                .build()?,
        )
        .distributed_by(Some(2), vec!["id".to_string()])
        .build()?;
    if let Err(error) = admin.create_table(&kv, &kv_descriptor, false).await {
        admin.drop_table(&log, true).await?;
        return Err(error.into());
    }
    let write_result: TestResult<()> = async {
        let table = connection.get_table(&log).await?;
        let writer = table.new_append()?.create_writer()?;
        for run in 0..3 {
            for (id, value) in [(1, "hola"), (2, "desde"), (3, "pyspark")] {
                let mut row = GenericRow::new(3);
                row.set_field(0, format!("fixture-{run}"));
                row.set_field(1, id);
                row.set_field(2, value);
                writer.append(&row)?;
            }
            writer.flush().await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = write_result {
        let _ = admin.drop_table(&log, true).await;
        let _ = admin.drop_table(&kv, true).await;
        return Err(error);
    }
    Ok((log, kv))
}

async fn check_log_progress(connection: &Arc<FlussConnection>, path: &TablePath) -> TestResult<()> {
    use fluss_datafusion::{LogProgress, LogTermination};
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    let provider = FlussLogTable::open(Arc::clone(connection), path.clone(), SCAN_TIMEOUT).await?;
    let mut observer = provider.subscribe_progress();
    ctx.register_table("progress_log", Arc::new(provider))?;
    let full = ctx
        .sql("SELECT id FROM progress_log")
        .await?
        .collect()
        .await?;
    let full_rows: usize = full.iter().map(RecordBatch::num_rows).sum();
    let mut offered = 0;
    while let Ok(event) = observer.try_recv() {
        match event {
            LogProgress::Offered(batch) => offered += batch.rows,
            LogProgress::Excluded { .. } => {
                panic!("fetch completion must not exclude unoffered queued data")
            }
            _ => {}
        }
    }
    assert_eq!(offered, full_rows);
    let result = ctx
        .sql("SELECT id FROM progress_log WHERE id > 1000000")
        .await?
        .collect()
        .await?;
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    let mut starts = HashMap::new();
    let mut stops = HashMap::new();
    let mut next = HashMap::new();
    let mut initialized = HashSet::new();
    let mut completed = HashSet::new();
    let mut identity = None;
    while let Ok(event) = observer.try_recv() {
        match event {
            LogProgress::Initialized {
                execution_id,
                partition,
                partitions,
                positions,
                ..
            } => {
                if let Some(previous) = identity {
                    assert_eq!(previous, execution_id);
                }
                identity = Some(execution_id);
                assert_eq!(partitions, 2);
                assert!(initialized.insert(partition));
                for position in positions {
                    starts.insert(position.bucket.clone(), position.start_offset);
                    next.insert(position.bucket.clone(), position.start_offset);
                    stops.insert(position.bucket, position.stop_offset.unwrap());
                }
            }
            LogProgress::Excluded {
                bucket,
                base_offset,
                next_offset,
                ..
            } => {
                assert_eq!(next[&bucket], base_offset);
                assert!(next_offset >= base_offset && next_offset <= stops[&bucket]);
                next.insert(bucket, next_offset);
            }
            LogProgress::Offered(_) => panic!("all-pruned source must not offer value batches"),
            LogProgress::Terminated {
                partition, status, ..
            } => {
                assert_eq!(status, LogTermination::Completed);
                completed.insert(partition);
            }
        }
    }
    assert_eq!(initialized.len(), 2);
    assert_eq!(completed, initialized);
    assert_eq!(stops.len(), 2);
    assert_eq!(next, stops);
    assert!(stops.values().sum::<i64>() > starts.values().sum::<i64>());

    let table = connection.get_table(path).await?;
    let writer = table.new_append()?.create_writer()?;
    let mut row = GenericRow::new(3);
    row.set_field(0, "progress-resume");
    row.set_field(1, 1_000_001_i32);
    row.set_field(2, "new");
    writer.append(&row)?;
    writer.flush().await?;
    let resumed = FlussLogTable::open_with_options(
        Arc::clone(connection),
        path.clone(),
        LogReadOptions {
            start: LogStart::Offsets(next.clone()),
            ..LogReadOptions::batch(SCAN_TIMEOUT)
        },
    )
    .await?;
    ctx.register_table("resumed_progress", Arc::new(resumed))?;
    let rows = ctx
        .sql("SELECT id FROM resumed_progress WHERE id > 1000000")
        .await?
        .collect()
        .await?;
    let ids: Vec<_> = rows
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, [1_000_001]);
    let missing = next.keys().next().unwrap().clone();
    next.remove(&missing);
    let incomplete = FlussLogTable::open_with_options(
        Arc::clone(connection),
        path.clone(),
        LogReadOptions {
            start: LogStart::Offsets(next),
            ..LogReadOptions::batch(SCAN_TIMEOUT)
        },
    )
    .await?;
    ctx.register_table("incomplete_progress", Arc::new(incomplete))?;
    assert!(
        ctx.sql("SELECT * FROM incomplete_progress")
            .await?
            .collect()
            .await
            .is_err()
    );
    Ok(())
}

async fn check_kv_sql(
    ctx: &SessionContext,
    connection: &Arc<FlussConnection>,
    log_path: &TablePath,
    kv_path: &TablePath,
) -> TestResult<()> {
    assert!(
        FlussKvTable::open(Arc::clone(connection), log_path.clone(), SCAN_TIMEOUT)
            .await
            .is_err(),
        "KV scans must reject append-only logs"
    );
    let table = FlussKvTable::open(Arc::clone(connection), kv_path.clone(), SCAN_TIMEOUT).await?;
    ctx.register_table("kv", Arc::new(table))?;
    assert_eq!(count_sql(ctx, "SELECT COUNT(*) FROM kv").await?, 0);

    let writer = connection
        .get_table(kv_path)
        .await?
        .new_upsert()?
        .create_writer()?;
    for id in 0..64 {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, format!("value-{id}"));
        writer.upsert(&row)?;
    }
    writer.flush().await?;
    assert_eq!(count_sql(ctx, "SELECT COUNT(*) FROM kv").await?, 64);

    let mut update = GenericRow::new(2);
    update.set_field(0, 3_i32);
    update.set_field(1, "updated");
    writer.upsert(&update)?;
    for id in [4_i32, 61_i32] {
        let mut key = GenericRow::new(2);
        key.set_field(0, id);
        writer.delete(&key)?;
    }
    writer.flush().await?;
    assert_eq!(count_sql(ctx, "SELECT COUNT(*) FROM kv").await?, 62);
    assert_eq!(
        count_sql(ctx, "SELECT COUNT(*) FROM kv WHERE id = 4").await?,
        0
    );
    assert_eq!(
        count_sql(ctx, "SELECT COUNT(*) FROM kv WHERE id = 61").await?,
        0
    );
    assert_eq!(
        count_sql(
            ctx,
            "SELECT COUNT(*) FROM kv WHERE id = 3 AND value = 'updated'"
        )
        .await?,
        1
    );
    assert_eq!(
        count_sql(ctx, "SELECT COUNT(*) FROM kv WHERE id > 60").await?,
        2
    );

    let projected = ctx.sql("SELECT value FROM kv WHERE id = 3").await?;
    let plan = projected.create_physical_plan().await?;
    let source = source_plan(&plan);
    assert_eq!(
        source.schema().fields().len(),
        2,
        "residual filter needs the id column"
    );
    let rows = collect_plan(plan, Arc::new(projected.task_ctx())).await?;
    let values: Vec<_> = rows
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| values.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(values, ["updated"]);

    assert_eq!(
        count_sql(
            ctx,
            &format!("SELECT COUNT(*) FROM fluss.{DATABASE}.{}", kv_path.table()),
        )
        .await?,
        62,
        "catalog must dispatch KV tables to the KV provider"
    );
    for (target, expected) in [(1, 1), (8, 2)] {
        let query =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(target));
        query.register_table(
            "kv",
            Arc::new(
                FlussKvTable::open(Arc::clone(connection), kv_path.clone(), SCAN_TIMEOUT).await?,
            ),
        )?;
        let plan = query
            .sql("SELECT * FROM kv")
            .await?
            .create_physical_plan()
            .await?;
        assert_eq!(
            source_plan(&plan).output_partitioning().partition_count(),
            expected
        );
        assert_eq!(count_sql(&query, "SELECT COUNT(*) FROM kv").await?, 62);
    }

    // The snapshot may span more than one ScanKv response. Count every row,
    // including continuations; dropping a limited scan must not poison the next.
    let large = "x".repeat(350_000);
    for id in 64..72 {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, format!("{id}-{large}"));
        writer.upsert(&row)?;
    }
    writer.flush().await?;
    let all = ctx.sql("SELECT * FROM kv").await?;
    let plan = all.create_physical_plan().await?;
    let batches = collect_plan(plan, Arc::new(all.task_ctx())).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 70);
    assert!(
        batches.len() > 2,
        "KV snapshot must span multiple RPC pages"
    );
    let limited = ctx
        .sql("SELECT id FROM kv LIMIT 1")
        .await?
        .collect()
        .await?;
    assert_eq!(limited.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(count_sql(ctx, "SELECT COUNT(*) FROM kv").await?, 70);

    check_memory_pool(connection, log_path, kv_path).await?;

    let parallel = ctx.sql("SELECT * FROM kv").await?;
    let source = source_plan(&parallel.create_physical_plan().await?);
    let reads = (0..4).map(|_| collect_plan(Arc::clone(&source), Arc::new(parallel.task_ctx())));
    for result in futures::future::try_join_all(reads).await? {
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 70);
    }
    assert_eq!(
        metric(&source.metrics().unwrap(), "fluss_active_partition_streams"),
        0
    );

    let timed = SessionContext::new();
    timed.register_table(
        "kv",
        Arc::new(
            FlussKvTable::open(
                Arc::clone(connection),
                kv_path.clone(),
                Duration::from_nanos(1),
            )
            .await?,
        ),
    )?;
    assert!(
        timed
            .sql("SELECT COUNT(*) FROM kv")
            .await?
            .collect()
            .await
            .is_err()
    );
    Ok(())
}

async fn check_memory_pool(
    connection: &Arc<FlussConnection>,
    log: &TablePath,
    kv: &TablePath,
) -> TestResult<()> {
    let tiny_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
    let tiny = SessionContext::new_with_config_rt(
        SessionConfig::new(),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&tiny_pool))
                .build()?,
        ),
    );
    tiny.register_table(
        "log",
        Arc::new(FlussLogTable::open(Arc::clone(connection), log.clone(), SCAN_TIMEOUT).await?),
    )?;
    tiny.register_table(
        "kv",
        Arc::new(FlussKvTable::open(Arc::clone(connection), kv.clone(), SCAN_TIMEOUT).await?),
    )?;
    for table in ["log", "kv"] {
        let query = tiny.sql(&format!("SELECT * FROM {table}")).await?;
        let plan = query.create_physical_plan().await?;
        let source = source_plan(&plan);
        let error = collect_plan(plan, Arc::new(query.task_ctx()))
            .await
            .expect_err("the scan must respect the query's shared memory pool");
        assert!(
            error.to_string().to_lowercase().contains("memory"),
            "{error}"
        );
        assert_eq!(tiny_pool.reserved(), 0);
        tokio::time::timeout(Duration::from_secs(2), async {
            while metric(&source.metrics().unwrap(), "fluss_active_partition_streams") != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert_eq!(
            metric(&source.metrics().unwrap(), "fluss_active_partition_streams"),
            0
        );
    }

    // Provider retention policy must reject an oversized batch even when the
    // DataFusion pool is unbounded. This is post-decode admission, not an RSS cap.
    let bounded = SessionContext::new();
    bounded.register_table(
        "log",
        Arc::new(
            FlussLogTable::open(Arc::clone(connection), log.clone(), SCAN_TIMEOUT)
                .await?
                .with_max_retained_batch_bytes(1)?,
        ),
    )?;
    bounded.register_table(
        "kv",
        Arc::new(
            FlussKvTable::open(Arc::clone(connection), kv.clone(), SCAN_TIMEOUT)
                .await?
                .with_max_retained_batch_bytes(1)?,
        ),
    )?;
    for table in ["log", "kv"] {
        let query = bounded.sql(&format!("SELECT * FROM {table}")).await?;
        let plan = query.create_physical_plan().await?;
        let source = source_plan(&plan);
        let error = collect_plan(plan, Arc::new(query.task_ctx()))
            .await
            .expect_err("retention ceiling must apply independently of the host pool");
        assert!(
            error.to_string().contains("max_retained_batch_bytes=1"),
            "{error}"
        );
        assert_eq!(
            metric(
                &source.metrics().unwrap(),
                "fluss_retained_source_buffer_bytes"
            ),
            0
        );
    }

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    // Executing a late source partition must not restart the scan budget,
    // even when the earlier partition has not yet polled its stream.
    let deadline_ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    let deadline_budget = Duration::from_millis(100);
    deadline_ctx.register_table(
        "log",
        Arc::new(FlussLogTable::open(Arc::clone(connection), log.clone(), deadline_budget).await?),
    )?;
    deadline_ctx.register_table(
        "kv",
        Arc::new(FlussKvTable::open(Arc::clone(connection), kv.clone(), deadline_budget).await?),
    )?;
    for table in ["log", "kv"] {
        let query = deadline_ctx.sql(&format!("SELECT * FROM {table}")).await?;
        let source = source_plan(&query.create_physical_plan().await?);
        assert!(source.output_partitioning().partition_count() > 1);
        let context = Arc::new(query.task_ctx());
        let early = source.execute(0, Arc::clone(&context))?;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut late = source.execute(1, context)?;
        let error = late
            .next()
            .await
            .expect("late source partition returns a deadline error")
            .expect_err("late partition must not receive a new timeout budget");
        assert!(error.to_string().contains("timed out"), "{error}");
        let datafusion::common::DataFusionError::External(cause) = &error else {
            panic!("source deadline must preserve an inspectable cause: {error}");
        };
        assert!(
            cause
                .downcast_ref::<fluss_datafusion::FlussScanTimeout>()
                .is_some()
        );
        drop(late);
        drop(early);
    }
    let slow = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(1),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        ),
    );
    slow.register_table(
        "kv",
        Arc::new(FlussKvTable::open(Arc::clone(connection), kv.clone(), SCAN_TIMEOUT).await?),
    )?;
    let query = slow.sql("SELECT * FROM kv").await?;
    let plan = query.create_physical_plan().await?;
    let source = source_plan(&plan);
    let mut stream = source.execute(0, Arc::new(query.task_ctx()))?;
    let first = stream.next().await.expect("first KV page")?;
    assert!(first.num_rows() > 0);
    let held = pool.reserved();
    assert!(held > 0 && held <= 8 * 1024 * 1024);
    assert_eq!(
        metric(
            &source.metrics().unwrap(),
            "fluss_retained_source_buffer_bytes"
        ),
        held
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        pool.reserved(),
        held,
        "a stalled consumer must not pull more pages"
    );
    drop(stream);
    assert_eq!(
        pool.reserved(),
        held,
        "retained Arrow data still owns the reservation"
    );
    drop(first);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(
        metric(
            &source.metrics().unwrap(),
            "fluss_retained_source_buffer_bytes"
        ),
        0
    );
    let count_query = slow.sql("SELECT COUNT(*) FROM kv").await?;
    let count_source = source_plan(&count_query.create_physical_plan().await?);
    let count_batches =
        collect_plan(Arc::clone(&count_source), Arc::new(count_query.task_ctx())).await?;
    assert!(
        count_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
            > 0
    );
    assert!(count_batches.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(
        metric(&count_source.metrics().unwrap(), "arrow_decoded_bytes"),
        0,
        "KV COUNT must not build unrequested Arrow value columns"
    );
    assert_eq!(
        metric(&source.metrics().unwrap(), "fluss_active_partition_streams"),
        0
    );

    // Distinct queries against the same provider share DataFusion's pool.
    // Pausing both consumers retains two decoded-batch reservations, while
    // retained buffers, rather than the next pull/stream drop, own the leases.
    let shared_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let shared = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(1),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&shared_pool))
                .build()?,
        ),
    );
    shared.register_table(
        "kv",
        Arc::new(FlussKvTable::open(Arc::clone(connection), kv.clone(), SCAN_TIMEOUT).await?),
    )?;
    let query = shared.sql("SELECT * FROM kv").await?;
    let source = source_plan(&query.create_physical_plan().await?);
    let mut first = source.execute(0, Arc::new(query.task_ctx()))?;
    let mut second = source.execute(0, Arc::new(query.task_ctx()))?;
    let first_batch = first.next().await.expect("first query page")?;
    assert!(first_batch.num_rows() > 0);
    let first_reserved = shared_pool.reserved();
    assert!(first_reserved > 0);
    let second_batch = second.next().await.expect("second query page")?;
    assert!(second_batch.num_rows() > 0);
    let both_reserved = shared_pool.reserved();
    assert!(both_reserved > first_reserved);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(shared_pool.reserved(), both_reserved);
    drop(first);
    assert_eq!(shared_pool.reserved(), both_reserved);
    drop(first_batch);
    assert!(shared_pool.reserved() > 0 && shared_pool.reserved() < both_reserved);
    drop(second);
    assert!(shared_pool.reserved() > 0);
    drop(second_batch);
    assert_eq!(shared_pool.reserved(), 0);
    for _ in 0..10 {
        let mut stream = source.execute(0, Arc::new(query.task_ctx()))?;
        assert!(
            stream
                .next()
                .await
                .expect("restarted query page")?
                .num_rows()
                > 0
        );
        drop(stream);
        assert_eq!(
            shared_pool.reserved(),
            0,
            "cancelled query retained a reservation"
        );
        assert_eq!(
            metric(&source.metrics().unwrap(), "fluss_active_partition_streams"),
            0
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; run with --ignored"]
async fn kv_snapshot_isolation_and_schema_evolution() -> TestResult<()> {
    let connection = connect().await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let path = TablePath::new(DATABASE, format!("kv_snapshot_{suffix}"));
    let admin = connection.get_admin()?;
    admin
        .create_table(
            &path,
            &TableDescriptor::builder()
                .schema(
                    Schema::builder()
                        .column("id", DataTypes::int())
                        .column("value", DataTypes::string())
                        .primary_key(vec!["id"])?
                        .build()?,
                )
                .distributed_by(Some(1), vec!["id".to_string()])
                .build()?,
            false,
        )
        .await?;
    let result = check_kv_snapshot(&connection, &path).await;
    let cleanup = admin.drop_table(&path, true).await;
    result?;
    cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn check_kv_snapshot(connection: &Arc<FlussConnection>, path: &TablePath) -> TestResult<()> {
    let table = connection.get_table(path).await?;
    let table_id = table.get_table_info().table_id;
    let writer = table.new_upsert()?.create_writer()?;
    let payload = "x".repeat(250_000);
    for id in 0..12 {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, format!("old-{id}-{payload}"));
        writer.upsert(&row)?;
    }
    writer.flush().await?;

    let mut reader = table
        .new_scan()
        .create_kv_batch_scanner(TableBucket::new(table_id, 0))?;
    let first = reader.next_batch().await?.expect("first snapshot page");
    assert!(first.num_rows() > 0 && first.num_rows() < 12);

    let mut updated = GenericRow::new(2);
    updated.set_field(0, 11_i32);
    updated.set_field(1, "new-11");
    writer.upsert(&updated)?;
    let mut deleted = GenericRow::new(2);
    deleted.set_field(0, 10_i32);
    writer.delete(&deleted)?;
    let mut inserted = GenericRow::new(2);
    inserted.set_field(0, 13_i32);
    inserted.set_field(1, "new-13");
    writer.upsert(&inserted)?;
    writer.flush().await?;

    let mut pages = vec![first];
    while let Some(batch) = reader.next_batch().await? {
        pages.push(batch);
    }
    assert!(pages.len() > 1);
    assert_eq!(reader.stats().sessions_opened, 1);
    assert_eq!(reader.stats().pages_received, pages.len());
    let mut ids = Vec::new();
    for page in pages {
        let keys = page
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = page
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..page.num_rows() {
            let id = keys.value(row);
            assert!(values.value(row).starts_with(&format!("old-{id}-")));
            ids.push(id);
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..12).collect::<Vec<_>>());

    let mut interrupted = table
        .new_scan()
        .create_kv_batch_scanner(TableBucket::new(table_id, 0))?;
    let first = interrupted.next_batch().await?.expect("first page");
    assert!(first.num_rows() < 12);
    let mut pending = Box::pin(interrupted.next_batch());
    assert!(matches!(
        futures::poll!(pending.as_mut()),
        std::task::Poll::Pending
    ));
    drop(pending);
    assert!(
        interrupted.next_batch().await.is_err(),
        "a cancelled continuation must not skip or duplicate a page"
    );
    drop(interrupted);
    let mut fresh = table
        .new_scan()
        .create_kv_batch_scanner(TableBucket::new(table_id, 0))?;
    let mut count = 0;
    while let Some(batch) = fresh.next_batch().await? {
        count += batch.num_rows();
    }
    assert_eq!(count, 12, "a fresh session must still read the whole state");

    // Old value records remain readable after an additive schema change;
    // only the newly written row has the added nullable column populated.
    connection
        .get_admin()?
        .alter_table(
            path,
            false,
            AlterTableChanges {
                add_columns: vec![AddColumn {
                    column_name: "note".into(),
                    data_type_json: DataTypes::string()
                        .serialize_json()?
                        .to_string()
                        .into_bytes(),
                    comment: None,
                    position: ColumnPositionType::Last,
                }],
                ..Default::default()
            },
        )
        .await?;
    let ctx = SessionContext::new();
    ctx.register_table(
        "kv",
        Arc::new(FlussKvTable::open(Arc::clone(connection), path.clone(), SCAN_TIMEOUT).await?),
    )?;
    assert_eq!(
        count_sql(&ctx, "SELECT COUNT(*) FROM kv WHERE note IS NULL").await?,
        12
    );
    let mut new = GenericRow::new(3);
    new.set_field(0, 14_i32);
    new.set_field(1, "new-14");
    new.set_field(2, "present");
    connection
        .get_table(path)
        .await?
        .new_upsert()?
        .create_writer()?
        .upsert(&new)?
        .await?;
    assert_eq!(
        count_sql(&ctx, "SELECT COUNT(*) FROM kv WHERE note = 'present'").await?,
        1
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; run with --ignored"]
async fn partitioned_logs_and_kv_discover_each_execution() -> TestResult<()> {
    let connection = connect().await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let log = TablePath::new(DATABASE, format!("partition_log_{suffix}"));
    let kv = TablePath::new(DATABASE, format!("partition_kv_{suffix}"));
    let admin = connection.get_admin()?;
    let schema = || {
        Schema::builder()
            .column("id", DataTypes::int())
            .column("region", DataTypes::string())
            .column("value", DataTypes::string())
    };
    admin
        .create_table(
            &log,
            &TableDescriptor::builder()
                .schema(schema().build()?)
                .partitioned_by(vec!["region"])
                .distributed_by(Some(2), vec!["id".to_string()])
                .build()?,
            false,
        )
        .await?;
    let kv_descriptor = TableDescriptor::builder()
        .schema(schema().primary_key(vec!["region", "id"])?.build()?)
        .partitioned_by(vec!["region"])
        .distributed_by(Some(2), vec!["id".to_string()])
        .build()?;
    if let Err(error) = admin.create_table(&kv, &kv_descriptor, false).await {
        admin.drop_table(&log, true).await?;
        return Err(error.into());
    }
    let result = check_partitioned_sql(&connection, &log, &kv).await;
    let log_cleanup = admin.drop_table(&log, true).await;
    let kv_cleanup = admin.drop_table(&kv, true).await;
    result?;
    log_cleanup?;
    kv_cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn check_partitioned_sql(
    connection: &Arc<FlussConnection>,
    log: &TablePath,
    kv: &TablePath,
) -> TestResult<()> {
    let admin = connection.get_admin()?;
    for name in ["north", "south"] {
        create_ready_partition(&admin, log, name).await?;
        create_ready_partition(&admin, kv, name).await?;
    }
    let ctx = SessionContext::new();
    ctx.register_table(
        "plog",
        Arc::new(FlussLogTable::open(Arc::clone(connection), log.clone(), SCAN_TIMEOUT).await?),
    )?;
    ctx.register_table(
        "pkv",
        Arc::new(FlussKvTable::open(Arc::clone(connection), kv.clone(), SCAN_TIMEOUT).await?),
    )?;
    assert_eq!(count_sql(&ctx, "SELECT COUNT(*) FROM plog").await?, 0);
    assert_eq!(count_sql(&ctx, "SELECT COUNT(*) FROM pkv").await?, 0);

    let log_table = connection.get_table(log).await?;
    let log_writer = log_table.new_append()?.create_writer()?;
    let kv_table = connection.get_table(kv).await?;
    let kv_writer = kv_table.new_upsert()?.create_writer()?;
    for (id, region) in [(1, "north"), (2, "north"), (3, "south"), (4, "south")] {
        let mut row = GenericRow::new(3);
        row.set_field(0, id);
        row.set_field(1, region);
        row.set_field(2, format!("value-{id}"));
        log_writer.append(&row)?;
        kv_writer.upsert(&row)?;
    }
    log_writer.flush().await?;
    kv_writer.flush().await?;
    let capped = SessionContext::new();
    capped.register_table(
        "plog",
        Arc::new(
            FlussLogTable::open(Arc::clone(connection), log.clone(), SCAN_TIMEOUT)
                .await?
                .with_max_assigned_buckets(3)?,
        ),
    )?;
    capped.register_table(
        "pkv",
        Arc::new(
            FlussKvTable::open(Arc::clone(connection), kv.clone(), SCAN_TIMEOUT)
                .await?
                .with_max_assigned_buckets(3)?,
        ),
    )?;
    for table in ["plog", "pkv"] {
        assert!(
            capped
                .sql(&format!("SELECT COUNT(*) FROM {table}"))
                .await?
                .collect()
                .await
                .is_err()
        );
        assert_eq!(
            count_sql(
                &capped,
                &format!("SELECT COUNT(*) FROM {table} WHERE region = 'north'")
            )
            .await?,
            2
        );
    }
    for table in ["plog", "pkv"] {
        assert_eq!(
            count_sql(&ctx, &format!("SELECT COUNT(*) FROM {table}")).await?,
            4
        );
        assert_eq!(
            count_sql(
                &ctx,
                &format!("SELECT COUNT(*) FROM {table} WHERE region = 'north'")
            )
            .await?,
            2
        );
        assert_eq!(
            count_sql(
                &ctx,
                &format!("SELECT COUNT(*) FROM {table} WHERE 'south' = region")
            )
            .await?,
            2
        );
        assert_eq!(
            count_sql(
                &ctx,
                &format!("SELECT COUNT(*) FROM {table} WHERE region = 'missing'")
            )
            .await?,
            0
        );
        assert_eq!(
            count_sql(
                &ctx,
                &format!("SELECT COUNT(*) FROM {table} WHERE region = 'north' OR id = 4")
            )
            .await?,
            3
        );
    }
    for (table, kind) in [("plog", "kind=log"), ("pkv", "kind=kv_snapshot")] {
        let query = ctx
            .sql(&format!("SELECT id FROM {table} WHERE region = 'north'"))
            .await?;
        let plan = query.create_physical_plan().await?;
        let source = source_plan(&plan);
        let rows = collect_plan(plan, Arc::new(query.task_ctx())).await?;
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let metrics = source.metrics().unwrap();
        assert_eq!(metric(&metrics, "fluss_partitions_discovered"), 2);
        assert_eq!(metric(&metrics, "fluss_partitions_selected"), 1);
        assert_eq!(metric(&metrics, "fluss_buckets_assigned"), 2);
        if table == "pkv" {
            assert!(metric(&metrics, "kv_sessions_opened") > 0);
            assert!(metric(&metrics, "kv_pages_received") > 0);
        }
        let explain = explain_text(
            &ctx,
            &format!("EXPLAIN ANALYZE SELECT id FROM {table} WHERE region = 'north'"),
        )
        .await?;
        assert!(explain.contains(kind), "missing scan kind in {explain}");
        assert!(
            explain.contains(if table == "pkv" {
                "projection=decoder"
            } else {
                "projection=server"
            }),
            "missing projection location in {explain}"
        );
        assert!(explain.contains("projected_columns"));
        assert!(explain.contains("fluss_partitions_selected"));
        if table == "pkv" {
            assert!(explain.contains("kv_first_page_time"));
        }
    }

    // Reuse exactly the same physical sources, not just the same providers.
    // Start partition 1 first: discovery metrics must not depend on stream 0.
    let log_df = ctx.sql("SELECT * FROM plog").await?;
    let log_source = source_plan(&log_df.create_physical_plan().await?);
    let kv_df = ctx.sql("SELECT * FROM pkv").await?;
    let kv_source = source_plan(&kv_df.create_physical_plan().await?);
    let delayed_log = Arc::new(log_df.task_ctx());
    let delayed_kv = Arc::new(kv_df.task_ctx());
    let first_log = collect_stream(log_source.execute(1, Arc::clone(&delayed_log))?).await?;
    let first_kv = collect_stream(kv_source.execute(1, Arc::clone(&delayed_kv))?).await?;
    assert_eq!(
        metric(
            &log_source.metrics().unwrap(),
            "fluss_partitions_discovered"
        ),
        2
    );
    assert_eq!(
        metric(&kv_source.metrics().unwrap(), "fluss_partitions_discovered"),
        2
    );

    // Old partitions retain two buckets after table defaults change; a new
    // partition receives three. The already-started execution still uses its
    // captured topology, while the same plan sees all buckets on the next run.
    for path in [log, kv] {
        admin
            .alter_table(
                path,
                false,
                AlterTableChanges {
                    modify_bucket_count: Some(3),
                    ..Default::default()
                },
            )
            .await?;
        for name in ["north", "south"] {
            assert_eq!(partition_bucket_count(&admin, path, name).await?, 2);
        }
    }
    create_ready_partition(&admin, log, "west").await?;
    create_ready_partition(&admin, kv, "west").await?;
    assert_eq!(partition_bucket_count(&admin, log, "west").await?, 3);
    assert_eq!(partition_bucket_count(&admin, kv, "west").await?, 3);
    let mut west_rows = 0_usize;
    let mut third_bucket_written = false;
    let mut west_ids = Vec::new();
    for id in [5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17] {
        let mut row = GenericRow::new(3);
        row.set_field(0, id);
        row.set_field(1, "west");
        row.set_field(2, format!("value-{id}"));
        log_writer.append(&row)?.await?;
        kv_writer.upsert(&row)?.await?;
        west_rows += 1;
        west_ids.push(id);
        let log_offsets = admin
            .list_partition_offsets(log, "west", &[0, 1, 2], OffsetSpec::Latest)
            .await?;
        let kv_offsets = admin
            .list_partition_offsets(kv, "west", &[0, 1, 2], OffsetSpec::Latest)
            .await?;
        if log_offsets[&2] > 0 && kv_offsets[&2] > 0 {
            third_bucket_written = true;
            break;
        }
    }
    assert!(
        third_bucket_written,
        "both sources must exercise bucket 2 of the rescaled partition"
    );
    log_writer.flush().await?;
    kv_writer.flush().await?;
    let late_log = collect_stream(log_source.execute(0, delayed_log)?).await?;
    let late_kv = collect_stream(kv_source.execute(0, delayed_kv)?).await?;
    assert_eq!(
        first_log
            .iter()
            .chain(&late_log)
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );
    assert_eq!(
        first_kv
            .iter()
            .chain(&late_kv)
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );
    for source in [&log_source, &kv_source] {
        let metrics = source.metrics().unwrap();
        assert_eq!(metric(&metrics, "fluss_partitions_discovered"), 2);
        assert_eq!(metric(&metrics, "fluss_partitions_selected"), 2);
        assert_eq!(metric(&metrics, "fluss_buckets_assigned"), 4);
    }
    // The same writers were created before ALTER. They must now use the old
    // partition's two buckets and the new partition's three buckets.
    let mut old_row = GenericRow::new(3);
    old_row.set_field(0, 1000_i32);
    old_row.set_field(1, "north");
    old_row.set_field(2, "after-rescale");
    log_writer.append(&old_row)?.await?;
    kv_writer.upsert(&old_row)?.await?;
    assert_eq!(
        count_sql(&ctx, "SELECT COUNT(*) FROM plog WHERE id = 1000").await?,
        1
    );
    assert_eq!(
        count_sql(&ctx, "SELECT COUNT(*) FROM pkv WHERE id = 1000").await?,
        1
    );
    let lookup_table = connection.get_table(kv).await?;
    let mut lookuper = lookup_table.new_lookup()?.create_lookuper()?;
    for (region, id) in [("north", 1_i32), ("north", 1000), ("south", 3)]
        .into_iter()
        .chain(west_ids.into_iter().map(|id| ("west", id)))
    {
        let mut key = GenericRow::new(2);
        key.set_field(0, region);
        key.set_field(1, id);
        assert!(
            lookuper.lookup(&key).await?.get_single_row()?.is_some(),
            "lookup must find {region}/{id} in its partition's own bucket layout"
        );
    }
    assert_eq!(
        collect_plan(Arc::clone(&log_source), Arc::new(log_df.task_ctx()))
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        5 + west_rows
    );
    assert_eq!(
        collect_plan(Arc::clone(&kv_source), Arc::new(kv_df.task_ctx()))
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        5 + west_rows
    );
    for table in ["plog", "pkv"] {
        assert_eq!(
            count_sql(&ctx, &format!("SELECT COUNT(*) FROM {table}")).await?,
            (5 + west_rows) as i64
        );
        let query = ctx.sql(&format!("SELECT * FROM {table}")).await?;
        let plan = query.create_physical_plan().await?;
        let source = source_plan(&plan);
        let rows = collect_plan(plan, Arc::new(query.task_ctx())).await?;
        assert_eq!(
            rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
            5 + west_rows
        );
        assert_eq!(
            metric(&source.metrics().unwrap(), "fluss_buckets_assigned"),
            7
        );
    }
    let south = PartitionSpec::new([("region", "south")].into_iter().collect());
    let old_log_id = partition_id(&admin, log, "south").await?;
    let old_kv_id = partition_id(&admin, kv, "south").await?;
    let log_query = ctx
        .sql("SELECT id FROM plog WHERE region = 'south'")
        .await?;
    let log_old = source_plan(&log_query.create_physical_plan().await?);
    let kv_query = ctx.sql("SELECT id FROM pkv WHERE region = 'south'").await?;
    let kv_old = source_plan(&kv_query.create_physical_plan().await?);
    let log_context = Arc::new(log_query.task_ctx());
    let kv_context = Arc::new(kv_query.task_ctx());
    collect_stream(log_old.execute(0, Arc::clone(&log_context))?).await?;
    collect_stream(kv_old.execute(0, Arc::clone(&kv_context))?).await?;
    admin.drop_partition(log, &south, false).await?;
    admin.drop_partition(kv, &south, false).await?;
    create_ready_partition(&admin, log, "south").await?;
    create_ready_partition(&admin, kv, "south").await?;
    assert_ne!(partition_id(&admin, log, "south").await?, old_log_id);
    assert_ne!(partition_id(&admin, kv, "south").await?, old_kv_id);
    let mut replacement = GenericRow::new(3);
    replacement.set_field(0, 6_i32);
    replacement.set_field(1, "south");
    replacement.set_field(2, "replacement");
    log_writer.append(&replacement)?.await?;
    kv_writer.upsert(&replacement)?.await?;
    assert_no_recreated_rows(&log_old, log_context).await?;
    assert_no_recreated_rows(&kv_old, kv_context).await?;
    assert_eq!(
        count_sql(&ctx, "SELECT COUNT(*) FROM plog WHERE id = 6").await?,
        1
    );
    assert_eq!(
        count_sql(&ctx, "SELECT COUNT(*) FROM pkv WHERE id = 6").await?,
        1
    );
    admin.drop_partition(log, &south, false).await?;
    admin.drop_partition(kv, &south, false).await?;
    assert_eq!(
        collect_plan(Arc::clone(&log_source), Arc::new(log_df.task_ctx()))
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3 + west_rows
    );
    assert_eq!(
        collect_plan(Arc::clone(&kv_source), Arc::new(kv_df.task_ctx()))
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3 + west_rows
    );
    for table in ["plog", "pkv"] {
        assert_eq!(
            count_sql(&ctx, &format!("SELECT COUNT(*) FROM {table}")).await?,
            (3 + west_rows) as i64
        );
    }
    Ok(())
}

async fn partition_id(
    admin: &fluss::client::FlussAdmin,
    path: &TablePath,
    name: &str,
) -> TestResult<i64> {
    Ok(admin
        .list_partition_infos(path)
        .await?
        .into_iter()
        .find(|info| info.get_partition_name() == name)
        .expect("partition must exist")
        .get_partition_id())
}

async fn partition_bucket_count(
    admin: &fluss::client::FlussAdmin,
    path: &TablePath,
    name: &str,
) -> TestResult<i32> {
    Ok(admin
        .list_partition_infos(path)
        .await?
        .into_iter()
        .find(|info| info.get_partition_name() == name)
        .expect("partition must exist")
        .get_bucket_count()
        .expect("server must report partition bucket count"))
}

async fn assert_no_recreated_rows(
    source: &Arc<dyn ExecutionPlan>,
    ctx: Arc<datafusion::execution::TaskContext>,
) -> TestResult<()> {
    // A dropped, not-yet-open old partition may fail; it must never continue
    // against the new partition ID under the same name.
    if let Ok(rows) = collect_stream(source.execute(1, ctx)?).await {
        for batch in rows {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            assert!(!(0..ids.len()).any(|index| ids.value(index) == 6));
        }
    }
    Ok(())
}

async fn create_ready_partition(
    admin: &fluss::client::FlussAdmin,
    path: &TablePath,
    name: &str,
) -> TestResult<()> {
    let spec = PartitionSpec::new([("region", name)].into_iter().collect());
    admin.create_partition(path, &spec, false).await?;
    let buckets: Vec<_> = (0..partition_bucket_count(admin, path, name).await?).collect();
    let ready = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if admin
                .list_partition_offsets(path, name, &buckets, OffsetSpec::Latest)
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    if ready.is_err() {
        return Err(format!(
            "Partition {path}/{name} buckets {buckets:?} never became readable: {:?}",
            admin
                .list_partition_offsets(path, name, &buckets, OffsetSpec::Latest)
                .await
        )
        .into());
    }
    Ok(())
}

async fn check_log_sql(ctx: &SessionContext) -> TestResult<i64> {
    let total = count_sql(ctx, "SELECT COUNT(*) FROM log").await?;
    assert_eq!(total, 9);

    let df = ctx.sql("SELECT * FROM log LIMIT 1").await?;
    let plan = df.create_physical_plan().await?;
    let source = source_plan(&plan);
    let limited = collect_plan(plan, Arc::new(df.task_ctx())).await?;
    assert_eq!(limited.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let rows_after_limit = source.metrics().unwrap().output_rows().unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        source.metrics().unwrap().output_rows(),
        Some(rows_after_limit)
    );
    assert_eq!(
        metric(&source.metrics().unwrap(), "fluss_active_partition_streams"),
        0
    );
    assert_eq!(count_sql(ctx, "SELECT COUNT(*) FROM log").await?, total);

    let filtered = ctx
        .sql("SELECT value FROM log WHERE id = 1")
        .await?
        .collect()
        .await?;
    let values: Vec<_> = filtered
        .iter()
        .flat_map(|batch| {
            assert_eq!(batch.num_columns(), 1);
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| values.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(!values.is_empty() && values.iter().all(|value| value == "hola"));
    Ok(total)
}

async fn check_source_projection(ctx: &SessionContext) -> TestResult<()> {
    let plan = ctx
        .sql("SELECT value FROM log")
        .await?
        .create_physical_plan()
        .await?;
    let source = source_plan(&plan);
    assert_eq!(source.schema().fields().len(), 1);
    assert_eq!(source.schema().field(0).name(), "value");
    assert_eq!(source.output_partitioning().partition_count(), 2);
    Ok(())
}

async fn check_parallelism(
    connection: &Arc<FlussConnection>,
    name: &str,
    total: i64,
) -> TestResult<()> {
    for (target, cap, expected) in [(1, None, 1), (2, None, 2), (16, None, 2), (16, Some(1), 1)] {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(target));
        let table = FlussLogTable::open(
            Arc::clone(connection),
            TablePath::new(DATABASE, name),
            SCAN_TIMEOUT,
        )
        .await?;
        let table = if let Some(cap) = cap {
            table.with_max_partitions(cap)?
        } else {
            table
        };
        ctx.register_table("log", Arc::new(table))?;
        let plan = ctx
            .sql("SELECT value FROM log")
            .await?
            .create_physical_plan()
            .await?;
        assert_eq!(
            source_plan(&plan).output_partitioning().partition_count(),
            expected
        );
        assert_eq!(count_sql(&ctx, "SELECT COUNT(*) FROM log").await?, total);
    }
    Ok(())
}

async fn check_explain_metrics(ctx: &SessionContext) -> TestResult<()> {
    let text = explain_text(ctx, "EXPLAIN ANALYZE SELECT COUNT(*) FROM log").await?;
    for name in [
        "FlussScanExec",
        "fluss_buckets_assigned",
        "arrow_decoded_bytes",
        "arrow_output_bytes",
        "fluss_offset_capture_time",
        "fluss_read_time",
        "fluss_peak_decoded_arrow_batch_bytes",
        "fluss_active_partition_streams",
    ] {
        assert!(
            text.contains(name),
            "EXPLAIN ANALYZE missing {name}: {text}"
        );
    }
    let verbose = explain_text(ctx, "EXPLAIN ANALYZE VERBOSE SELECT COUNT(*) FROM log").await?;
    assert!(verbose.contains("buckets=0") && verbose.contains("buckets=1"));
    Ok(())
}

async fn check_catalog(
    ctx: &SessionContext,
    connection: &Arc<FlussConnection>,
    path: &TablePath,
    total: i64,
) -> TestResult<()> {
    let catalog = FlussCatalog::load(Arc::clone(connection), SCAN_TIMEOUT).await?;
    ctx.register_catalog("fluss", Arc::new(catalog));
    assert_eq!(
        count_sql(
            ctx,
            &format!("SELECT COUNT(*) FROM fluss.{DATABASE}.{}", path.table())
        )
        .await?,
        total
    );
    Ok(())
}

async fn check_timeout_and_kv(
    connection: &Arc<FlussConnection>,
    log: &TablePath,
    kv: &TablePath,
) -> TestResult<()> {
    let ctx = SessionContext::new();
    register_log(&ctx, connection, log.table(), Duration::from_nanos(1)).await?;
    let df = ctx.sql("SELECT COUNT(*) FROM log").await?;
    let plan = df.create_physical_plan().await?;
    let source = source_plan(&plan);
    let error = collect_plan(plan, Arc::new(df.task_ctx()))
        .await
        .expect_err("expired scan must fail instead of returning a partial count");
    assert!(error.to_string().contains("timed out"));
    assert_eq!(
        metric(&source.metrics().unwrap(), "fluss_active_partition_streams"),
        0
    );

    let error = FlussLogTable::open(Arc::clone(connection), kv.clone(), Duration::from_secs(2))
        .await
        .expect_err("KV tables must not be treated as append-only logs");
    assert!(matches!(error, DataFusionError::Plan(_)));
    Ok(())
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; run with --ignored"]
async fn empty_log_and_new_offsets_on_each_query() -> TestResult<()> {
    let connection = connect().await?;
    let path = create_empty_log(&connection).await?;
    let admin = connection.get_admin()?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let mut config = SessionConfig::new()
        .with_target_partitions(2)
        .with_batch_size(64);
    // Native sort reservations are per partition: two default 10 MiB spill
    // reservations cannot fit this 16 MiB host pool, even for 512 tiny rows.
    // The engine/application supplies the policy; the provider must not alter it.
    config.options_mut().execution.sort_spill_reservation_bytes = 1024 * 1024;
    let ctx = SessionContext::new_with_config_rt(
        config,
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        ),
    );
    let result = async {
        let provider =
            FlussLogTable::open(Arc::clone(&connection), path.clone(), SCAN_TIMEOUT).await?;
        let mut progress = provider.subscribe_progress();
        ctx.register_table("log", Arc::new(provider))?;
        assert_eq!(count_sql(&ctx, "SELECT COUNT(*) FROM log").await?, 0);
        let mut empty_buckets = HashSet::new();
        let mut initialized = HashSet::new();
        let mut completed = HashSet::new();
        while let Ok(event) = progress.try_recv() {
            match event {
                fluss_datafusion::LogProgress::Initialized {
                    partition,
                    positions,
                    ..
                } => {
                    initialized.insert(partition);
                    for position in positions {
                        assert_eq!(position.start_offset, 0);
                        assert_eq!(position.stop_offset, Some(0));
                        empty_buckets.insert(position.bucket);
                    }
                }
                fluss_datafusion::LogProgress::Terminated {
                    partition, status, ..
                } => {
                    assert_eq!(status, fluss_datafusion::LogTermination::Completed);
                    completed.insert(partition);
                }
                _ => panic!("empty source cannot offer or exclude rows"),
            }
        }
        assert_eq!(completed, initialized);
        assert_eq!(
            empty_buckets.len(),
            connection
                .get_table(&path)
                .await?
                .get_table_info()
                .get_num_buckets() as usize
        );
        let df = ctx.sql("SELECT * FROM log").await?;
        let source = source_plan(&df.create_physical_plan().await?);
        check_late_partition(&connection, &path, &admin, &df, &source).await?;
        check_batch_metrics(&source.metrics().unwrap());
        check_filter_pushdown(&ctx).await?;
        check_reference_queries(&ctx).await?;
        check_concurrent_reexecution(&df, &source).await?;
        assert_eq!(
            count_sql(&ctx, "SELECT COUNT(*) FROM log WHERE id = 8").await?,
            1
        );
        check_failure_after_first_partition(&admin, &path, &df, &source).await
    }
    .await;
    drop(ctx);
    let cleanup = admin.drop_table(&path, true).await;
    result?;
    cleanup?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while pool.reserved() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| {
        format!(
            "caller pool retains {} bytes after reference operators/reexecution/failure",
            pool.reserved()
        )
    })?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn create_empty_log(connection: &Arc<FlussConnection>) -> TestResult<TablePath> {
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let path = TablePath::new(DATABASE, format!("datafusion_test_{suffix}"));
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("optional", DataTypes::int())
                .build()?,
        )
        .distributed_by(Some(2), vec!["id".to_string()])
        .property("table.statistics.columns", "id")
        .build()?;
    connection
        .get_admin()?
        .create_table(&path, &descriptor, false)
        .await?;
    Ok(path)
}

async fn append_rows(connection: &Arc<FlussConnection>, path: &TablePath) -> TestResult<()> {
    let table = connection.get_table(path).await?;
    let writer = table.new_append()?.create_writer()?;
    for id in 0..512 {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        if id % 2 == 0 {
            row.set_field(1, id);
        }
        writer.append(&row)?;
        if id % 32 == 31 {
            writer.flush().await?;
        }
    }
    writer.flush().await?;
    Ok(())
}

async fn check_late_partition(
    connection: &Arc<FlussConnection>,
    path: &TablePath,
    admin: &fluss::client::FlussAdmin,
    df: &datafusion::dataframe::DataFrame,
    source: &Arc<dyn ExecutionPlan>,
) -> TestResult<()> {
    assert_eq!(source.output_partitioning().partition_count(), 2);
    let empty = collect_plan(Arc::clone(source), Arc::new(df.task_ctx())).await?;
    assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    let delayed = Arc::new(df.task_ctx());
    assert!(
        collect_stream(source.execute(0, Arc::clone(&delayed))?)
            .await?
            .is_empty()
    );

    append_rows(connection, path).await?;
    let offsets = admin
        .list_offsets(path, &[0, 1], OffsetSpec::Latest)
        .await?;
    assert!(offsets[&0] > 0 && offsets[&1] > 0);
    assert!(
        collect_stream(source.execute(1, delayed)?)
            .await?
            .is_empty()
    );
    let fresh = collect_plan(Arc::clone(source), Arc::new(df.task_ctx())).await?;
    assert_eq!(fresh.iter().map(RecordBatch::num_rows).sum::<usize>(), 512);
    Ok(())
}

fn check_batch_metrics(metrics: &MetricsSet) {
    let batches = metrics
        .sum(|m| matches!(m.value(), MetricValue::OutputBatches(_)))
        .unwrap()
        .as_usize();
    let decoded = metric(metrics, "arrow_decoded_bytes");
    let peak = metric(metrics, "fluss_peak_decoded_arrow_batch_bytes");
    assert!(
        batches > 2,
        "expected several streamed batches, got {batches}"
    );
    assert!(peak > 0 && peak < decoded);
}

async fn check_filter_pushdown(ctx: &SessionContext) -> TestResult<()> {
    let all = ctx.sql("SELECT * FROM log").await?;
    let full_plan = all.create_physical_plan().await?;
    let full_source = source_plan(&full_plan);
    let full_rows = collect_plan(full_plan, Arc::new(all.task_ctx())).await?;
    assert_eq!(
        full_rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
        512
    );
    let full_bytes = metric(&full_source.metrics().unwrap(), "arrow_decoded_bytes");

    // Pruning every batch, including the last, must complete without timeout.
    let none = ctx.sql("SELECT * FROM log WHERE id > 1000").await?;
    let plan = none.create_physical_plan().await?;
    let source = source_plan(&plan);
    let rows = collect_plan(plan, Arc::new(none.task_ctx())).await?;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    let pruned_bytes = metric(&source.metrics().unwrap(), "arrow_decoded_bytes");
    assert!(
        pruned_bytes < full_bytes,
        "filter failed to prune: {pruned_bytes} >= {full_bytes}"
    );

    let range = ctx
        .sql("SELECT * FROM log WHERE id >= 100 AND id < 200")
        .await?;
    let range_plan = range.create_physical_plan().await?;
    let range_source = source_plan(&range_plan);
    let range_rows = collect_plan(range_plan, Arc::new(range.task_ctx())).await?;
    assert_eq!(
        range_rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
        100
    );
    assert!(metric(&range_source.metrics().unwrap(), "arrow_decoded_bytes") < full_bytes);
    assert_eq!(
        count_sql(
            ctx,
            "SELECT COUNT(*) FROM log WHERE id >= 100 AND id < 200 AND optional IS NULL"
        )
        .await?,
        50,
        "DataFusion must apply the unsupported IS NULL conjunct exactly"
    );

    // Outside the column's Int32 range: no lossy literal narrowing or unsafe
    // pushdown. DataFusion still evaluates the SQL expression exactly.
    let outside = ctx.sql("SELECT * FROM log WHERE id > 2147483648").await?;
    let outside_plan = outside.create_physical_plan().await?;
    let outside_source = source_plan(&outside_plan);
    let outside_rows = collect_plan(outside_plan, Arc::new(outside.task_ctx())).await?;
    assert_eq!(
        outside_rows
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    assert_eq!(
        metric(&outside_source.metrics().unwrap(), "arrow_decoded_bytes"),
        full_bytes
    );
    Ok(())
}

async fn check_reference_queries(ctx: &SessionContext) -> TestResult<()> {
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..512));
    let optional: ArrayRef = Arc::new(Int32Array::from(
        (0..512)
            .map(|id| (id % 2 == 0).then_some(id))
            .collect::<Vec<_>>(),
    ));
    ctx.register_batch(
        "reference",
        RecordBatch::try_from_iter([("id", ids), ("optional", optional)])?,
    )?;
    // Both queries run in DataFusion. Only the Fluss source and its optional
    // pruning/projection differ from the Arrow reference table.
    for query in [
        "SELECT optional, id FROM {table} WHERE id >= 100 AND id < 200 AND optional IS NULL ORDER BY id",
        "SELECT id FROM {table} WHERE id > 2147483648 OR optional IS NULL ORDER BY id",
        "SELECT id, optional FROM {table} WHERE id > 100 AND optional < 120 ORDER BY id",
        "SELECT optional IS NULL AS missing, COUNT(*) AS n, SUM(id) AS total FROM {table} GROUP BY optional IS NULL ORDER BY missing",
        "SELECT l.id, r.optional FROM {table} AS l INNER JOIN reference AS r ON l.optional = r.id WHERE l.id >= 100 AND l.id < 140 ORDER BY l.id",
        "SELECT l.id, r.optional FROM {table} AS l LEFT JOIN reference AS r ON l.optional = r.id WHERE l.id >= 100 AND l.id < 110 ORDER BY l.id",
        "SELECT id, optional FROM {table} ORDER BY optional NULLS FIRST, id DESC LIMIT 7",
    ] {
        let actual = sql_rows(ctx, &query.replace("{table}", "log")).await?;
        let expected = sql_rows(ctx, &query.replace("{table}", "reference")).await?;
        assert_eq!(
            actual, expected,
            "Fluss changed DataFusion's result for {query}"
        );
    }
    Ok(())
}

async fn sql_rows(ctx: &SessionContext, sql: &str) -> TestResult<Vec<Vec<ScalarValue>>> {
    let mut rows = Vec::new();
    for batch in ctx.sql(sql).await?.collect().await? {
        for row in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|column| ScalarValue::try_from_array(batch.column(column), row))
                    .collect::<datafusion::common::Result<Vec<_>>>()?,
            );
        }
    }
    Ok(rows)
}

async fn check_concurrent_reexecution(
    df: &datafusion::dataframe::DataFrame,
    source: &Arc<dyn ExecutionPlan>,
) -> TestResult<()> {
    // Reuse one physical plan with distinct DataFusion query contexts. Each
    // execution must capture its own offsets and read every bucket once.
    let reads = (0..3).map(|_| collect_plan(Arc::clone(source), Arc::new(df.task_ctx())));
    for batches in futures::future::try_join_all(reads).await? {
        let mut ids = Vec::new();
        for batch in batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            ids.extend((0..column.len()).map(|row| column.value(row)));
        }
        ids.sort_unstable();
        assert_eq!(ids, (0..512).collect::<Vec<_>>());
    }
    assert_eq!(
        metric(&source.metrics().unwrap(), "fluss_active_partition_streams"),
        0
    );
    Ok(())
}

fn isolated_kubectl(kubeconfig: &str, args: &[&str]) -> TestResult<String> {
    let output = Command::new("kubectl")
        .env("KUBECONFIG", kubeconfig)
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_*; checks CA validation without mutating the cluster"]
async fn native_tls_rejects_untrusted_ca_without_exposing_credentials() -> TestResult<()> {
    let password = std::env::var("FLUSS_PASSWORD")?;
    let config = Config {
        bootstrap_servers: std::env::var("FLUSS_BOOTSTRAP")?,
        security_ssl_enabled: true,
        security_ssl_ca_file: None, // native-sni's private CA must be supplied explicitly
        security_protocol: "sasl".into(),
        security_sasl_username: std::env::var("FLUSS_USER")?,
        security_sasl_password: password.clone(),
        ..Config::default()
    };
    assert!(!format!("{config:?}").contains(&password));
    let error = match FlussConnection::new(config).await {
        Err(error) => error,
        Ok(connection) => {
            connection.close(Duration::from_secs(5)).await?;
            return Err("native-sni TLS accepted a client without its private CA".into());
        }
    };
    let message = error.to_string();
    assert!(
        message.to_ascii_lowercase().contains("tls")
            || message.to_ascii_lowercase().contains("certificate"),
        "{message}"
    );
    assert!(
        !message.contains(&password),
        "connection error exposed the SASL secret"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires native-sni KUBECONFIG and FLUSS_*; restarts only its active coordinator pod"]
async fn coordinator_failover_keeps_or_rejects_an_open_bounded_log_scan() -> TestResult<()> {
    let kubeconfig = std::env::var("KUBECONFIG")?;
    if isolated_kubectl(&kubeconfig, &["config", "current-context"])?.as_str() != "k3d-native-sni" {
        return Err("failover test requires the isolated k3d-native-sni context".into());
    }
    let connection = connect().await?;
    let admin = connection.get_admin()?;
    let path = create_empty_log(&connection).await?;
    let result: TestResult<()> = async {
        append_rows(&connection, &path).await?;
        let earliest = admin
            .list_offsets(&path, &[0], OffsetSpec::Earliest)
            .await?[&0];
        let latest = admin.list_offsets(&path, &[0], OffsetSpec::Latest).await?[&0];
        assert!(
            latest - earliest > 1,
            "the paused bucket needs multiple batches"
        );
        let mut config = connection.config().clone();
        config.scanner_log_max_poll_records = 1;
        let read_connection = Arc::new(FlussConnection::new(config).await?);
        let ctx = SessionContext::new();
        register_log(&ctx, &read_connection, path.table(), SCAN_TIMEOUT).await?;
        let query = ctx.sql("SELECT id FROM log").await?;
        let source = source_plan(&query.create_physical_plan().await?);
        let mut stream = source.execute(0, Arc::new(query.task_ctx()))?;
        let first = stream.next().await.expect("first pre-failover batch")?;
        if first.num_rows() == 0 || first.num_rows() as i64 >= latest - earliest {
            return Err(format!(
                "scan must pause before bucket completion, first batch had {} rows",
                first.num_rows()
            )
            .into());
        }
        let coordinator_id = connection
            .get_metadata()
            .get_cluster()
            .get_coordinator_server()
            .ok_or("no active Fluss coordinator in metadata")?
            .id();
        if ![0, 1].contains(&coordinator_id) {
            return Err(format!("unexpected isolated coordinator id: {coordinator_id}").into());
        }
        let pod = format!("native-coordinator-{coordinator_id}");
        let other = format!("native-coordinator-{}", 1 - coordinator_id);
        isolated_kubectl(
            &kubeconfig,
            &[
                "-n",
                "native-test",
                "wait",
                "--for=condition=Ready",
                &format!("pod/{other}"),
                "--timeout=15s",
            ],
        )?;
        let uid = isolated_kubectl(
            &kubeconfig,
            &[
                "-n",
                "native-test",
                "get",
                "pod",
                &pod,
                "-o",
                "jsonpath={.metadata.uid}",
            ],
        )?;
        isolated_kubectl(
            &kubeconfig,
            &["-n", "native-test", "delete", "pod", &pod, "--wait=false"],
        )?;

        // A scan opened before failover may finish from its tabletserver or
        // fail explicitly. It may not report success with missing offsets.
        let first_ids = first.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut seen: Vec<i32> = (0..first_ids.len()).map(|index| first_ids.value(index)).collect();
        let ongoing = tokio::time::timeout(Duration::from_secs(45), async {
            while let Some(next) = stream.next().await {
                let batch = next?;
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                seen.extend((0..ids.len()).map(|index| ids.value(index)));
            }
            Ok::<(), DataFusionError>(())
        })
        .await;
        drop(stream);

        // Always wait for the exact replacement pod and Green health before
        // evaluating the in-flight result or attempting a new query.
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                if isolated_kubectl(
                    &kubeconfig,
                    &[
                        "-n",
                        "native-test",
                        "get",
                        "pod",
                        &pod,
                        "-o",
                        "jsonpath={.metadata.uid}",
                    ],
                )
                .is_ok_and(|current| current != uid)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        })
        .await.map_err(|_| "coordinator replacement UID did not change within 120s")?;
        isolated_kubectl(
            &kubeconfig,
            &[
                "-n",
                "native-test",
                "wait",
                "--for=condition=Ready",
                &format!("pod/{pod}"),
                "--timeout=120s",
            ],
        )?;
        tokio::time::timeout(Duration::from_secs(90), async {
            while admin
                .get_cluster_health()
                .await
                .ok()
                .is_none_or(|health| health.status != ClusterHealthStatus::Green)
            {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        })
        .await.map_err(|_| "cluster health did not become Green within 90s after coordinator replacement")?;
        let fresh_count = tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                if let Ok(count) = count_sql(&ctx, "SELECT COUNT(*) FROM log").await {
                    break count;
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }).await.map_err(|_| "fresh bounded query did not recover within 90s of coordinator failover")?;
        if fresh_count != 512 {
            return Err(format!("fresh query returned {fresh_count} instead of 512 rows").into());
        }
        match ongoing.map_err(|_| "open source did not finish or fail within its 45s failover bound")? {
            Ok(()) => {
                let observed = seen.len();
                seen.sort_unstable();
                seen.dedup();
                if seen.len() != observed || seen.len() as i64 != latest - earliest {
                    return Err(format!("old scan lost or duplicated bucket offsets: expected {}, got {observed} rows ({} distinct)", latest - earliest, seen.len()).into());
                }
            }
            Err(error) => assert!(!error.to_string().is_empty()),
        }
        read_connection.close(Duration::from_secs(5)).await?;
        Ok(())
    }
    .await;
    let cleanup = admin.drop_table(&path, true).await;
    result?;
    cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; run with --ignored"]
async fn old_log_and_kv_plans_reject_changed_or_recreated_tables() -> TestResult<()> {
    let connection = connect().await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let log = TablePath::new(DATABASE, format!("log_lifecycle_{suffix}"));
    let kv = TablePath::new(DATABASE, format!("kv_lifecycle_{suffix}"));
    let admin = connection.get_admin()?;
    let schema = || {
        Schema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string())
    };
    let log_descriptor = TableDescriptor::builder()
        .schema(schema().build()?)
        .distributed_by(Some(1), vec!["id".into()])
        .build()?;
    let kv_descriptor = TableDescriptor::builder()
        .schema(schema().primary_key(vec!["id"])?.build()?)
        .distributed_by(Some(1), vec!["id".into()])
        .build()?;
    admin.create_table(&log, &log_descriptor, false).await?;
    if let Err(error) = admin.create_table(&kv, &kv_descriptor, false).await {
        admin.drop_table(&log, true).await?;
        return Err(error.into());
    }
    let result = check_old_plans(&connection, &log, &kv).await;
    let log_cleanup = admin.drop_table(&log, true).await;
    let kv_cleanup = admin.drop_table(&kv, true).await;
    result?;
    log_cleanup?;
    kv_cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn check_old_plans(
    connection: &Arc<FlussConnection>,
    log: &TablePath,
    kv: &TablePath,
) -> TestResult<()> {
    write_lifecycle_row(connection, log, kv, 1).await?;
    let ctx = SessionContext::new();
    ctx.register_table(
        "old_log",
        Arc::new(FlussLogTable::open(Arc::clone(connection), log.clone(), SCAN_TIMEOUT).await?),
    )?;
    ctx.register_table(
        "old_kv",
        Arc::new(FlussKvTable::open(Arc::clone(connection), kv.clone(), SCAN_TIMEOUT).await?),
    )?;
    let mut old_plans = Vec::new();
    for name in ["old_log", "old_kv"] {
        let query = ctx.sql(&format!("SELECT * FROM {name}")).await?;
        old_plans.push((
            query.create_physical_plan().await?,
            Arc::new(query.task_ctx()),
        ));
    }
    let admin = connection.get_admin()?;
    for path in [log, kv] {
        admin
            .alter_table(
                path,
                false,
                AlterTableChanges {
                    add_columns: vec![AddColumn {
                        column_name: "note".into(),
                        data_type_json: DataTypes::string()
                            .serialize_json()?
                            .to_string()
                            .into_bytes(),
                        comment: None,
                        position: ColumnPositionType::Last,
                    }],
                    ..Default::default()
                },
            )
            .await?;
    }
    for (plan, task_ctx) in &old_plans {
        match collect_plan(Arc::clone(plan), Arc::clone(task_ctx)).await {
            Ok(batches) => {
                // An additive change may leave the old projected schema valid.
                // Its plan must still return the original columns and row.
                assert_eq!(plan.schema().fields().len(), 2);
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
                for batch in batches {
                    assert_eq!(batch.schema(), plan.schema());
                    assert_eq!(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(0),
                        1
                    );
                }
            }
            Err(error) => assert!(
                error.to_string().contains("schema changed")
                    || error.to_string().contains("schema or topology changed"),
                "{error}"
            ),
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let updated = SessionContext::new();
        updated.register_table(
            "updated_log",
            Arc::new(FlussLogTable::open(Arc::clone(connection), log.clone(), SCAN_TIMEOUT).await?),
        )?;
        updated.register_table(
            "updated_kv",
            Arc::new(FlussKvTable::open(Arc::clone(connection), kv.clone(), SCAN_TIMEOUT).await?),
        )?;
        match async {
            for name in ["updated_log", "updated_kv"] {
                let count = count_sql(
                    &updated,
                    &format!("SELECT COUNT(*) FROM {name} WHERE note IS NULL"),
                )
                .await?;
                if count != 1 {
                    return Err(format!("{name}: expected one retained row, got {count}").into());
                }
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await
        {
            Ok(()) => break,
            Err(error) if tokio::time::Instant::now() >= deadline => {
                return Err(
                    format!("fresh providers did not read added nullable column: {error}").into(),
                );
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Evolved logs now decode full rows before projecting. A restrictive
    // DataFusion pool must reject that decoded batch and release its charge.
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
    let tiny = SessionContext::new_with_config_rt(
        SessionConfig::new(),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        ),
    );
    tiny.register_table(
        "evolved_log",
        Arc::new(FlussLogTable::open(Arc::clone(connection), log.clone(), SCAN_TIMEOUT).await?),
    )?;
    let query = tiny.sql("SELECT id FROM evolved_log").await?;
    let error = collect_plan(
        query.create_physical_plan().await?,
        Arc::new(query.task_ctx()),
    )
    .await
    .expect_err("projecting an evolved log must still charge its decoded batch");
    assert!(
        error.to_string().to_lowercase().contains("memory"),
        "{error}"
    );
    assert_eq!(pool.reserved(), 0);

    // Recreate under exactly the same names and schema as the original
    // plans. Table identity, not the SQL name or field names, must win.
    for path in [log, kv] {
        admin.drop_table(path, true).await?;
    }
    let schema = || {
        Schema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string())
    };
    admin
        .create_table(
            log,
            &TableDescriptor::builder()
                .schema(schema().build()?)
                .distributed_by(Some(1), vec!["id".into()])
                .build()?,
            false,
        )
        .await?;
    admin
        .create_table(
            kv,
            &TableDescriptor::builder()
                .schema(schema().primary_key(vec!["id"])?.build()?)
                .distributed_by(Some(1), vec!["id".into()])
                .build()?,
            false,
        )
        .await?;
    write_lifecycle_row(connection, log, kv, 7).await?;
    for (plan, task_ctx) in old_plans {
        let error = collect_plan(plan, task_ctx)
            .await
            .expect_err("an old plan must not read a replacement table");
        assert!(
            error.to_string().contains("topology changed")
                || error.to_string().contains("schema or topology changed"),
            "{error}"
        );
    }
    for (name, table) in [
        (
            "new_log",
            Arc::new(FlussLogTable::open(Arc::clone(connection), log.clone(), SCAN_TIMEOUT).await?)
                as Arc<dyn datafusion::catalog::TableProvider>,
        ),
        (
            "new_kv",
            Arc::new(FlussKvTable::open(Arc::clone(connection), kv.clone(), SCAN_TIMEOUT).await?)
                as Arc<dyn datafusion::catalog::TableProvider>,
        ),
    ] {
        ctx.register_table(name, table)?;
        assert_eq!(
            sql_rows(&ctx, &format!("SELECT id FROM {name}")).await?,
            vec![vec![ScalarValue::Int32(Some(7))]]
        );
    }
    Ok(())
}

async fn write_lifecycle_row(
    connection: &Arc<FlussConnection>,
    log: &TablePath,
    kv: &TablePath,
    id: i32,
) -> TestResult<()> {
    let mut row = GenericRow::new(2);
    row.set_field(0, id);
    row.set_field(1, format!("value-{id}"));
    let log_writer = connection
        .get_table(log)
        .await?
        .new_append()?
        .create_writer()?;
    log_writer.append(&row)?.await?;
    log_writer.flush().await?;
    let kv_writer = connection
        .get_table(kv)
        .await?
        .new_upsert()?
        .create_writer()?;
    kv_writer.upsert(&row)?.await?;
    kv_writer.flush().await?;
    Ok(())
}

async fn check_failure_after_first_partition(
    admin: &fluss::client::FlussAdmin,
    path: &TablePath,
    df: &datafusion::dataframe::DataFrame,
    source: &Arc<dyn ExecutionPlan>,
) -> TestResult<()> {
    let context = Arc::new(df.task_ctx());
    assert!(
        !collect_stream(source.execute(0, Arc::clone(&context))?)
            .await?
            .is_empty()
    );
    admin.drop_table(path, false).await?;
    let error = collect_stream(source.execute(1, context)?)
        .await
        .expect_err("a late bucket failure must not look like a complete read");
    assert!(!error.to_string().is_empty());
    Ok(())
}
