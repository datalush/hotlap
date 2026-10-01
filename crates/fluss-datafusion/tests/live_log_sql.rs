// SPDX-License-Identifier: Apache-2.0
//! Optional isolated-lab integration: run with FLUSS_* from ../lab/.env.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::collect as collect_plan;
use datafusion::physical_plan::common::collect as collect_stream;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;
use fluss_datafusion::{FlussCatalog, FlussKvTable, FlussLogTable};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
const DATABASE: &str = "datafusion_tests";
const SCAN_TIMEOUT: Duration = Duration::from_secs(45);

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
        check_kv_sql(&ctx, &connection, &log_path, &kv_path).await
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
        return Err(Box::new(error));
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
    let ctx = SessionContext::new();
    let result = async {
        register_log(&ctx, &connection, path.table(), SCAN_TIMEOUT).await?;
        let df = ctx.sql("SELECT * FROM log").await?;
        let source = source_plan(&df.create_physical_plan().await?);
        check_late_partition(&connection, &path, &admin, &df, &source).await?;
        check_batch_metrics(&source.metrics().unwrap());
        check_filter_pushdown(&ctx).await?;
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
