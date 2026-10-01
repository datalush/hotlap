// SPDX-License-Identifier: Apache-2.0
//! Optional isolated-lab integration: run with FLUSS_* from ../lab/.env.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, StringArray};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::collect as collect_plan;
use datafusion::physical_plan::common::collect as collect_stream;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;
use fluss_datafusion::{FlussCatalog, FlussLogTable};

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; run with --ignored"]
async fn bounded_sql_and_kv_rejection_against_native_sni() -> Result<(), Box<dyn std::error::Error>>
{
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
    let ctx = SessionContext::new();
    ctx.register_table(
        "log",
        Arc::new(
            FlussLogTable::open(
                Arc::clone(&connection),
                TablePath::new("lab_spark", "demo_log"),
                Duration::from_secs(45),
            )
            .await?,
        ),
    )?;

    let count = ctx.sql("SELECT COUNT(*) FROM log").await?.collect().await?;
    let total = count[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert!(total > 0);
    let limited_df = ctx.sql("SELECT * FROM log LIMIT 1").await?;
    let limited_plan = limited_df.create_physical_plan().await?;
    fn source_plan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.name() == "FlussScanExec" {
            return Some(Arc::clone(plan));
        }
        plan.children().into_iter().find_map(source_plan)
    }
    let limited_source = source_plan(&limited_plan).expect("LIMIT has a Fluss source");
    let limited = collect_plan(limited_plan, Arc::new(limited_df.task_ctx())).await?;
    assert_eq!(
        limited.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let rows_after_limit = limited_source.metrics().unwrap().output_rows().unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        limited_source.metrics().unwrap().output_rows(),
        Some(rows_after_limit)
    );
    assert_eq!(
        limited_source
            .metrics()
            .unwrap()
            .sum_by_name("fluss_active_partition_streams")
            .unwrap()
            .as_usize(),
        0,
        "LIMIT must release source partitions"
    );
    let after_limit = ctx.sql("SELECT COUNT(*) FROM log").await?.collect().await?;
    assert_eq!(
        after_limit[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        total,
        "a limited read must not consume the next query's scanner"
    );
    let filtered = ctx
        .sql("SELECT value FROM log WHERE id = 1")
        .await?
        .collect()
        .await?;
    let values: Vec<String> = filtered
        .iter()
        .flat_map(|batch| {
            assert_eq!(batch.num_columns(), 1);
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| column.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(!values.is_empty());
    assert!(values.iter().all(|value| value == "hola"));

    let projected = ctx
        .sql("SELECT value FROM log")
        .await?
        .create_physical_plan()
        .await?;
    let source = source_plan(&projected).expect("SQL must plan a Fluss stream");
    let schema = source.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "value");
    assert_eq!(source.output_partitioning().partition_count(), 2);

    for (target, cap, expected) in [(1, None, 1), (2, None, 2), (16, None, 2), (16, Some(1), 1)] {
        let tuned =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(target));
        let table = FlussLogTable::open(
            Arc::clone(&connection),
            TablePath::new("lab_spark", "demo_log"),
            Duration::from_secs(45),
        )
        .await?;
        let table = if let Some(cap) = cap {
            table.with_max_partitions(cap)?
        } else {
            table
        };
        tuned.register_table("log", Arc::new(table))?;
        let source = source_plan(
            &tuned
                .sql("SELECT value FROM log")
                .await?
                .create_physical_plan()
                .await?,
        )
        .unwrap();
        assert_eq!(source.output_partitioning().partition_count(), expected);
        let count = tuned
            .sql("SELECT COUNT(*) FROM log")
            .await?
            .collect()
            .await?;
        assert_eq!(
            count[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            total
        );
    }

    let explain = ctx
        .sql("EXPLAIN ANALYZE SELECT COUNT(*) FROM log")
        .await?
        .collect()
        .await?;
    let text = explain
        .iter()
        .map(|batch| {
            let plans = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..plans.len())
                .map(|i| plans.value(i))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect::<Vec<_>>()
        .join("\n");
    for metric in [
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
            text.contains(metric),
            "EXPLAIN ANALYZE missing {metric}: {text}"
        );
    }
    let verbose = ctx
        .sql("EXPLAIN ANALYZE VERBOSE SELECT COUNT(*) FROM log")
        .await?
        .collect()
        .await?;
    let verbose = verbose
        .iter()
        .map(|batch| {
            let plans = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..plans.len())
                .map(|i| plans.value(i))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(verbose.contains("buckets=0") && verbose.contains("buckets=1"));

    let catalog = FlussCatalog::load(Arc::clone(&connection), Duration::from_secs(45)).await?;
    ctx.register_catalog("fluss", Arc::new(catalog));
    let catalog_count = ctx
        .sql("SELECT COUNT(*) FROM fluss.lab_spark.demo_log")
        .await?
        .collect()
        .await?;
    assert_eq!(
        catalog_count[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        total
    );

    let short_ctx = SessionContext::new();
    short_ctx.register_table(
        "tight",
        Arc::new(
            FlussLogTable::open(
                Arc::clone(&connection),
                TablePath::new("lab_spark", "demo_log"),
                Duration::from_nanos(1),
            )
            .await?,
        ),
    )?;
    let short_df = short_ctx.sql("SELECT COUNT(*) FROM tight").await?;
    let short_plan = short_df.create_physical_plan().await?;
    let short_source = source_plan(&short_plan).unwrap();
    let timeout = collect_plan(short_plan, Arc::new(short_df.task_ctx()))
        .await
        .expect_err("expired scan must fail instead of returning a partial count");
    assert!(timeout.to_string().contains("timed out"));
    assert_eq!(
        short_source
            .metrics()
            .unwrap()
            .sum_by_name("fluss_active_partition_streams")
            .unwrap()
            .as_usize(),
        0,
        "a failed scan must release its partitions"
    );
    drop(short_ctx);

    let error = FlussLogTable::open(
        Arc::clone(&connection),
        TablePath::new("lab_spark", "demo"),
        Duration::from_secs(2),
    )
    .await
    .expect_err("KV tables must not be treated as append-only logs");
    assert!(matches!(error, DataFusionError::Plan(_)));
    drop(ctx);
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; run with --ignored"]
async fn empty_log_and_new_offsets_on_each_query() -> Result<(), Box<dyn std::error::Error>> {
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
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let path = TablePath::new("lab_spark", format!("datafusion_test_{suffix}"));
    let admin = connection.get_admin()?;
    let descriptor = TableDescriptor::builder()
        .schema(Schema::builder().column("id", DataTypes::int()).build()?)
        .distributed_by(Some(2), vec!["id".to_string()])
        .build()?;
    admin.create_table(&path, &descriptor, false).await?;

    let ctx = SessionContext::new();
    ctx.register_table(
        "log",
        Arc::new(
            FlussLogTable::open(
                Arc::clone(&connection),
                path.clone(),
                Duration::from_secs(45),
            )
            .await?,
        ),
    )?;
    let df = ctx.sql("SELECT * FROM log").await?;
    let plan = df.create_physical_plan().await?;
    fn source_plan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.name() == "FlussScanExec" {
            return Some(Arc::clone(plan));
        }
        plan.children().into_iter().find_map(source_plan)
    }
    let source = source_plan(&plan).expect("SQL must plan a Fluss stream");
    assert_eq!(source.output_partitioning().partition_count(), 2);
    let count = collect_plan(Arc::clone(&source), Arc::new(df.task_ctx())).await?;
    assert_eq!(count.iter().map(|batch| batch.num_rows()).sum::<usize>(), 0);

    let delayed_context = Arc::new(df.task_ctx());
    let early = collect_stream(source.execute(0, Arc::clone(&delayed_context))?).await?;
    assert!(early.is_empty());

    let table = connection.get_table(&path).await?;
    let writer = table.new_append()?.create_writer()?;
    for id in 0..512 {
        let mut row = GenericRow::new(1);
        row.set_field(0, id);
        writer.append(&row)?;
        if id % 32 == 31 {
            writer.flush().await?;
        }
    }
    writer.flush().await?;
    let offsets = admin
        .list_offsets(&path, &[0, 1], OffsetSpec::Latest)
        .await?;
    assert!(offsets[&0] > 0 && offsets[&1] > 0);
    let late = collect_stream(source.execute(1, delayed_context)?).await?;
    assert!(
        late.is_empty(),
        "a late partition must retain the original offsets"
    );
    // Reuse the physical plan after writes. A fresh TaskContext represents a
    // new execution and must not reuse the first execution's stopping offsets.
    let count = collect_plan(Arc::clone(&source), Arc::new(df.task_ctx())).await?;
    assert_eq!(
        count.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        512
    );
    let metrics = source.metrics().unwrap();
    let batches = metrics
        .sum(|m| matches!(m.value(), MetricValue::OutputBatches(_)))
        .unwrap()
        .as_usize();
    let decoded_bytes = metrics
        .sum_by_name("arrow_decoded_bytes")
        .unwrap()
        .as_usize();
    let peak_bytes = metrics
        .sum_by_name("fluss_peak_decoded_arrow_batch_bytes")
        .unwrap()
        .as_usize();
    assert!(
        batches > 2,
        "expected several streamed batches, got {batches}"
    );
    assert!(peak_bytes > 0 && peak_bytes < decoded_bytes);
    let count = ctx
        .sql("SELECT COUNT(*) FROM log WHERE id = 8")
        .await?
        .collect()
        .await?;
    assert_eq!(
        count[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    drop(table);
    drop(ctx);
    let failure_context = Arc::new(df.task_ctx());
    let before_failure = collect_stream(source.execute(0, Arc::clone(&failure_context))?).await?;
    assert!(!before_failure.is_empty());
    admin.drop_table(&path, false).await?;
    let failed_partition = collect_stream(source.execute(1, failure_context)?)
        .await
        .expect_err("a late bucket failure must not look like a complete read");
    assert!(!failed_partition.to_string().is_empty());
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}
