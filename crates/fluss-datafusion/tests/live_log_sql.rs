// SPDX-License-Identifier: Apache-2.0
//! Optional isolated-lab integration: run with FLUSS_* from ../lab/.env.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::GenericRow;
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
    let limited = ctx
        .sql("SELECT * FROM log LIMIT 1")
        .await?
        .collect()
        .await?;
    assert_eq!(
        limited.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
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
    fn source_schema(plan: &Arc<dyn ExecutionPlan>) -> Option<arrow::datatypes::SchemaRef> {
        if plan.name() == "StreamingTableExec" {
            return Some(plan.schema());
        }
        plan.children().into_iter().find_map(source_schema)
    }
    let schema = source_schema(&projected).expect("SQL must plan a Fluss stream");
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "value");

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
    let timeout = short_ctx
        .sql("SELECT COUNT(*) FROM tight")
        .await?
        .collect()
        .await
        .expect_err("expired scan must fail instead of returning a partial count");
    assert!(timeout.to_string().contains("timed out"));
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
        .distributed_by(Some(2), vec![])
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
    let count = ctx.sql("SELECT COUNT(*) FROM log").await?.collect().await?;
    assert_eq!(
        count[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );

    let table = connection.get_table(&path).await?;
    let writer = table.new_append()?.create_writer()?;
    for id in [7, 8] {
        let mut row = GenericRow::new(1);
        row.set_field(0, id);
        writer.append(&row)?;
    }
    writer.flush().await?;
    let count = ctx.sql("SELECT COUNT(*) FROM log").await?.collect().await?;
    assert_eq!(
        count[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
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
    admin.drop_table(&path, false).await?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}
