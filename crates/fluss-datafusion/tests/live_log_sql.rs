// SPDX-License-Identifier: Apache-2.0
//! Optional isolated-lab integration: run with FLUSS_* from ../lab/.env.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use datafusion::common::DataFusionError;
use datafusion::prelude::SessionContext;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::TablePath;
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
