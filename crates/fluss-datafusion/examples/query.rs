// SPDX-License-Identifier: Apache-2.0
//! Query an explicitly registered append-only Fluss log table with DataFusion.
//! Run with FLUSS_BOOTSTRAP, FLUSS_CA_FILE, FLUSS_USER and FLUSS_PASSWORD set.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::TablePath;
use fluss_datafusion::{FlussCatalog, FlussLogTable};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let database = args.next().ok_or("expected database and log table names")?;
    let table = args.next().ok_or("expected database and log table names")?;
    if args.next().is_some() {
        return Err("expected exactly a database and a log table name".into());
    }
    let connection = Arc::new(
        FlussConnection::new(Config {
            bootstrap_servers: env::var("FLUSS_BOOTSTRAP")?,
            security_ssl_enabled: true,
            security_ssl_ca_file: Some(env::var("FLUSS_CA_FILE")?),
            security_protocol: "sasl".into(),
            security_sasl_username: env::var("FLUSS_USER")?,
            security_sasl_password: env::var("FLUSS_PASSWORD")?,
            ..Config::default()
        })
        .await?,
    );
    let source = FlussLogTable::open(
        Arc::clone(&connection),
        TablePath::new(database.clone(), table.clone()),
        Duration::from_secs(45),
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table("log", Arc::new(source))?;
    ctx.sql("SELECT COUNT(*) AS rows FROM log")
        .await?
        .show()
        .await?;
    ctx.sql("SELECT * FROM log LIMIT 5").await?.show().await?;
    ctx.sql("SELECT value FROM log WHERE id = 1 ORDER BY value LIMIT 5")
        .await?
        .show()
        .await?;
    let catalog = FlussCatalog::load(Arc::clone(&connection), Duration::from_secs(45)).await?;
    ctx.register_catalog("fluss", Arc::new(catalog));
    ctx.sql(&format!(
        "SELECT COUNT(*) AS rows FROM fluss.{}.{}",
        database, table
    ))
    .await?
    .show()
    .await?;
    ctx.sql("EXPLAIN ANALYZE SELECT COUNT(*) FROM log")
        .await?
        .show()
        .await?;
    drop(ctx);
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}
