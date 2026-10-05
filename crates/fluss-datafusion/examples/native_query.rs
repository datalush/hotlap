// SPDX-License-Identifier: Apache-2.0
//! Native DataFusion SQL with separate Fluss connection and engine policy.
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::TablePath;
use fluss_datafusion::{FlussKvTable, FlussLogTable};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;

type ExampleResult<T> = Result<T, Box<dyn std::error::Error>>;
fn positive(name: &str, default: usize) -> ExampleResult<usize> {
    let value = std::env::var(name).map_or(Ok(default), |v| v.parse::<usize>())?;
    if value == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(value)
}
#[tokio::main]
async fn main() -> ExampleResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() < 3 || args.len() > 4 || !["log", "kv"].contains(&args[2].as_str()) {
        return Err("usage: native_query DATABASE TABLE log|kv [SQL using fluss_source]".into());
    }
    let mut config = Config {
        bootstrap_servers: std::env::var("FLUSS_BOOTSTRAP")?,
        ..Default::default()
    };
    if let Ok(ca) = std::env::var("FLUSS_CA_FILE") {
        config.security_ssl_enabled = true;
        config.security_ssl_ca_file = Some(ca);
    }
    if let Ok(user) = std::env::var("FLUSS_USER") {
        config.security_protocol = "sasl".into();
        config.security_sasl_username = user;
        config.security_sasl_password = std::env::var("FLUSS_PASSWORD")?;
    }
    let connection = Arc::new(FlussConnection::new(config).await?);
    let result = async {
        let bytes = positive("DATAFUSION_POOL_MIB", 64)?
            .checked_mul(1024 * 1024)
            .ok_or("pool size overflow")?;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_target_partitions(positive("DATAFUSION_TARGET_PARTITIONS", 2)?),
            Arc::new(RuntimeEnvBuilder::new().with_memory_pool(pool).build()?),
        );
        let path = TablePath::new(&args[0], &args[1]);
        let timeout = Duration::from_secs(60);
        if args[2] == "kv" {
            ctx.register_table(
                "fluss_source",
                Arc::new(FlussKvTable::open(Arc::clone(&connection), path, timeout).await?),
            )?;
        } else {
            ctx.register_table(
                "fluss_source",
                Arc::new(FlussLogTable::open(Arc::clone(&connection), path, timeout).await?),
            )?;
        }
        let sql = args
            .get(3)
            .map_or("SELECT COUNT(*) FROM fluss_source", String::as_str);
        let mut stream = ctx.sql(sql).await?.execute_stream().await?;
        while let Some(batch) = stream.next().await {
            arrow::util::pretty::print_batches(&[batch?])?;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let close = connection.close(Duration::from_secs(5)).await;
    result?;
    close?;
    Ok(())
}
