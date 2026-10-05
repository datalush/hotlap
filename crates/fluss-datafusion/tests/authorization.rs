// SPDX-License-Identifier: Apache-2.0
//! Real SASL/ACL checks in an owned fixture, through native DataFusion SQL.

use arrow::array::{Int32Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use fluss::client::FlussConnection;
use fluss::error::FlussError;
use fluss::metadata::{
    AclInfo, DataTypes, OperationType, PermissionType, ResourceType, Schema, TableDescriptor,
    TablePath,
};
use fluss_datafusion::{FlussKvTable, FlussLogTable, FlussWriteProgress, FlussWriteTermination};
use fluss_test_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
use futures::FutureExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
const USERS: &[(&str, &str)] = &[
    ("admin", "fixture-admin"),
    ("reader", "fixture-reader"),
    ("writer", "fixture-writer"),
];

fn rows_written(rows: &[RecordBatch]) -> u64 {
    rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}
fn authorization_cause(error: &DataFusionError) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(cause) = current {
        if let Some(native) = cause.downcast_ref::<fluss::error::Error>()
            && native.api_error() == Some(FlussError::AuthorizationException)
        {
            return true;
        }
        current = cause.source();
    }
    false
}
async fn context(
    connection: Arc<FlussConnection>,
    log: &TablePath,
    kv: &TablePath,
) -> TestResult<(
    SessionContext,
    Arc<dyn MemoryPool>,
    tokio::sync::broadcast::Receiver<FlussWriteProgress>,
)> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(1),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        ),
    );
    ctx.register_table(
        "events",
        Arc::new(
            FlussLogTable::open(
                Arc::clone(&connection),
                log.clone(),
                Duration::from_secs(10),
            )
            .await?,
        ),
    )?;
    let provider = FlussKvTable::open(connection, kv.clone(), Duration::from_secs(10)).await?;
    let writes = provider.subscribe_writes();
    ctx.register_table("state", Arc::new(provider))?;
    Ok((ctx, pool, writes))
}
async fn released(pool: &Arc<dyn MemoryPool>) -> TestResult<()> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while pool.reserved() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}
async fn cases(cluster: &FlussTestingCluster) -> TestResult<()> {
    let admin_conn = Arc::new(
        cluster
            .get_fluss_connection_with_sasl(USERS[0].0, USERS[0].1)
            .await,
    );
    let metadata = admin_conn.get_metadata();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            metadata
                .update_tables_metadata(&Default::default(), &Default::default(), vec![])
                .await?;
            if metadata.get_cluster().get_tablet_server(0).is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok::<_, fluss::error::Error>(())
    })
    .await??;
    let admin = admin_conn.get_admin()?;
    admin
        .create_database("authorization_tests", None, true)
        .await?;
    let log = TablePath::new("authorization_tests", "events");
    let kv = TablePath::new("authorization_tests", "state");
    let fields = || {
        Schema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string())
    };
    admin
        .create_table(
            &log,
            &TableDescriptor::builder()
                .schema(fields().build()?)
                .distributed_by(Some(1), vec!["id".into()])
                .build()?,
            false,
        )
        .await?;
    admin
        .create_table(
            &kv,
            &TableDescriptor::builder()
                .schema(fields().primary_key(vec!["id"])?.build()?)
                .distributed_by(Some(1), vec!["id".into()])
                .build()?,
            false,
        )
        .await?;
    let mut acls = Vec::new();
    for user in ["reader", "writer"] {
        for resource in ["authorization_tests.events", "authorization_tests.state"] {
            for operation in [OperationType::Describe, OperationType::Read]
                .into_iter()
                .chain((user == "writer").then_some(OperationType::Write))
            {
                acls.push(AclInfo {
                    resource_name: resource.into(),
                    resource_type: ResourceType::Table,
                    principal_name: user.into(),
                    principal_type: "User".into(),
                    host: "*".into(),
                    operation_type: operation,
                    permission_type: PermissionType::Allow,
                });
            }
        }
    }
    for result in admin.create_acls(acls).await? {
        assert!(result.error.is_none(), "{result:?}");
    }
    let writer = Arc::new(
        cluster
            .get_fluss_connection_with_sasl(USERS[2].0, USERS[2].1)
            .await,
    );
    let (write_ctx, write_pool, _) = context(writer.clone(), &log, &kv).await?;
    assert_eq!(
        rows_written(
            &write_ctx
                .sql("INSERT INTO events VALUES (1,'event')")
                .await?
                .collect()
                .await?
        ),
        1
    );
    assert_eq!(
        rows_written(
            &write_ctx
                .sql("INSERT INTO state VALUES (1,'old'),(2,'delete')")
                .await?
                .collect()
                .await?
        ),
        2
    );
    for idempotence in [false, true] {
        let mut config = admin_conn.config().clone();
        config.security_sasl_username = USERS[1].0.into();
        config.security_sasl_password = USERS[1].1.into();
        config.writer_enable_idempotence = idempotence;
        let reader = Arc::new(FlussConnection::new(config).await?);
        let (ctx, pool, mut writes) = context(reader.clone(), &log, &kv).await?;
        assert_eq!(
            ctx.sql("SELECT id FROM events")
                .await?
                .collect()
                .await?
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        assert_eq!(
            ctx.sql("SELECT id FROM state")
                .await?
                .collect()
                .await?
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        let append_error = ctx
            .sql("INSERT INTO events VALUES (3,'denied')")
            .await?
            .collect()
            .await
            .expect_err("read-only principal cannot append logs");
        assert!(
            authorization_cause(&append_error),
            "typed log authorization cause lost: {append_error}"
        );
        released(&pool).await?;
        for sql in [
            "INSERT INTO state VALUES (3,'denied')",
            "DELETE FROM state WHERE id=2",
            "MERGE INTO state AS t USING (VALUES (1,'denied'),(3,'denied')) AS s(id,value) ON t.id=s.id WHEN MATCHED THEN UPDATE SET value=s.value WHEN NOT MATCHED THEN INSERT(id,value) VALUES(s.id,s.value)",
        ] {
            let error = ctx
                .sql(sql)
                .await?
                .collect()
                .await
                .expect_err("read-only principal must not modify data");
            assert!(
                authorization_cause(&error),
                "typed authorization cause lost (idempotence={idempotence}): {error}"
            );
            let terminal = loop {
                if let FlussWriteProgress::Terminated(summary) = writes.try_recv()? {
                    break summary;
                }
            };
            assert_eq!(terminal.status, FlussWriteTermination::Failed);
            assert_eq!(terminal.counts.confirmed, 0);
            released(&pool).await?;
        }
        let rows = ctx
            .sql("SELECT id FROM state ORDER BY id")
            .await?
            .collect()
            .await?;
        let ids = rows
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
            .collect::<Vec<_>>();
        assert_eq!(ids, [1, 2]);
        drop(rows);
        released(&pool).await?;
        let events = ctx.sql("SELECT id FROM events").await?.collect().await?;
        assert_eq!(events.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        drop(events);
        released(&pool).await?;
        reader.close(Duration::from_secs(2)).await?;
    }
    assert_eq!(rows_written(&write_ctx.sql("MERGE INTO state AS t USING (VALUES(1,'authorized'),(3,'new')) AS s(id,value) ON t.id=s.id WHEN MATCHED THEN UPDATE SET value=s.value WHEN NOT MATCHED THEN INSERT(id,value) VALUES(s.id,s.value)").await?.collect().await?),2);
    assert_eq!(
        rows_written(
            &write_ctx
                .sql("DELETE FROM state WHERE id=2")
                .await?
                .collect()
                .await?
        ),
        1
    );
    let final_rows = write_ctx
        .sql("SELECT id,value FROM state ORDER BY id")
        .await?
        .collect()
        .await?;
    let final_ids = final_rows
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
        .collect::<Vec<_>>();
    let final_values = final_rows
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap().to_owned())
        })
        .collect::<Vec<_>>();
    assert_eq!(final_ids, [1, 3]);
    assert_eq!(final_values, ["authorized", "new"]);
    drop(final_rows);
    released(&write_pool).await?;
    writer.close(Duration::from_secs(2)).await?;
    admin_conn.close(Duration::from_secs(2)).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Docker owned SASL/ACL fixture; real read-only and authorized native SQL"]
async fn native_sql_enforces_read_only_acl_and_preserves_authorization_causes() -> TestResult<()> {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let conf = std::collections::HashMap::from([
        ("authorizer.enabled".into(), "true".into()),
        ("super.users".into(), "User:admin".into()),
    ]);
    let mut builder = FlussTestingClusterBuilder::new_with_cluster_conf(
        format!("df-authorization-{suffix}"),
        &conf,
    )
    .with_port(20000 + (suffix % 20000) as u16)
    .with_sasl(
        USERS
            .iter()
            .map(|(u, p)| (u.to_string(), p.to_string()))
            .collect(),
    );
    let cluster = builder.build().await;
    let result = std::panic::AssertUnwindSafe(async {
        tokio::time::timeout(Duration::from_secs(90), cases(&cluster))
            .await
            .map_err(|_| "authorization matrix exceeded90s")?
    })
    .catch_unwind()
    .await;
    cluster.stop();
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
