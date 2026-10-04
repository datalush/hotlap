// SPDX-License-Identifier: Apache-2.0
//! Opt-in SQL INSERT tests against isolated native-sni tables.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef, Int32Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::collect as collect_plan;
use datafusion::prelude::{SessionConfig, SessionContext};
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{
    AlterTableChanges, DataTypes, PartitionSpec, Schema, TableDescriptor, TablePath,
};
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;
use fluss_datafusion::{FlussKvTable, FlussLogTable, LogReadOptions};
use futures::StreamExt;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

fn rows_written(batches: &[arrow::record_batch::RecordBatch]) -> u64 {
    assert_eq!(batches.len(), 1);
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}

fn lab_config() -> TestResult<Config> {
    Ok(Config {
        bootstrap_servers: std::env::var("FLUSS_BOOTSTRAP")?,
        security_ssl_enabled: true,
        security_ssl_ca_file: Some(std::env::var("FLUSS_CA_FILE")?),
        security_protocol: "sasl".into(),
        security_sasl_username: std::env::var("FLUSS_USER")?,
        security_sasl_password: std::env::var("FLUSS_PASSWORD")?,
        ..Config::default()
    })
}

async fn connect() -> TestResult<Arc<FlussConnection>> {
    let connection = Arc::new(FlussConnection::new(lab_config()?).await?);
    connection
        .get_admin()?
        .create_database("datafusion_tests", None, true)
        .await?;
    Ok(connection)
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; writes isolated Fluss tables"]
async fn insert_log_and_upsert_kv_from_sql() -> TestResult<()> {
    let connection = connect().await?;
    let admin = connection.get_admin()?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let log = TablePath::new("datafusion_tests", format!("insert_log_{suffix}"));
    let kv = TablePath::new("datafusion_tests", format!("insert_kv_{suffix}"));
    let schema = || {
        Schema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string())
    };
    admin
        .create_table(
            &log,
            &TableDescriptor::builder()
                .schema(schema().build()?)
                .distributed_by(Some(2), vec!["id".into()])
                .build()?,
            false,
        )
        .await?;
    let result: TestResult<()> = async {
        admin
            .create_table(
                &kv,
                &TableDescriptor::builder()
                    .schema(schema().primary_key(vec!["id"])?.build()?)
                    .distributed_by(Some(2), vec!["id".into()])
                    .build()?,
                false,
            )
            .await?;
        let ctx = SessionContext::new();
        ctx.register_table(
            "events",
            Arc::new(
                FlussLogTable::open(
                    Arc::clone(&connection),
                    log.clone(),
                    Duration::from_secs(45),
                )
                .await?,
            ),
        )?;
        ctx.register_table(
            "state",
            Arc::new(
                FlussKvTable::open(Arc::clone(&connection), kv.clone(), Duration::from_secs(45))
                    .await?,
            ),
        )?;
        assert_eq!(
            rows_written(
                &ctx.sql("INSERT INTO events (id, value) VALUES (1, 'one'), (2, 'two')")
                    .await?
                    .collect()
                    .await?
            ),
            2
        );
        assert_eq!(
            rows_written(
                &ctx.sql("INSERT INTO events SELECT id + 10, value FROM events WHERE id <= 2")
                    .await?
                    .collect()
                    .await?
            ),
            2
        );
        assert_eq!(
            rows_written(
                &ctx.sql("INSERT INTO state (id, value) VALUES (1, 'one'), (2, 'two')")
                    .await?
                    .collect()
                    .await?
            ),
            2
        );
        assert_eq!(
            rows_written(
                &ctx.sql("INSERT INTO state (id, value) VALUES (1, 'updated')")
                    .await?
                    .collect()
                    .await?
            ),
            1
        );

        let values = ctx
            .sql("SELECT value FROM state WHERE id = 1")
            .await?
            .collect()
            .await?;
        let found: Vec<_> = values
            .iter()
            .flat_map(|batch| {
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..col.len())
                    .map(|i| col.value(i).to_owned())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(found, ["updated"]);
        assert_eq!(
            rows_written(
                &ctx.sql("DELETE FROM state WHERE state.id = 2 AND value = 'two'")
                    .await?
                    .collect()
                    .await?
            ),
            1
        );
        assert_eq!(
            rows_written(
                &ctx.sql("DELETE FROM state WHERE id = 2")
                    .await?
                    .collect()
                    .await?
            ),
            0
        );
        assert_eq!(
            rows_written(&ctx.sql("DELETE FROM state").await?.collect().await?),
            1
        );
        let empty = ctx
            .sql("SELECT COUNT(*) FROM state")
            .await?
            .collect()
            .await?;
        assert_eq!(
            empty[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        ctx.sql("INSERT INTO state (id, value) VALUES (1, 'before'), (2, 'delete')").await?.collect().await?;
        let merged = ctx.sql(
            "MERGE INTO state AS t USING (VALUES (1, 'after', 'u'), (2, 'gone', 'd'), (3, 'new', 'i')) AS s(id, value, op) ON t.id = s.id WHEN MATCHED AND s.op = 'd' THEN DELETE WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)"
        ).await?.collect().await?;
        assert_eq!(rows_written(&merged), 3);
        let merged_rows = ctx.sql("SELECT value FROM state ORDER BY id").await?.collect().await?;
        let values = merged_rows.iter().flat_map(|batch| {
            let col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            (0..col.len()).map(|i|col.value(i).to_owned()).collect::<Vec<_>>()
        }).collect::<Vec<_>>();
        assert_eq!(values, ["after", "new"]);
        let no_op = ctx.sql(
            "MERGE INTO state AS t USING (VALUES (1, 'unused')) AS s(id, value) ON t.id = s.id WHEN MATCHED AND s.value = 'never' THEN UPDATE SET value = s.value"
        ).await?.collect().await?;
        assert_eq!(rows_written(&no_op), 0);
        let priority = ctx.sql(
            "MERGE INTO state AS t USING (VALUES (1, 'first')) AS s(id, value) ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN MATCHED THEN DELETE"
        ).await?.collect().await?;
        assert_eq!(rows_written(&priority), 1);
        let by_source = ctx.sql(
            "MERGE INTO state AS t USING (VALUES (1)) AS s(id) ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE"
        ).await?.collect().await?;
        assert_eq!(rows_written(&by_source), 1);
        let duplicates = ctx.sql(
            "MERGE INTO state AS t USING (VALUES (1, 'a'), (1, 'b')) AS s(id, value) ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value"
        ).await?.collect().await.expect_err("multiple source matches must not silently overwrite a key");
        assert!(duplicates.to_string().contains("multiple modifying actions"), "{duplicates}");
        let preserved = ctx.sql("SELECT value FROM state WHERE id = 1").await?.collect().await?;
        assert_eq!(preserved[0].column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "first");
        let ctx_ref = &ctx;
        let insert_once = |id: i32| async move {
            let sql = format!("INSERT INTO events (id, value) VALUES ({id}, 'parallel')");
            Ok::<u64, Box<dyn std::error::Error>>(rows_written(
                &ctx_ref.sql(&sql).await?.collect().await?,
            ))
        };
        let (a, b) = tokio::try_join!(insert_once(21), insert_once(22))?;
        assert_eq!((a, b), (1, 1));
        let count = ctx
            .sql("SELECT COUNT(*) FROM events")
            .await?
            .collect()
            .await?;
        assert_eq!(
            count[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            6
        );

        let deferred = ctx
            .sql("INSERT INTO events (id, value) VALUES (99, 'stale-plan')")
            .await?;
        let old_plan = deferred.create_physical_plan().await?;
        admin.drop_table(&log, true).await?;
        admin
            .create_table(
                &log,
                &TableDescriptor::builder()
                    .schema(schema().build()?)
                    .distributed_by(Some(2), vec!["id".into()])
                    .build()?,
                false,
            )
            .await?;
        let error = collect_plan(old_plan, Arc::new(deferred.task_ctx()))
            .await
            .expect_err("an old DML plan cannot write to a recreated Fluss table");
        assert!(
            error.to_string().contains("changed table identity"),
            "{error}"
        );
        Ok(())
    }
    .await;
    let log_cleanup = admin.drop_table(&log, true).await;
    let kv_cleanup = admin.drop_table(&kv, true).await;
    result?;
    log_cleanup?;
    kv_cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; writes a 4 MiB isolated log"]
async fn sql_insert_backpressures_without_blocking_sender() -> TestResult<()> {
    let mut config = lab_config()?;
    config.writer_buffer_memory_size = 2 * 1024 * 1024;
    config.writer_batch_size = 1024 * 1024;
    config.writer_buffer_wait_timeout_ms = 5_000;
    config.writer_retries = 3;
    let connection = Arc::new(FlussConnection::new(config).await?);
    let admin = connection.get_admin()?;
    admin
        .create_database("datafusion_tests", None, true)
        .await?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = TablePath::new("datafusion_tests", format!("insert_pressure_{suffix}"));
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
                .build()?,
            false,
        )
        .await?;
    let result: TestResult<()> = async {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new(),
            Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::clone(&pool))
                    .build()?,
            ),
        );
        ctx.register_table(
            "sink",
            Arc::new(
                FlussLogTable::open(
                    Arc::clone(&connection),
                    path.clone(),
                    Duration::from_secs(45),
                )
                .await?,
            ),
        )?;
        let value = "x".repeat(200_000);
        ctx.register_batch(
            "input",
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int32Array::from((0..20).collect::<Vec<_>>())) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(StringArray::from(vec![value; 20])) as ArrayRef,
                ),
            ])?,
        )?;
        let written = tokio::time::timeout(
            Duration::from_secs(25),
            ctx.sql("INSERT INTO sink SELECT id, value FROM input")
                .await?
                .collect(),
        )
        .await??;
        assert_eq!(rows_written(&written), 20);
        assert_eq!(
            pool.reserved(),
            0,
            "sink must release its batch reservation"
        );
        let rows = ctx
            .sql("SELECT COUNT(*) FROM sink")
            .await?
            .collect()
            .await?;
        assert_eq!(
            rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            20
        );
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
            "sink",
            Arc::new(
                FlussLogTable::open(
                    Arc::clone(&connection),
                    path.clone(),
                    Duration::from_secs(45),
                )
                .await?,
            ),
        )?;
        let error = tiny
            .sql("INSERT INTO sink (id, value) VALUES (999, 'cannot reserve')")
            .await?
            .collect()
            .await
            .expect_err("sink must respect the query memory budget");
        assert!(
            error.to_string().to_lowercase().contains("memory"),
            "{error}"
        );
        assert_eq!(tiny_pool.reserved(), 0);
        let absent = ctx
            .sql("SELECT COUNT(*) FROM sink WHERE id = 999")
            .await?
            .collect()
            .await?;
        assert_eq!(
            absent[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        Ok(())
    }
    .await;
    let cleanup = admin.drop_table(&path, true).await;
    result?;
    cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

async fn ready_partition(
    admin: &fluss::client::FlussAdmin,
    path: &TablePath,
    name: &str,
    buckets: i32,
) -> TestResult<()> {
    admin
        .create_partition(
            path,
            &PartitionSpec::new([("region", name)].into_iter().collect()),
            false,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if admin
                .list_partition_offsets(
                    path,
                    name,
                    &(0..buckets).collect::<Vec<_>>(),
                    OffsetSpec::Latest,
                )
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await?;
    Ok(())
}

async fn wait_latest(
    admin: &fluss::client::FlussAdmin,
    path: &TablePath,
    minimum: i64,
) -> TestResult<()> {
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if admin
                .list_offsets(path, &[0], OffsetSpec::Latest)
                .await
                .is_ok_and(|offsets| offsets.get(&0).is_some_and(|offset| *offset >= minimum))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; writes isolated streaming tables"]
async fn streaming_sql_insert_confirms_before_input_ends() -> TestResult<()> {
    let connection = connect().await?;
    let admin = connection.get_admin()?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let source = TablePath::new("datafusion_tests", format!("write_source_{suffix}"));
    let destination = TablePath::new("datafusion_tests", format!("write_sink_{suffix}"));
    let schema = || {
        Schema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string())
            .build()
    };
    let descriptor = TableDescriptor::builder()
        .schema(schema()?)
        .distributed_by(Some(1), vec!["id".into()])
        .build()?;
    admin.create_table(&source, &descriptor, false).await?;
    let result: TestResult<()> = async {
        admin.create_table(&destination, &descriptor, false).await?;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_batch_size(1),
        );
        ctx.register_table(
            "feed",
            Arc::new(
                FlussLogTable::open_with_options(
                    Arc::clone(&connection),
                    source.clone(),
                    LogReadOptions::default(),
                )
                .await?,
            ),
        )?;
        ctx.register_table(
            "sink",
            Arc::new(
                FlussLogTable::open(
                    Arc::clone(&connection),
                    destination.clone(),
                    Duration::from_secs(30),
                )
                .await?,
            ),
        )?;
        let df = ctx
            .sql("INSERT INTO sink SELECT id, value FROM feed")
            .await?;
        let plan = df.create_physical_plan().await?;
        let task_ctx = Arc::new(df.task_ctx());
        let ongoing = tokio::spawn(async move {
            let mut output = plan.execute(0, task_ctx)?;
            output.next().await.transpose()
        });
        tokio::time::sleep(Duration::from_millis(700)).await;
        let producer = connection
            .get_table(&source)
            .await?
            .new_append()?
            .create_writer()?;
        for id in [1_i32, 2_i32] {
            let mut row = GenericRow::new(2);
            row.set_field(0, id);
            row.set_field(1, format!("event-{id}"));
            producer.append(&row)?;
            producer.flush().await?;
            wait_latest(&admin, &destination, id as i64).await?;
            assert!(
                !ongoing.is_finished(),
                "INSERT on a continuous source must stay active"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let rows = ctx
            .sql("SELECT COUNT(*) FROM sink")
            .await?
            .collect()
            .await?;
        assert_eq!(
            rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        ongoing.abort();
        assert!(ongoing.await.unwrap_err().is_cancelled());
        Ok(())
    }
    .await;
    let source_cleanup = admin.drop_table(&source, true).await;
    let sink_cleanup = admin.drop_table(&destination, true).await;
    result?;
    source_cleanup?;
    sink_cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires native-sni and FLUSS_* credentials; writes isolated partitioned tables"]
async fn sql_insert_routes_mixed_partitions_after_rescale() -> TestResult<()> {
    let connection = connect().await?;
    let admin = connection.get_admin()?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let log = TablePath::new("datafusion_tests", format!("insert_plog_{suffix}"));
    let kv = TablePath::new("datafusion_tests", format!("insert_pkv_{suffix}"));
    let fields = || {
        Schema::builder()
            .column("id", DataTypes::int())
            .column("region", DataTypes::string())
            .column("value", DataTypes::string())
    };
    admin
        .create_table(
            &log,
            &TableDescriptor::builder()
                .schema(fields().build()?)
                .partitioned_by(vec!["region"])
                .distributed_by(Some(2), vec!["id".into()])
                .build()?,
            false,
        )
        .await?;
    let result: TestResult<()> = async {
        admin.create_table(
            &kv,
            &TableDescriptor::builder().schema(fields().primary_key(vec!["region", "id"])?.build()?)
                .partitioned_by(vec!["region"])
                .distributed_by(Some(2), vec!["id".into()]).build()?,
            false,
        ).await?;
        for path in [&log, &kv] {
            ready_partition(&admin, path, "north", 2).await?;
            ready_partition(&admin, path, "south", 2).await?;
        }
        let ctx = SessionContext::new();
        ctx.register_table("events", Arc::new(
            FlussLogTable::open(Arc::clone(&connection), log.clone(), Duration::from_secs(45)).await?
        ))?;
        ctx.register_table("state", Arc::new(
            FlussKvTable::open(Arc::clone(&connection), kv.clone(), Duration::from_secs(45)).await?
        ))?;
        for dest in ["events", "state"] {
            let sql = format!("INSERT INTO {dest} (id, region, value) VALUES (1, 'north', 'n1'), (2, 'south', 's2'), (3, 'north', 'n3'), (4, 'south', 's4')");
            assert_eq!(rows_written(&ctx.sql(&sql).await?.collect().await?), 4);
            assert_eq!(rows_written(&ctx.sql(&format!("INSERT INTO {dest} (id, region, value) VALUES (1, 'north', 'updated')")).await?.collect().await?), 1);
        }
        assert_eq!(ctx.sql("SELECT COUNT(*) FROM events WHERE region = 'south'").await?.collect().await?[0]
            .column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0), 2);
        let north = ctx.sql("SELECT value FROM state WHERE id = 1 AND region = 'north'").await?.collect().await?;
        assert_eq!(north[0].column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "updated");

        for path in [&log, &kv] {
            admin.alter_table(path, false, AlterTableChanges {
                modify_bucket_count: Some(3), ..Default::default()
            }).await?;
            ready_partition(&admin, path, "west", 3).await?;
        }
        for dest in ["events", "state"] {
            let sql = format!("INSERT INTO {dest} (id, region, value) VALUES (10, 'north', 'old-layout'), (11, 'west', 'new-layout')");
            assert_eq!(rows_written(&ctx.sql(&sql).await?.collect().await?), 2);
            let result = ctx.sql(&format!("SELECT COUNT(*) FROM {dest} WHERE region = 'west'")).await?.collect().await?;
            assert_eq!(result[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0), 1);
        }
        let merged = ctx.sql(
            "MERGE INTO state AS t USING (VALUES (1, 'north', 'merged'), (12, 'west', 'merge-insert')) AS s(id, region, value) ON t.id = s.id AND t.region = s.region WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT (id, region, value) VALUES (s.id, s.region, s.value)"
        ).await?.collect().await?;
        assert_eq!(rows_written(&merged), 2);
        let updated = ctx.sql("SELECT value FROM state WHERE id = 1 AND region = 'north'").await?.collect().await?;
        assert_eq!(updated[0].column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "merged");
        assert_eq!(rows_written(&ctx.sql("DELETE FROM state WHERE region = 'west'").await?.collect().await?), 2);
        let result = ctx.sql("SELECT COUNT(*) FROM state WHERE region = 'west'").await?.collect().await?;
        assert_eq!(result[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0), 0);
        Ok(())
    }.await;
    let log_cleanup = admin.drop_table(&log, true).await;
    let kv_cleanup = admin.drop_table(&kv, true).await;
    result?;
    log_cleanup?;
    kv_cleanup?;
    connection.close(Duration::from_secs(5)).await?;
    Ok(())
}
