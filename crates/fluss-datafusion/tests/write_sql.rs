// SPDX-License-Identifier: Apache-2.0
//! Opt-in SQL INSERT tests against isolated native-sni tables.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{ColumnarValue, LogicalPlan, Volatility, create_udf};
use datafusion::physical_plan::collect as collect_plan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{
    AlterTableChanges, DataTypes, PartitionSpec, Schema, TableDescriptor, TablePath,
};
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;
use fluss_datafusion::{
    FlussInsertCapability, FlussKvTable, FlussLogTable, FlussReadCapability, LogReadMode,
    LogReadOptions,
};
use futures::StreamExt;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Observe SELECT helper graphs planned inside DELETE/MERGE, not just the
/// outer DML. A direct DefaultPhysicalPlanner bypass cannot satisfy this probe.
#[derive(Debug)]
struct RecordingPlanner {
    helper_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl QueryPlanner for RecordingPlanner {
    async fn create_physical_plan(
        &self,
        logical: &LogicalPlan,
        session: &dyn Session,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if !matches!(logical, LogicalPlan::Dml(_)) {
            self.helper_calls.fetch_add(1, Ordering::Relaxed);
        }
        DefaultPhysicalPlanner::default()
            .create_physical_plan(logical, session)
            .await
    }
}

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
        let helper_calls = Arc::new(AtomicUsize::new(0));
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_query_planner(Arc::new(RecordingPlanner { helper_calls: Arc::clone(&helper_calls) }))
                .build(),
        );
        ctx.register_udf(create_udf(
            "is_two",
            vec![DataType::Int32],
            DataType::Boolean,
            Volatility::Immutable,
            Arc::new(|args| {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                let input = arrays[0].as_any().downcast_ref::<Int32Array>().unwrap();
                let output: BooleanArray = input.iter().map(|id| id.map(|id| id == 2)).collect();
                Ok(ColumnarValue::Array(Arc::new(output)))
            }),
        ));
        let log_provider = FlussLogTable::open(Arc::clone(&connection), log.clone(), Duration::from_secs(45)).await?;
        assert_eq!(log_provider.capabilities().read, FlussReadCapability::Log(LogReadMode::Batch));
        assert_eq!(log_provider.capabilities().insert, FlussInsertCapability::Append);
        assert!(!log_provider.capabilities().delete && !log_provider.capabilities().merge);
        let kv_provider = FlussKvTable::open(Arc::clone(&connection), kv.clone(), Duration::from_secs(45)).await?;
        assert_eq!(kv_provider.capabilities().read, FlussReadCapability::KvSnapshot);
        assert_eq!(kv_provider.capabilities().insert, FlussInsertCapability::FullRowUpsert);
        assert!(kv_provider.capabilities().delete && kv_provider.capabilities().merge);
        ctx.register_table(
            "events",
            Arc::new(log_provider),
        )?;
        ctx.register_table(
            "state",
            Arc::new(kv_provider),
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
        let alias_error = ctx
            .sql("DELETE FROM state AS selected WHERE is_two(selected.id)")
            .await
            .expect_err("DataFusion 55.1 cannot resolve this DELETE target alias");
        assert!(alias_error.to_string().contains("selected"), "{alias_error}");
        let before_delete = helper_calls.load(Ordering::Relaxed);
        assert_eq!(
            rows_written(
                &ctx.sql("DELETE FROM state WHERE is_two(state.id) AND value = 'two'")
                    .await?
                    .collect()
                    .await?
            ),
            1
        );
        assert!(helper_calls.load(Ordering::Relaxed) > before_delete, "DELETE must use the caller's planner for its selection graph");
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
        let merge_source = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            ("value", Arc::new(StringArray::from(vec!["after", "gone", "new"])) as ArrayRef),
            ("op", Arc::new(StringArray::from(vec!["u", "d", "i"])) as ArrayRef),
        ])?;
        ctx.register_table("merge_input", Arc::new(MemTable::try_new(
            merge_source.schema(),
            (0..3).map(|index| vec![merge_source.slice(index, 1)]).collect(),
        )?))?;
        let before_merge = helper_calls.load(Ordering::Relaxed);
        let merged = ctx.sql(
            "MERGE INTO state AS t USING merge_input AS s ON t.id = s.id WHEN MATCHED AND s.op = 'd' THEN DELETE WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)"
        ).await?.collect().await?;
        assert_eq!(rows_written(&merged), 3);
        assert!(helper_calls.load(Ordering::Relaxed) > before_merge, "MERGE must use the caller's planner for its operator graph");
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

        // A native provider with three physical partitions supplies INSERT.
        // The standard optimizer must enforce the sink's single-partition
        // requirement without our former FFI-motivated manual coalesce.
        let mut partitions = Vec::new();
        for id in [31, 32, 33] {
            partitions.push(vec![RecordBatch::try_from_iter(vec![
                ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                ("value", Arc::new(StringArray::from(vec!["external"])) as ArrayRef),
            ])?]);
        }
        let source_schema = partitions[0][0].schema();
        ctx.register_table("partitioned_input", Arc::new(MemTable::try_new(source_schema, partitions)?))?;
        let source_plan = ctx.table("partitioned_input").await?.create_physical_plan().await?;
        assert!(source_plan.output_partitioning().partition_count() > 1);
        let insert = ctx.sql("INSERT INTO events SELECT * FROM partitioned_input").await?;
        let plan = insert.create_physical_plan().await?;
        assert_eq!(plan.children()[0].output_partitioning().partition_count(), 1);
        assert_eq!(rows_written(&collect_plan(Arc::clone(&plan), Arc::new(insert.task_ctx())).await?), 3);
        assert_eq!(rows_written(&collect_plan(plan, Arc::new(insert.task_ctx())).await?), 3);
        let inserted = ctx.sql("SELECT id FROM events WHERE id >= 31 ORDER BY id").await?.collect().await?;
        let ids: Vec<_> = inserted.iter().flat_map(|batch| {
            batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec()
        }).collect();
        assert_eq!(ids, [31,31,32,32,33,33]);

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
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new().with_target_partitions(1),
            Arc::new(RuntimeEnvBuilder::new().with_memory_pool(Arc::clone(&pool)).build()?),
        );
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
        // One physical input batch interleaves old/new partitions and many
        // bucket keys. Check exact native bucket membership and per-bucket
        // input order, not merely the total across all buckets.
        let ids = (1000..1096).rev().collect::<Vec<i32>>();
        let regions = (0..ids.len()).map(|i| if i % 2 == 0 { "north" } else { "west" }).collect::<Vec<_>>();
        let values = ids.iter().enumerate().map(|(i, id)| (i % 7 != 0).then(|| format!("arrow-{id}"))).collect::<Vec<_>>();
        ctx.register_batch("mixed_arrow", RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(ids.clone())) as ArrayRef),
            ("region", Arc::new(StringArray::from(regions.clone())) as ArrayRef),
            ("value", Arc::new(StringArray::from(values.clone())) as ArrayRef),
        ])?)?;
        // Earlier KV SELECT results remain retained in this scope, with their
        // own source leases. Only this INSERT's additional charge must vanish.
        let reserved_before_insert = pool.reserved();
        assert_eq!(rows_written(&ctx.sql("INSERT INTO events SELECT id, region, value FROM mixed_arrow").await?.collect().await?), 96);
        assert_eq!(pool.reserved(), reserved_before_insert, "ACKed slices/gathers must release sink leases without freeing retained source results");
        let partitions = admin.list_partition_infos(&log).await?;
        connection.get_metadata().update_table_metadata(&log).await?;
        connection.get_metadata().check_and_update_partition_metadata_by_ids(&log, &partitions.iter().map(|p| p.get_partition_id()).collect::<Vec<_>>()).await?;
        let table = connection.get_table(&log).await?;
        let mut counts = std::collections::HashMap::new();
        let mut starts = std::collections::HashMap::new();
        let mut expected = std::collections::HashMap::<_, Vec<_>>::new();
        for partition in &partitions {
            if !["north", "west"].contains(&partition.get_partition_name().as_str()) { continue; }
            let count = partition.get_bucket_count().expect("coordinator reports effective layout");
            assert_eq!(count, if partition.get_partition_name() == "north" { 2 } else { 3 });
            counts.insert(partition.get_partition_id(), count);
            for bucket in 0..count { starts.insert((partition.get_partition_id(), bucket), 0); }
        }
        // The existing native row API is the reference: do not reproduce its
        // private hash/routing implementation inside the connector's tests.
        let reference = table.new_append()?.create_writer()?;
        for (index, &id) in ids.iter().enumerate() {
            let mut row = GenericRow::new(3);
            row.set_field(0, id);
            row.set_field(1, regions[index].to_owned());
            row.set_field(2, format!("reference-{id}"));
            reference.append(&row)?;
        }
        reference.flush().await?;
        let expected_values = ids.iter().copied().zip(values).collect::<std::collections::HashMap<_, _>>();
        let scanner = table.new_scan().create_record_batch_log_scanner()?;
        scanner.subscribe_partition_buckets_with_counts(&starts, counts).await?;
        let mut observed = std::collections::HashMap::<_, Vec<_>>::new();
        tokio::time::timeout(Duration::from_secs(15), async {
            while observed.values().map(Vec::len).sum::<usize>() < ids.len() || expected.values().map(Vec::len).sum::<usize>() < ids.len() {
                for batch in scanner.poll(Duration::from_secs(1)).await? {
                    let ids = batch.batch().column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                    let values = batch.batch().column(2).as_any().downcast_ref::<StringArray>().unwrap();
                    for row in 0..ids.len() {
                        if ids.value(row) < 1000 { continue; }
                        let bucket = (batch.bucket().partition_id().unwrap(), batch.bucket().bucket_id());
                        if !values.is_null(row) && values.value(row).starts_with("reference-") {
                            expected.entry(bucket).or_default().push((ids.value(row), expected_values[&ids.value(row)].clone()));
                        } else {
                            observed.entry(bucket).or_default().push((ids.value(row), (!values.is_null(row)).then(|| values.value(row).to_owned())));
                        }
                    }
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        }).await??;
        assert_eq!(observed, expected);
        assert_eq!(expected.len(), 5, "exercise all old2/new3 destination groups");
        for (partition, bucket) in starts.keys() { scanner.unsubscribe_partition(*partition, *bucket).await?; }
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
