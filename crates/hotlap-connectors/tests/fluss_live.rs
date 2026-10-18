//! Live Fluss acceptance tests. Run with `--include-ignored` and FLUSS_BOOTSTRAP.

use std::collections::HashMap;
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{DataGetters, GenericRow};
use futures::{FutureExt, StreamExt};
use hotlap_connectors::fluss::source::FlussSource;
use hotlap_connectors::source::Source;

#[path = "support/fixture_lifecycle.rs"]
mod fixture_lifecycle;
use fixture_lifecycle::{cleanup_database, finish_owned_lifecycle, protect_owned_setup};

struct OwnedTable {
    connection: FlussConnection,
    path: TablePath,
    name: String,
    database: String,
}

impl OwnedTable {
    async fn create() -> Self {
        let bootstrap = std::env::var("FLUSS_BOOTSTRAP").expect("FLUSS_BOOTSTRAP");
        let connection = FlussConnection::new(Config {
            bootstrap_servers: bootstrap,
            ..Config::default()
        })
        .await
        .expect("connect to Fluss");
        let database = format!("hotlap_db_{}", unique_suffix());
        let name = "records".to_string();
        let path = TablePath::new(&database, &name);
        let admin = connection.get_admin().expect("admin");
        admin
            .create_database(&database, None, false)
            .await
            .expect("create owned database");
        protect_owned_setup(&admin, &database, async {
            let descriptor = TableDescriptor::builder()
                .schema(
                    Schema::builder()
                        .column("id", DataTypes::bigint())
                        .column("payload", DataTypes::string())
                        .build()
                        .expect("schema"),
                )
                .build()
                .expect("descriptor");
            admin
                .create_table(&path, &descriptor, false)
                .await
                .expect("create owned table");
        })
        .await;
        eprintln!("created owned Fluss fixture {database}/{name}");
        Self {
            connection,
            path,
            name,
            database,
        }
    }

    async fn drop_owned(&self) -> Result<(), String> {
        let admin = self
            .connection
            .get_admin()
            .map_err(|error| error.to_string())?;
        cleanup_database(&admin, &self.database).await?;
        eprintln!("dropped owned Fluss fixture database {}", self.database);
        Ok(())
    }
}

fn unique_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    format!(
        "{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    )
}

async fn append_known_rows(table: &OwnedTable) {
    let handle = table
        .connection
        .get_table(&table.path)
        .await
        .expect("open owned table");
    let writer = handle
        .new_append()
        .expect("append")
        .create_writer()
        .expect("writer");
    for id in 1..=3_i64 {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, format!("payload-{id}"));
        writer.append(&row).expect("append row");
    }
    writer.flush().await.expect("flush rows");
}

async fn raw_timestamps(table: &OwnedTable) -> HashMap<i64, i64> {
    let handle = table
        .connection
        .get_table(&table.path)
        .await
        .expect("open owned table");
    let scanner = handle.new_scan().create_log_scanner().expect("raw scanner");
    scanner.subscribe(0, 0).await.expect("subscribe");
    let mut timestamps = HashMap::new();
    for _ in 0..20 {
        let records = tokio::time::timeout(
            Duration::from_secs(3),
            scanner.poll(Duration::from_millis(250)),
        )
        .await
        .expect("raw scan poll timed out")
        .expect("raw scan");
        for record in records.records_by_buckets().values().flatten() {
            let id = record.row().get_long(0).expect("record id");
            timestamps.insert(id, record.timestamp());
        }
        if timestamps.len() == 3 {
            break;
        }
    }
    timestamps
}

#[tokio::test]
#[ignore = "requires a live Fluss cluster"]
async fn fluss_source_event_time_matches_raw_record_timestamps() {
    let table = OwnedTable::create().await;
    let result = std::panic::AssertUnwindSafe(async {
        append_known_rows(&table).await;
        let expected = raw_timestamps(&table).await;
        assert_eq!(
            expected.len(),
            3,
            "raw scanner did not return all fixture rows"
        );

        let bootstrap = std::env::var("FLUSS_BOOTSTRAP").expect("FLUSS_BOOTSTRAP");
        let source = FlussSource::open_from_bootstrap(
            &bootstrap,
            &format!("{}/{}", table.database, table.name),
        )
        .await
        .expect("open source");
        let split = source
            .splits()
            .expect("splits")
            .into_iter()
            .next()
            .expect("bucket");
        let time_idx = source.event_time_column().expect("event-time index");
        let mut stream = source.read(&split).expect("read stream");
        let mut observed = HashMap::new();
        tokio::time::timeout(Duration::from_secs(15), async {
            while observed.len() < expected.len() {
                let batch = stream
                    .next()
                    .await
                    .expect("source ended before all fixture rows")
                    .expect("source batch");
                let id_idx = batch.batch.schema().index_of("id").expect("id column");
                assert_eq!(batch.batch.schema().field(time_idx).name(), "_event_time");
                let ids = batch
                    .batch
                    .column(id_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id array");
                let times = batch
                    .batch
                    .column(time_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("timestamp array");
                for row in 0..ids.len() {
                    let id = ids.value(row);
                    assert_eq!(
                        expected.get(&id),
                        Some(&times.value(row)),
                        "timestamp mismatch for id {id}"
                    );
                    assert!(
                        observed.insert(id, times.value(row)).is_none(),
                        "duplicate id {id}"
                    );
                }
            }
        })
        .await
        .expect("source read timed out");
        assert_eq!(observed, expected);
    })
    .catch_unwind()
    .await;
    finish_owned_lifecycle(result, table.drop_owned().await);
}

#[tokio::test]
#[ignore = "requires a live Fluss cluster"]
async fn fluss_live_fixture_table_is_isolated_and_droppable() {
    let table = OwnedTable::create().await;
    let result = std::panic::AssertUnwindSafe(append_known_rows(&table))
        .catch_unwind()
        .await;
    finish_owned_lifecycle(result, table.drop_owned().await);
}

#[tokio::test]
#[ignore = "requires a live Fluss cluster"]
async fn failed_owned_setup_drops_its_database_and_preserves_failure() {
    let bootstrap = std::env::var("FLUSS_BOOTSTRAP").expect("FLUSS_BOOTSTRAP");
    let connection = FlussConnection::new(Config {
        bootstrap_servers: bootstrap,
        ..Config::default()
    })
    .await
    .expect("connect to Fluss");
    let database = format!("hotlap_failed_setup_{}", unique_suffix());
    let admin = connection.get_admin().expect("admin");
    admin
        .create_database(&database, None, false)
        .await
        .expect("create owned database");
    eprintln!("created owned failure fixture database {database}");

    let failure = std::panic::AssertUnwindSafe(protect_owned_setup(&admin, &database, async {
        panic!("controlled fixture setup failure")
    }))
    .catch_unwind()
    .await
    .expect_err("controlled setup failure should propagate");
    assert_eq!(
        failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied()),
        Some("controlled fixture setup failure")
    );
    assert!(
        !admin
            .list_databases()
            .await
            .expect("list databases")
            .contains(&database),
        "failed fixture setup left its owned database behind"
    );
    eprintln!("dropped owned failure fixture database {database}");
}
