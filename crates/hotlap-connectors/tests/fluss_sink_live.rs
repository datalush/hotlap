//! Live Fluss sink test. Run: cargo test -p hotlap-connectors --test fluss_sink_live -- --ignored
//! Requires a running Fluss cluster; env FLUSS_BOOTSTRAP overrides the address.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{DataTypes, Schema as FlussSchema, TableDescriptor, TablePath};
use futures::FutureExt;
use hotlap::ZSetBatch;
use hotlap_connectors::fluss::sink::FlussSink;
use hotlap_connectors::sink::Sink;

#[path = "support/fixture_lifecycle.rs"]
mod fixture_lifecycle;
use fixture_lifecycle::{cleanup_database, finish_owned_lifecycle, protect_owned_setup};

#[tokio::test]
#[ignore = "requires a live Fluss cluster"]
async fn fluss_sink_appends_a_batch() {
    let bootstrap = std::env::var("FLUSS_BOOTSTRAP").expect("FLUSS_BOOTSTRAP");
    let connection = FlussConnection::new(Config {
        bootstrap_servers: bootstrap.clone(),
        ..Config::default()
    })
    .await
    .expect("connect");
    let database = format!(
        "hotlap_sink_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    );
    let path = TablePath::new(&database, "records");
    let admin = connection.get_admin().expect("admin");
    admin
        .create_database(&database, None, false)
        .await
        .expect("create owned database");
    protect_owned_setup(&admin, &database, async {
        let descriptor = TableDescriptor::builder()
            .schema(
                FlussSchema::builder()
                    .column("k", DataTypes::bigint())
                    .build()
                    .expect("table schema"),
            )
            .build()
            .expect("table descriptor");
        admin
            .create_table(&path, &descriptor, false)
            .await
            .expect("create owned table");
    })
    .await;
    eprintln!("created owned Fluss sink fixture {database}/records");
    let result = std::panic::AssertUnwindSafe(async {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let sink = FlussSink::open_from_bootstrap(
            &bootstrap,
            &format!("{database}/records"),
            schema.clone(),
        )
        .await
        .expect("open sink");

        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        let z = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1]))).unwrap();
        let stream = Box::pin(futures::stream::iter(vec![Ok(z)]));
        sink.write(stream).await.expect("write");
        sink.commit().await.expect("commit");
    })
    .catch_unwind()
    .await;
    let cleanup = cleanup_database(&admin, &database).await;
    eprintln!("dropped owned Fluss sink fixture database {database}");
    finish_owned_lifecycle(result, cleanup);
}
