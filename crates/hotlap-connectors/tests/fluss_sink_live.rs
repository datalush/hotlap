//! Live Fluss sink test. Run: cargo test -p hotlap-connectors --test fluss_sink_live -- --ignored
//! Requires a running Fluss cluster; env FLUSS_BOOTSTRAP overrides the address.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap::ZSetBatch;
use hotlap_connectors::fluss::sink::FlussSink;
use hotlap_connectors::sink::Sink;

#[tokio::test]
#[ignore = "requires a live Fluss cluster"]
async fn fluss_sink_appends_a_batch() {
    let bootstrap = std::env::var("FLUSS_BOOTSTRAP").expect("FLUSS_BOOTSTRAP");
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let sink = FlussSink::open_from_bootstrap(&bootstrap, "/default/sink_table", schema.clone())
        .await
        .expect("open sink");

    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let z = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1]))).unwrap();
    let stream = Box::pin(futures::stream::iter(vec![Ok(z)]));
    sink.write(stream).await.expect("write");
    sink.commit().await.expect("commit");
}
