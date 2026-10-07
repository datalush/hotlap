//! Live Fluss tests. Run: cargo test -p hotlap-connectors --test fluss_live -- --ignored
//! Requires a running Fluss cluster; env FLUSS_BOOTSTRAP overrides the address.

use futures::StreamExt;
use hotlap_connectors::source::Source;

#[tokio::test]
#[ignore = "requires a live Fluss cluster"]
async fn fluss_source_reads_and_exposes_event_time() {
    let bootstrap = std::env::var("FLUSS_BOOTSTRAP").expect("FLUSS_BOOTSTRAP");
    let source = hotlap_connectors::fluss::source::FlussSource::open_from_bootstrap(
        &bootstrap,
        "/default/log_table",
    )
    .await
    .expect("open source");

    assert!(source.event_time_column().is_some());
    let splits = source.splits().expect("splits");
    assert!(!splits.is_empty());

    let mut stream = source.read(&splits[0]).expect("read");
    if let Some(Ok(sb)) = stream.next().await {
        let schema = sb.batch.schema();
        let idx = source.event_time_column().unwrap();
        assert_eq!(schema.field(idx).name(), "_event_time");
    }
}
