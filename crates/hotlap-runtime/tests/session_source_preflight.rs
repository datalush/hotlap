//! SQL source metadata mismatches reject before effectful sink creation.

#[path = "session_source_preflight/fixture.rs"]
mod fixture;

use arrow::datatypes::DataType;

#[test]
fn changed_source_schema_rejects_before_factory_and_keeps_retry_config() {
    fixture::retry_after_metadata_rejection(DataType::Int32, 0);
}

#[test]
fn changed_source_splits_reject_before_factory_and_keep_retry_config() {
    fixture::retry_after_metadata_rejection(DataType::Int64, 1);
}
