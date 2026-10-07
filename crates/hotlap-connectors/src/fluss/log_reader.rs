//! Seam over `fluss-rs`: open a log scanner and poll ordered records.
//! This is the A1 boundary: the future shared-layer extraction cuts here.

use std::collections::HashMap;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use fluss::client::{FlussConnection, LogScanner};
use fluss::metadata::TablePath;
use fluss::row::ColumnarRow;

use crate::error::ConnectorError;

use super::fluss_err;

/// One polled record with its broker timestamp and offset.
pub struct Rec {
    /// Broker event-time timestamp (ms).
    pub timestamp: i64,
    /// Log offset of the record.
    pub offset: i64,
    /// The record's columns.
    pub row: ColumnarRow,
}

/// A live per-record log scanner over a Fluss table.
pub struct FlussLogReader {
    scanner: LogScanner,
    schema: SchemaRef,
}

impl FlussLogReader {
    /// Open a reader over `table_path`, subscribing `buckets` at their offsets.
    pub async fn open(
        connection: &FlussConnection,
        table_path: &TablePath,
        buckets: &[(i32, i64)],
    ) -> Result<Self, ConnectorError> {
        let table = connection.get_table(table_path).await.map_err(fluss_err)?;
        let schema = table
            .new_scan()
            .create_record_batch_log_scanner()
            .map_err(fluss_err)?
            .schema();
        let scanner = table.new_scan().create_log_scanner().map_err(fluss_err)?;
        let offsets: HashMap<i32, i64> = buckets.iter().copied().collect();
        scanner
            .subscribe_buckets(&offsets)
            .await
            .map_err(fluss_err)?;
        Ok(Self { scanner, schema })
    }

    /// Arrow schema of the records (without `_event_time`).
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Poll at most one batch of records within `timeout`.
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<Rec>, ConnectorError> {
        let records = self.scanner.poll(timeout).await.map_err(fluss_err)?;
        Ok(records
            .into_iter()
            .map(|record| Rec {
                timestamp: record.timestamp(),
                offset: record.offset(),
                row: record.row().clone(),
            })
            .collect())
    }
}
