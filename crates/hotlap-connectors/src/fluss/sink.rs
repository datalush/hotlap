//! Fluss append/upsert sink: writes a view's changelog to a Fluss table.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use fluss::client::FlussConnection;
use fluss::config::Config;
use futures::StreamExt;
use hotlap::ZSetBatch;

use crate::error::ConnectorError;
use crate::sink::{ChangeStream, Sink, SinkCapabilities};

use super::sink_convert::rows_to_batch;
use super::sink_writer::FlussWriter;
use super::{fluss_err, parse_path};

/// Fluss sink. Log tables are appended (at-least-once); primary-key tables are
/// upserted (idempotent). Retractions (`diff < 0`) are rejected in both modes.
pub struct FlussSink {
    writer: FlussWriter,
    schema: SchemaRef,
    physical_identity: String,
}

/// Metadata resolved from a Fluss table without opening its writer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlussSinkMetadata {
    pub physical_identity: String,
    pub capabilities: SinkCapabilities,
}

impl FlussSink {
    /// Resolve table identity and output capabilities without creating a writer.
    pub async fn describe_target(
        bootstrap: &str,
        path: &str,
    ) -> Result<FlussSinkMetadata, ConnectorError> {
        let config = Config {
            bootstrap_servers: bootstrap.to_string(),
            ..Config::default()
        };
        let connection = FlussConnection::new(config).await.map_err(fluss_err)?;
        let table = connection
            .get_table(&parse_path(path)?)
            .await
            .map_err(fluss_err)?;
        let info = table.get_table_info();
        Ok(FlussSinkMetadata {
            physical_identity: physical_table_identity(bootstrap, path, info.get_table_id()),
            capabilities: if table.has_primary_key() {
                SinkCapabilities::Idempotent
            } else {
                SinkCapabilities::AtLeastOnce
            },
        })
    }

    /// Connect to `bootstrap` and open `path` (`<database>/<table>`).
    ///
    /// A table with a primary key is written through the upsert writer, which
    /// makes replay after a crash idempotent; a log table is appended.
    pub async fn open_from_bootstrap(
        bootstrap: &str,
        path: &str,
        schema: SchemaRef,
    ) -> Result<Self, ConnectorError> {
        let config = Config {
            bootstrap_servers: bootstrap.to_string(),
            ..Config::default()
        };
        let connection = FlussConnection::new(config).await.map_err(fluss_err)?;
        let table = connection
            .get_table(&parse_path(path)?)
            .await
            .map_err(fluss_err)?;
        let info = table.get_table_info();
        let physical_identity = physical_table_identity(bootstrap, path, info.get_table_id());
        let writer = if table.has_primary_key() {
            let row_type = Arc::new(info.row_type().clone());
            let upsert = table.new_upsert().map_err(fluss_err)?;
            FlussWriter::Upsert {
                writer: upsert.create_writer().map_err(fluss_err)?,
                row_type,
            }
        } else {
            let append = table.new_append().map_err(fluss_err)?;
            FlussWriter::Append(append.create_writer().map_err(fluss_err)?)
        };
        Ok(Self {
            writer,
            schema,
            physical_identity,
        })
    }
}

fn physical_table_identity(
    bootstrap: &str,
    path: &str,
    table_id: impl std::fmt::Display,
) -> String {
    format!(
        "fluss:{}:{}:{}",
        super::source::normalize_cluster_locator(bootstrap),
        path.trim_matches('/')
            .split('/')
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("/"),
        table_id
    )
}

/// Reject any change with a negative diff (append cannot delete).
pub fn retraction_check(batch: &ZSetBatch) -> Result<(), ConnectorError> {
    let diffs = batch
        .diff
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ConnectorError::Arrow("diff column must be Int64".into()))?;
    if (0..diffs.len()).any(|i| diffs.value(i) < 0) {
        return Err(ConnectorError::Unsupported(
            "append sink cannot apply retractions (diff < 0)".into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl Sink for FlussSink {
    fn physical_identity(&self) -> Option<String> {
        Some(self.physical_identity.clone())
    }

    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            let batch = item?;
            let records = rows_to_batch(&self.schema, &batch)?;
            if records.num_rows() == 0 {
                continue;
            }
            self.writer.write_batch(&records)?;
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        self.writer.capabilities()
    }

    fn accepts_retractions(&self) -> bool {
        // Both append and upsert modes reject negative diffs; the plan must be
        // refused before any write rather than failing per batch.
        false
    }

    fn commit_redriable(&self) -> bool {
        // Idempotent upserts make a replay safe, but not a re-driven commit: the
        // writer only queues writes in memory, so a new writer after a crash
        // cannot flush what an earlier one accepted. Never inferred from
        // `Idempotent`; a restarted sink replays instead.
        false
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        // Fluss has no sink-side transaction; commit is the awaited flush.
        self.writer.flush().await
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        // No transaction to drop: log appends are already visible and upserts
        // are idempotent, so abort is intentionally a no-op.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn zset(schema: Arc<Schema>, columns: Vec<ArrayRef>, diffs: Vec<i64>) -> ZSetBatch {
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
    }

    fn int_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
    }

    #[test]
    fn rejects_retraction() {
        let z = zset(
            int_schema(),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
            vec![1, -1],
        );
        assert!(retraction_check(&z).is_err());
        let ok = zset(
            int_schema(),
            vec![Arc::new(Int64Array::from(vec![1]))],
            vec![1],
        );
        assert!(retraction_check(&ok).is_ok());
    }

    #[test]
    fn expands_multiplicity_into_rows() {
        let z = zset(
            int_schema(),
            vec![Arc::new(Int64Array::from(vec![7, 8]))],
            vec![2, 0],
        );
        let batch = rows_to_batch(&int_schema(), &z).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn builds_timestamp_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let columns: Vec<ArrayRef> = vec![Arc::new(TimestampMillisecondArray::from(vec![1000]))];
        let z = zset(schema.clone(), columns, vec![1]);
        let batch = rows_to_batch(&schema, &z).unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            schema.field(0).data_type()
        );
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn rejects_unsupported_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];
        let z = zset(int_schema(), columns, vec![1]);
        assert!(rows_to_batch(&schema, &z).is_err());
    }
}
