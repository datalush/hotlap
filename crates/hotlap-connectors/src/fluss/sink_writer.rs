//! Fluss writers behind the append and upsert sink modes.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use fluss::client::{AppendWriter, UpsertWriter};
use fluss::metadata::RowType;
use fluss::record::ArrowReader;

use crate::error::ConnectorError;
use crate::sink::SinkCapabilities;

use super::fluss_err;

/// The Fluss writer matching the target table's key type.
pub(crate) enum FlussWriter {
    /// Append-only log table (no primary key): at-least-once.
    Append(AppendWriter),
    /// Primary-key table: replay-safe keyed upserts, so effectively-once.
    Upsert {
        writer: UpsertWriter,
        row_type: Arc<RowType>,
    },
}

impl FlussWriter {
    /// Delivery guarantee implied by the writer kind.
    pub(crate) fn capabilities(&self) -> SinkCapabilities {
        match self {
            Self::Append(_) => SinkCapabilities::AtLeastOnce,
            Self::Upsert { .. } => SinkCapabilities::Idempotent,
        }
    }

    /// Queue one already-expanded batch.
    pub(crate) fn write_batch(&self, batch: &RecordBatch) -> Result<(), ConnectorError> {
        match self {
            Self::Append(writer) => {
                // Fire-and-forget: errors surface on the awaited flush in commit.
                drop(
                    writer
                        .append_arrow_batch(batch.clone())
                        .map_err(fluss_err)?,
                );
                Ok(())
            }
            Self::Upsert { writer, row_type } => {
                let reader = ArrowReader::new(Arc::new(batch.clone()), Arc::clone(row_type))
                    .map_err(fluss_err)?;
                for row in 0..reader.row_count() {
                    drop(writer.upsert(&reader.read(row)).map_err(fluss_err)?);
                }
                Ok(())
            }
        }
    }

    /// Flush every queued write; this is the sink's commit.
    pub(crate) async fn flush(&self) -> Result<(), ConnectorError> {
        match self {
            Self::Append(writer) => writer.flush().await.map_err(fluss_err),
            Self::Upsert { writer, .. } => writer.flush().await.map_err(fluss_err),
        }
    }
}
