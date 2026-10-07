//! Fluss append sink: writes a view's changelog to a Fluss log table.

use arrow::array::{ArrayRef, BooleanBuilder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use fluss::client::{AppendWriter, FlussConnection};
use fluss::config::Config;
use futures::StreamExt;
use hotlap::{ChangeBatch, Row, Scalar};

use crate::error::ConnectorError;
use crate::sink::{ChangeStream, Sink};

use super::{fluss_err, parse_path};

/// Append-only Fluss sink. Retractions (`diff < 0`) are rejected.
pub struct FlussSink {
    writer: AppendWriter,
    schema: SchemaRef,
}

impl FlussSink {
    /// Connect to `bootstrap` and open `path` (`<database>/<table>`) for append.
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
        let writer = table
            .new_append()
            .map_err(fluss_err)?
            .create_writer()
            .map_err(fluss_err)?;
        Ok(Self { writer, schema })
    }

    fn writer(&self) -> &AppendWriter {
        &self.writer
    }
}

/// Reject any change with a negative diff (append cannot delete).
pub fn retraction_check(batch: &ChangeBatch) -> Result<(), ConnectorError> {
    if batch.rows.iter().any(|(_, diff)| *diff < 0) {
        return Err(ConnectorError::Unsupported(
            "append sink cannot apply retractions (diff < 0)".into(),
        ));
    }
    Ok(())
}

/// Build one Arrow batch, repeating each row `diff` times (append multiplicity).
fn rows_to_batch(schema: &SchemaRef, batch: &ChangeBatch) -> Result<RecordBatch, ConnectorError> {
    retraction_check(batch)?;
    let rows: Vec<&Row> = batch
        .rows
        .iter()
        .flat_map(|(row, diff)| std::iter::repeat_n(row, usize::try_from(*diff).unwrap_or(0)))
        .collect();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (col, field) in schema.fields().iter().enumerate() {
        arrays.push(build_column(field.data_type(), col, &rows)?);
    }
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|error| ConnectorError::Arrow(format!("record batch: {error}")))
}

fn build_column(
    data_type: &DataType,
    col: usize,
    rows: &[&Row],
) -> Result<ArrayRef, ConnectorError> {
    match data_type {
        DataType::Int64 | DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.0.get(col) {
                    Some(Scalar::I64(value)) => builder.append_value(*value),
                    Some(Scalar::Null) | None => builder.append_null(),
                    other => return Err(mismatch(data_type, other)),
                }
            }
            Ok(std::sync::Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
            for row in rows {
                match row.0.get(col) {
                    Some(Scalar::Str(value)) => builder.append_value(value),
                    Some(Scalar::Null) | None => builder.append_null(),
                    other => return Err(mismatch(data_type, other)),
                }
            }
            Ok(std::sync::Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.0.get(col) {
                    Some(Scalar::Bool(value)) => builder.append_value(*value),
                    Some(Scalar::Null) | None => builder.append_null(),
                    other => return Err(mismatch(data_type, other)),
                }
            }
            Ok(std::sync::Arc::new(builder.finish()))
        }
        other => Err(ConnectorError::Unsupported(format!(
            "unsupported column type {other:?}"
        ))),
    }
}

fn mismatch(data_type: &DataType, scalar: Option<&Scalar>) -> ConnectorError {
    ConnectorError::Arrow(format!(
        "value {scalar:?} does not match column type {data_type:?}"
    ))
}

#[async_trait]
impl Sink for FlussSink {
    async fn write(&self, mut changes: ChangeStream) -> Result<(), ConnectorError> {
        while let Some(item) = changes.next().await {
            let batch = item?;
            let records = rows_to_batch(&self.schema, &batch)?;
            if records.num_rows() == 0 {
                continue;
            }
            // Fire-and-forget: errors surface on the awaited `flush` in commit.
            drop(
                self.writer()
                    .append_arrow_batch(records)
                    .map_err(fluss_err)?,
            );
        }
        Ok(())
    }

    async fn commit(&self) -> Result<(), ConnectorError> {
        self.writer().flush().await.map_err(fluss_err)
    }

    async fn abort(&self) -> Result<(), ConnectorError> {
        // Append has no transaction; already-queued writes are already visible.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use hotlap::{ChangeBatch, Row, Scalar};

    use super::*;

    #[test]
    fn rejects_retraction() {
        let mut b = ChangeBatch::default();
        b.push(Row(vec![Scalar::I64(1)]), 1);
        assert!(retraction_check(&b).is_ok());
        b.push(Row(vec![Scalar::I64(2)]), -1);
        assert!(retraction_check(&b).is_err());
    }

    #[test]
    fn expands_multiplicity_into_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mut b = ChangeBatch::default();
        b.push(Row(vec![Scalar::I64(7)]), 2);
        b.push(Row(vec![Scalar::I64(8)]), 0);
        let batch = rows_to_batch(&schema, &b).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn rejects_unsupported_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let mut b = ChangeBatch::default();
        b.push(Row(vec![Scalar::Null]), 1);
        assert!(rows_to_batch(&schema, &b).is_err());
    }
}
