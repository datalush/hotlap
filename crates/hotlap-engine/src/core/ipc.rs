//! Arrow IPC encoding for the snapshot's rectangular tables and schemas.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use hotlap_core::ZSetBatch;
use hotlap_core::snapshot::SnapshotTable;

use crate::error::EngineError;
use crate::zset::int64_diffs;

/// Encodes a Z-set as an Arrow IPC table plus its diffs.
pub(crate) fn encode_zset(z: &ZSetBatch) -> Result<SnapshotTable, EngineError> {
    let ipc = encode_table(&z.batch)?;
    let diff: Vec<i64> = int64_diffs(z.diff())?.values().to_vec();
    Ok(SnapshotTable { ipc, diff })
}

/// Decodes a Z-set, rejecting a diff column whose length does not match.
pub(crate) fn decode_zset(table: &SnapshotTable) -> Result<ZSetBatch, EngineError> {
    let batch = decode_table(&table.ipc)?;
    if table.diff.len() != batch.num_rows() {
        return Err(EngineError::Infrastructure(format!(
            "snapshot diff length {} does not match table rows {}",
            table.diff.len(),
            batch.num_rows()
        )));
    }
    let diff: ArrayRef = Arc::new(Int64Array::from(table.diff.clone()));
    Ok(ZSetBatch::new(batch, diff)?)
}

/// Encodes only a schema, as an Arrow IPC stream with no batches.
pub(crate) fn encode_schema(schema: &SchemaRef) -> Result<Vec<u8>, EngineError> {
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buffer, schema.as_ref())?;
    writer.finish()?;
    Ok(buffer)
}

/// Reads the schema carried by an Arrow IPC stream.
pub(crate) fn decode_schema(bytes: &[u8]) -> Result<SchemaRef, EngineError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    Ok(reader.schema())
}

/// Writes one batch (possibly empty) as an Arrow IPC stream.
fn encode_table(batch: &RecordBatch) -> Result<Vec<u8>, EngineError> {
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buffer)
}

/// Reads the single batch carried by an Arrow IPC stream.
fn decode_table(bytes: &[u8]) -> Result<RecordBatch, EngineError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
    batches
        .into_iter()
        .next()
        .ok_or_else(|| EngineError::Infrastructure("snapshot table has no batch".to_string()))
}
