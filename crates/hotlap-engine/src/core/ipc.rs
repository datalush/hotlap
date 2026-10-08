//! Arrow IPC encoding for the snapshot's rectangular tables and schemas, plus
//! a versioned binary container for whole snapshots.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bincode::Options;
use serde::Serialize;
use serde::de::DeserializeOwned;

use hotlap_core::ZSetBatch;
use hotlap_core::snapshot::{ENGINE_SNAPSHOT_FORMAT_VERSION, EngineSnapshot, SnapshotTable};

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

/// Magic bytes at the start of every binary frame.
const FRAME_MAGIC: [u8; 4] = *b"HLSP";
/// Layout version of the frame header; unknown values are rejected.
const FRAME_VERSION: u32 = 1;
/// Fixed header length: magic (4) + version (4) + payload length (8).
const FRAME_HEADER: usize = 16;
/// Upper bound for a payload length, so a corrupt prefix cannot demand an
/// unbounded allocation.
const MAX_FRAME_BYTES: u64 = 1 << 34;

/// Encodes any serializable value as a versioned, length-prefixed binary frame.
pub fn encode_framed<T: Serialize>(value: &T) -> Result<Vec<u8>, EngineError> {
    let payload = codec().serialize(value).map_err(codec_err)?;
    let mut out = Vec::with_capacity(FRAME_HEADER + payload.len());
    out.extend_from_slice(&FRAME_MAGIC);
    out.extend_from_slice(&FRAME_VERSION.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decodes a value from a binary frame, rejecting a bad magic, an unknown
/// version, a length mismatch or trailing bytes instead of panicking.
pub fn decode_framed<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, EngineError> {
    if bytes.len() < FRAME_HEADER {
        return Err(corrupt(format!("frame is {} bytes", bytes.len())));
    }
    if bytes[..4] != FRAME_MAGIC {
        return Err(corrupt("frame magic does not match".into()));
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if version != FRAME_VERSION {
        return Err(EngineError::Unsupported(format!(
            "unknown frame version {version}"
        )));
    }
    let length = u64::from_le_bytes(bytes[8..16].try_into().unwrap_or_default());
    if length > MAX_FRAME_BYTES {
        return Err(corrupt(format!("frame length {length} is too large")));
    }
    let payload = &bytes[FRAME_HEADER..];
    if payload.len() as u64 != length {
        return Err(corrupt(format!(
            "frame length {length} does not match payload {}",
            payload.len()
        )));
    }
    codec()
        .with_limit(length)
        .deserialize(payload)
        .map_err(codec_err)
}

/// Encodes an [`EngineSnapshot`] after checking its layout version.
pub fn encode_snapshot(snapshot: &EngineSnapshot) -> Result<Vec<u8>, EngineError> {
    if snapshot.format_version != ENGINE_SNAPSHOT_FORMAT_VERSION {
        return Err(EngineError::Unsupported(format!(
            "cannot encode snapshot format version {}",
            snapshot.format_version
        )));
    }
    encode_framed(snapshot)
}

/// Decodes an [`EngineSnapshot`] and rejects an unknown layout version.
pub fn decode_snapshot(bytes: &[u8]) -> Result<EngineSnapshot, EngineError> {
    let snapshot: EngineSnapshot = decode_framed(bytes)?;
    if snapshot.format_version != ENGINE_SNAPSHOT_FORMAT_VERSION {
        return Err(EngineError::Unsupported(format!(
            "unknown snapshot format version {}",
            snapshot.format_version
        )));
    }
    Ok(snapshot)
}

/// Fixed, deterministic bincode 1.x configuration: fixed-width integers and no
/// trailing bytes, so bytes produced once are read back identically.
///
/// The payload layout is coupled to bincode 1.x. Moving to a different bincode
/// major is a wire break and must bump `FRAME_VERSION`, not just the crate
/// version, or old frames would decode incorrectly.
fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}

/// Maps any bincode failure to an infrastructure error.
fn codec_err(error: Box<bincode::ErrorKind>) -> EngineError {
    EngineError::Infrastructure(format!("snapshot codec: {error}"))
}

/// Builds an infrastructure error for a structurally invalid frame.
fn corrupt(message: String) -> EngineError {
    EngineError::Infrastructure(format!("corrupt snapshot frame: {message}"))
}
