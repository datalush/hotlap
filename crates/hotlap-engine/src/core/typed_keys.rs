use arrow::array::ArrayData;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use std::io::Cursor;

use crate::error::EngineError;

#[path = "typed_keys/capture.rs"]
mod capture;
use capture::compact_array;

#[derive(Clone)]
pub(crate) struct TypedKeyRow {
    batch: RecordBatch,
}

impl TypedKeyRow {
    pub(crate) fn capture(source: &RecordBatch, row: usize) -> Result<Self, EngineError> {
        if row >= source.num_rows() {
            return Err(EngineError::Infrastructure(
                "typed key row out of bounds".into(),
            ));
        }
        let schema = source.schema();
        let mut columns = Vec::with_capacity(source.num_columns());
        for (column, field) in source.columns().iter().zip(schema.fields()) {
            columns.push(compact_array(column, field, row)?);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(1));
        let batch = RecordBatch::try_new_with_options(schema, columns, &options)?;
        Ok(Self { batch })
    }

    pub(crate) fn batch(&self) -> &RecordBatch {
        &self.batch
    }
}

pub(crate) fn schema_uses_native_capture(schema: &SchemaRef) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| needs_native(field.data_type()))
}

fn needs_native(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _)
        | DataType::Union(_, _)
        | DataType::RunEndEncoded(_, _)
        | DataType::Utf8View
        | DataType::BinaryView
        | DataType::ListView(_)
        | DataType::LargeListView(_) => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            needs_native(field.data_type())
        }
        DataType::Map(field, _) => needs_native(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| needs_native(field.data_type())),
        _ => false,
    }
}

pub(crate) fn encode<'a>(
    schema: &SchemaRef,
    batches: impl IntoIterator<Item = &'a RecordBatch>,
) -> Result<Vec<u8>, EngineError> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema.as_ref())?;
    for batch in batches {
        if batch.schema().as_ref() != schema.as_ref() {
            return Err(EngineError::Infrastructure(
                "typed key schema mismatch".into(),
            ));
        }
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(bytes)
}

pub(crate) fn decode(
    bytes: &[u8],
    expected: &SchemaRef,
) -> Result<(Vec<RecordBatch>, usize), EngineError> {
    const EOS: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor.clone(), None)?;
    if reader.schema().as_ref() != expected.as_ref() {
        return Err(EngineError::Infrastructure(
            "typed key schema mismatch".into(),
        ));
    }
    let mut batches = Vec::new();
    for batch in reader.by_ref() {
        batches.push(batch?);
    }
    let consumed = reader.get_ref().position() as usize;
    drop(reader);
    if consumed != bytes.len() || !bytes.ends_with(&EOS) {
        return Err(EngineError::Infrastructure(
            "typed key stream is truncated or has trailing bytes".into(),
        ));
    }
    let rows = batches.iter().try_fold(0usize, |total, batch| {
        for column in batch.columns() {
            let data: ArrayData = column.to_data();
            data.validate_full()?;
        }
        total
            .checked_add(batch.num_rows())
            .ok_or_else(|| EngineError::Infrastructure("typed key row count overflow".into()))
    })?;
    Ok((batches, rows))
}
