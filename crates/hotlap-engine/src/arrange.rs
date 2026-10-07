mod state;

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, new_empty_array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, Row, RowConverter};

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::{KeyConverter, converter_for};
use crate::zset::int64_diffs;
use state::RowState;

/// Incremental keyed state: `key-row -> payload-row -> diff`.
///
/// Keys and payloads live in `arrow::row` byte format, so identity is
/// byte-based and independent of the Arrow array representation. Deltas
/// accumulate across epochs; Arrow arrays appear only when
/// [`KeyedArrangement::to_zset`] materializes the state.
pub struct KeyedArrangement {
    schema: SchemaRef,
    key_indices: Vec<usize>,
    payload_indices: Vec<usize>,
    key_converter: RowConverter,
    payload_converter: RowConverter,
    state: RowState,
}

impl KeyedArrangement {
    /// Builds an arrangement over the key columns named by `key_indices`.
    ///
    /// Remaining schema columns are payload. Keys must be non-empty and in range.
    pub fn new(schema: SchemaRef, key_indices: &[usize]) -> Result<Self, EngineError> {
        if key_indices.is_empty()
            || key_indices
                .iter()
                .any(|&index| index >= schema.fields().len())
        {
            return Err(EngineError::Unsupported(
                "arrangement keys must be non-empty and in range".to_string(),
            ));
        }
        let payload_indices: Vec<usize> = (0..schema.fields().len())
            .filter(|index| !key_indices.contains(index))
            .collect();
        Ok(Self {
            key_converter: converter_for(&schema, key_indices)?,
            payload_converter: converter_for(&schema, &payload_indices)?,
            schema,
            key_indices: key_indices.to_vec(),
            payload_indices,
            state: RowState::default(),
        })
    }

    /// Applies the signed diffs of `batch` to the state.
    pub fn apply(&mut self, batch: &ZSetBatch, keys: &KeyConverter) -> Result<(), EngineError> {
        self.adjust(batch, keys, 1)
    }

    /// Inserts the rows of `batch`, adding each row's `diff`.
    pub fn insert(&mut self, batch: &ZSetBatch, keys: &KeyConverter) -> Result<(), EngineError> {
        self.apply(batch, keys)
    }

    /// Retracts the rows of `batch`, subtracting each row's `diff`.
    pub fn retract(&mut self, batch: &ZSetBatch, keys: &KeyConverter) -> Result<(), EngineError> {
        self.adjust(batch, keys, -1)
    }

    /// Number of surviving `(key, payload)` entries.
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// Returns `true` when no entry survives.
    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    /// Schema column indices treated as keys by this arrangement.
    pub fn key_indices(&self) -> &[usize] {
        &self.key_indices
    }

    /// Iterates `(key bytes, payload bytes, diff)` in deterministic order.
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>, i64)> + '_ {
        self.state
            .sorted()
            .into_iter()
            .map(|(key, payload, diff)| (key.as_ref().to_vec(), payload.as_ref().to_vec(), diff))
    }

    /// Materializes the state as a deterministic [`ZSetBatch`].
    ///
    /// Rows are ordered by key bytes, then payload bytes.
    pub fn to_zset(&self) -> Result<ZSetBatch, EngineError> {
        let entries = self.state.sorted();
        if entries.is_empty() {
            return self.empty_zset();
        }
        let keys: Vec<&OwnedRow> = entries.iter().map(|entry| entry.0).collect();
        let payloads: Vec<&OwnedRow> = entries.iter().map(|entry| entry.1).collect();
        let key_arrays = decode(&self.key_converter, &keys)?;
        let payload_arrays = decode(&self.payload_converter, &payloads)?;
        let batch = self.assemble(&key_arrays, &payload_arrays)?;
        let diffs: Vec<i64> = entries.iter().map(|entry| entry.2).collect();
        ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs)))
    }

    /// Adds `sign * diff` for every row of `batch` to the state.
    fn adjust(
        &mut self,
        batch: &ZSetBatch,
        keys: &KeyConverter,
        sign: i64,
    ) -> Result<(), EngineError> {
        if keys.column_indices() != self.key_indices.as_slice() {
            return Err(EngineError::Unsupported(
                "key converter does not match arrangement keys".to_string(),
            ));
        }
        let key_rows = keys.convert(batch.batch.columns())?;
        let payload_columns: Vec<ArrayRef> = self
            .payload_indices
            .iter()
            .map(|&index| batch.batch.column(index).clone())
            .collect();
        let payload_rows = if payload_columns.is_empty() {
            None
        } else {
            Some(self.payload_converter.convert_columns(&payload_columns)?)
        };
        let diffs = int64_diffs(batch.diff())?;
        for index in 0..batch.len() {
            let key = key_rows.row(index).owned();
            let payload = match &payload_rows {
                Some(rows) => rows.row(index).owned(),
                None => self.unit_payload(),
            };
            self.state.add(key, payload, sign * diffs.value(index));
        }
        Ok(())
    }

    /// Returns the canonical payload used when the arrangement has no payload.
    fn unit_payload(&self) -> OwnedRow {
        self.payload_converter.parser().parse(&[]).owned()
    }

    /// Rebuilds a full-schema batch from decoded key and payload columns.
    fn assemble(
        &self,
        key_arrays: &[ArrayRef],
        payload_arrays: &[ArrayRef],
    ) -> Result<RecordBatch, EngineError> {
        let columns = (0..self.schema.fields().len())
            .map(
                |field| match self.key_indices.iter().position(|&index| index == field) {
                    Some(position) => key_arrays[position].clone(),
                    None => {
                        let position = self
                            .payload_indices
                            .iter()
                            .position(|&index| index == field)
                            .unwrap_or(0);
                        payload_arrays[position].clone()
                    }
                },
            )
            .collect();
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    /// Builds an empty batch with one zero-length array per schema field.
    fn empty_zset(&self) -> Result<ZSetBatch, EngineError> {
        let columns: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();
        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        ZSetBatch::new(batch, Arc::new(Int64Array::from(Vec::<i64>::new())))
    }
}

/// Decodes `arrow::row` bytes back into column arrays for `converter`.
fn decode(converter: &RowConverter, rows: &[&OwnedRow]) -> Result<Vec<ArrayRef>, EngineError> {
    let parser = converter.parser();
    let parsed: Vec<Row<'_>> = rows.iter().map(|row| parser.parse(row.as_ref())).collect();
    Ok(converter.convert_rows(parsed)?)
}
