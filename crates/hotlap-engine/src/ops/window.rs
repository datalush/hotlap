mod counters;
mod helpers;
mod snapshot;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter};

use crate::batch::ZSetBatch;
use crate::core::ipc::typed_keys::{TypedKeyRow, schema_uses_native_capture};
use crate::error::EngineError;
use crate::keys::converter_for;
use crate::zset::int64_diffs;

use helpers::{Closed, Windows, decode, event_times};

#[cfg(test)]
mod snapshot_tests;

/// Stateful tumbling-window count over event-time.
///
/// Open buckets live in a `BTreeMap` keyed by window start, so closing visits
/// only windows whose end the watermark reached (O(closed)). Each delta row
/// joins `[ws, ws + size)`. Late insertions and closed-window deltas drop.
pub struct TumbleCount {
    key: Vec<usize>,
    time_col: usize,
    size: i64,
    watermark: i64,
    dropped_late: u64,
    dropped_closed: u64,
    schema: Option<SchemaRef>,
    converter: Option<RowConverter>,
    windows: Windows,
    native_keys: Option<BTreeMap<(i64, Vec<u8>), TypedKeyRow>>,
}

impl TumbleCount {
    pub(crate) fn has_open_windows(&self) -> bool {
        !self.windows.is_empty()
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.schema.is_some()
    }

    pub(crate) fn memory_usage(&self) -> Result<(usize, usize, usize), EngineError> {
        let mut payload = 0usize;
        let mut key_capacity = self
            .key
            .capacity()
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| EngineError::Infrastructure("window config capacity overflow".into()))?;
        let mut structural = self
            .windows
            .len()
            .checked_mul(std::mem::size_of::<(i64, BTreeMap<Vec<u8>, (OwnedRow, i64)>)>())
            .ok_or_else(|| EngineError::Infrastructure("window estimate overflow".into()))?;
        for buckets in self.windows.values() {
            structural = structural
                .checked_add(
                    buckets
                        .len()
                        .checked_mul(std::mem::size_of::<(Vec<u8>, (OwnedRow, i64))>())
                        .ok_or_else(|| {
                            EngineError::Infrastructure("window estimate overflow".into())
                        })?,
                )
                .ok_or_else(|| EngineError::Infrastructure("window estimate overflow".into()))?;
            for (key, (row, _)) in buckets {
                payload = payload
                    .checked_add(key.len())
                    .and_then(|total| total.checked_add(row.as_ref().len()))
                    .ok_or_else(|| EngineError::Infrastructure("window payload overflow".into()))?;
                key_capacity = key_capacity.checked_add(key.capacity()).ok_or_else(|| {
                    EngineError::Infrastructure("window capacity overflow".into())
                })?;
            }
        }
        Ok((payload, key_capacity, structural))
    }

    /// Creates a window over the given key and event-time columns. The input
    /// schema is learned from the first `apply`; `size` is in event-time units.
    pub fn new(key: &[usize], time_col: usize, size: i64) -> Self {
        Self {
            key: key.to_vec(),
            time_col,
            size,
            watermark: 0,
            dropped_late: 0,
            dropped_closed: 0,
            schema: None,
            converter: None,
            windows: BTreeMap::new(),
            native_keys: None,
        }
    }

    /// Applies one input delta at watermark `watermark` and returns the windows
    /// that close at this watermark as an append-only changelog.
    pub fn apply(&mut self, z: &ZSetBatch, watermark: i64) -> Result<ZSetBatch, EngineError> {
        self.ensure_schema(z)?;
        self.record(z)?;
        self.watermark = self.watermark.max(watermark.max(0));
        let closed = self.take_closed();
        self.materialize(&closed)
    }

    /// Validates the window against the input schema, learning it on first use.
    fn ensure_schema(&mut self, z: &ZSetBatch) -> Result<(), EngineError> {
        if self.size <= 0 {
            return Err(EngineError::Unsupported(
                "window size must be positive".to_string(),
            ));
        }
        let schema = z.schema();
        let columns = schema.fields().len();
        if self.key.is_empty()
            || self.key.iter().any(|&index| index >= columns)
            || self.time_col >= columns
        {
            return Err(EngineError::Unsupported(
                "window columns out of range".to_string(),
            ));
        }
        match &self.schema {
            Some(existing) if existing != &schema => Err(EngineError::Unsupported(
                "window input schema changed".to_string(),
            )),
            Some(_) => Ok(()),
            None => {
                self.converter = Some(converter_for(&schema, &self.key)?);
                let key_schema = Arc::new(Schema::new(
                    self.key
                        .iter()
                        .map(|&i| schema.field(i).clone())
                        .collect::<Vec<_>>(),
                ));
                if schema_uses_native_capture(&key_schema) {
                    self.native_keys = Some(BTreeMap::new());
                }
                self.schema = Some(schema);
                Ok(())
            }
        }
    }

    /// Assigns every on-time row of `z` to its `(key, window_start)` bucket.
    fn record(&mut self, z: &ZSetBatch) -> Result<(), EngineError> {
        let converter = self
            .converter
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("window converter missing".to_string()))?;
        let key_columns: Vec<ArrayRef> = self
            .key
            .iter()
            .map(|&index| z.batch.column(index).clone())
            .collect();
        let key_rows = converter.convert_columns(&key_columns)?;
        let key_schema = Arc::new(Schema::new(
            self.key
                .iter()
                .map(|&i| z.schema().field(i).clone())
                .collect::<Vec<_>>(),
        ));
        let key_batch = RecordBatch::try_new(key_schema, key_columns)?;
        let times = event_times(z.batch.column(self.time_col))?;
        let diffs = int64_diffs(z.diff())?;
        for index in 0..z.len() {
            let ts = times.value(index);
            let diff = diffs.value(index);
            if diff > 0 && ts < self.watermark {
                self.dropped_late += 1;
                continue;
            }
            let window_start = (ts / self.size) * self.size;
            if window_start <= self.watermark.saturating_sub(self.size) {
                // Already closed/emitted: append-only output cannot retract it.
                self.dropped_closed += 1;
                continue;
            }
            let key = key_rows.row(index).owned();
            let bytes = key.as_ref().to_vec();
            let existing = self.windows.get(&window_start).and_then(|b| b.get(&bytes));
            if let Some((_, count)) = existing {
                let updated = count.checked_add(diff).ok_or_else(|| {
                    EngineError::Infrastructure("window count overflow".to_string())
                })?;
                self.windows
                    .get_mut(&window_start)
                    .unwrap()
                    .get_mut(&bytes)
                    .unwrap()
                    .1 = updated;
                continue;
            }
            let native = self
                .native_keys
                .as_ref()
                .map(|_| TypedKeyRow::capture(&key_batch, index))
                .transpose()?;
            self.windows
                .entry(window_start)
                .or_default()
                .insert(bytes.clone(), (key, diff));
            if let (Some(cache), Some(native)) = (&mut self.native_keys, native) {
                cache.insert((window_start, bytes), native);
            }
        }
        Ok(())
    }

    /// Removes the buckets whose window end the watermark has reached.
    fn take_closed(&mut self) -> Vec<Closed> {
        let max_start = self.watermark.saturating_sub(self.size);
        let ready: Vec<i64> = self
            .windows
            .range(..=max_start)
            .map(|(window_start, _)| *window_start)
            .collect();
        let mut closed = Vec::new();
        for window_start in ready {
            if let Some(buckets) = self.windows.remove(&window_start) {
                for (key_bytes, (key, count)) in buckets {
                    if let Some(cache) = &mut self.native_keys {
                        cache.remove(&(window_start, key_bytes));
                    }
                    if count != 0 {
                        closed.push((key, window_start, count));
                    }
                }
            }
        }
        closed
    }

    /// Builds the `key ++ [window_start, count]` batch with an all-ones diff.
    fn materialize(&self, closed: &[Closed]) -> Result<ZSetBatch, EngineError> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("window schema missing".to_string()))?;
        let mut fields: Vec<Field> = self.key.iter().map(|&i| schema.field(i).clone()).collect();
        fields.push(Field::new("window_start", DataType::Int64, false));
        fields.push(Field::new("count", DataType::Int64, false));
        let fields = Arc::new(Schema::new(fields));

        if closed.is_empty() {
            return helpers::empty_output(fields, &self.key, schema);
        }

        let converter = self
            .converter
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("window converter missing".to_string()))?;
        let key_rows: Vec<&OwnedRow> = closed.iter().map(|entry| &entry.0).collect();
        let mut columns = decode(converter, &key_rows)?;
        columns.push(Arc::new(Int64Array::from(
            closed.iter().map(|entry| entry.1).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            closed.iter().map(|entry| entry.2).collect::<Vec<_>>(),
        )));
        let batch = RecordBatch::try_new(fields, columns)?;
        let diff: ArrayRef = Arc::new(Int64Array::from(vec![1i64; closed.len()]));
        Ok(ZSetBatch::new(batch, diff)?)
    }
}
