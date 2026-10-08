mod helpers;
mod snapshot;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter};

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::converter_for;
use crate::zset::int64_diffs;

use helpers::{Closed, Windows, decode, event_times};

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
}

impl TumbleCount {
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

    /// Number of rows dropped as late since construction.
    pub fn late_dropped(&self) -> u64 {
        self.dropped_late
    }

    /// Number of deltas dropped because their window had already closed.
    pub fn late_closed_dropped(&self) -> u64 {
        self.dropped_closed
    }

    /// Number of tumbling windows still open (not yet emitted).
    pub fn open_windows(&self) -> u64 {
        self.windows.len() as u64
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
            let bucket = self
                .windows
                .entry(window_start)
                .or_default()
                .entry(bytes)
                .or_insert((key, 0));
            bucket.1 = bucket
                .1
                .checked_add(diff)
                .ok_or_else(|| EngineError::Infrastructure("window count overflow".to_string()))?;
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
                for (_, (key, count)) in buckets {
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
