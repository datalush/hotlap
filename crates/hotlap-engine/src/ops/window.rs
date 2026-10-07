use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, new_empty_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, Row, RowConverter};

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::converter_for;
use crate::zset::int64_diffs;

/// A single closed window: `(key row, window_start, count)`.
type Closed = (OwnedRow, i64, i64);

/// Stateful tumbling-window count over event-time.
///
/// `apply` assigns every on-time row to the window `[ws, ws + size)` with
/// `ws = (event_ts / size) * size`, accumulating signed diffs per `(key, ws)`
/// bucket. A row whose `event_ts` is below the *previous* watermark is late and
/// dropped. Windows whose end the new watermark has reached are emitted once,
/// append-only, as `key ++ [window_start, count]`, then freed. A bucket that
/// consolidates to zero before close emits nothing.
pub struct TumbleCount {
    key: Vec<usize>,
    time_col: usize,
    size: i64,
    watermark: i64,
    dropped_late: u64,
    schema: Option<SchemaRef>,
    converter: Option<RowConverter>,
    buckets: HashMap<(OwnedRow, i64), i64>,
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
            schema: None,
            converter: None,
            buckets: HashMap::new(),
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
        let converter = self.converter.as_ref().expect("converter initialized");
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
            if ts < self.watermark {
                self.dropped_late += 1;
                continue;
            }
            let window_start = (ts / self.size) * self.size;
            let key = key_rows.row(index).owned();
            *self.buckets.entry((key, window_start)).or_insert(0) += diffs.value(index);
        }
        Ok(())
    }

    /// Removes the buckets whose window end the watermark has reached.
    fn take_closed(&mut self) -> Vec<Closed> {
        let mut closed: Vec<((OwnedRow, i64), i64)> = self
            .buckets
            .iter()
            .filter(|((_, ws), _)| ws.saturating_add(self.size) <= self.watermark)
            .map(|(bucket, &count)| (bucket.clone(), count))
            .collect();
        closed.sort_by(|a, b| a.0.0.cmp(&b.0.0).then_with(|| a.0.1.cmp(&b.0.1)));
        let mut out = Vec::new();
        for (bucket, count) in closed {
            self.buckets.remove(&bucket);
            if count != 0 {
                out.push((bucket.0, bucket.1, count));
            }
        }
        out
    }

    /// Builds the `key ++ [window_start, count]` batch with an all-ones diff.
    fn materialize(&self, closed: &[Closed]) -> Result<ZSetBatch, EngineError> {
        let schema = self.schema.as_ref().expect("schema initialized");
        let mut fields: Vec<Field> = self
            .key
            .iter()
            .map(|&index| schema.field(index).clone())
            .collect();
        fields.push(Field::new("window_start", DataType::Int64, false));
        fields.push(Field::new("count", DataType::Int64, false));

        if closed.is_empty() {
            let mut columns: Vec<ArrayRef> = self
                .key
                .iter()
                .map(|&index| new_empty_array(schema.field(index).data_type()))
                .collect();
            columns.push(Arc::new(Int64Array::from(Vec::<i64>::new())));
            columns.push(Arc::new(Int64Array::from(Vec::<i64>::new())));
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
            return ZSetBatch::new(batch, Arc::new(Int64Array::from(Vec::<i64>::new())));
        }

        let converter = self.converter.as_ref().expect("converter initialized");
        let key_rows: Vec<&OwnedRow> = closed.iter().map(|entry| &entry.0).collect();
        let mut columns = decode(converter, &key_rows)?;
        columns.push(Arc::new(Int64Array::from(
            closed.iter().map(|entry| entry.1).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            closed.iter().map(|entry| entry.2).collect::<Vec<_>>(),
        )));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        let diff: ArrayRef = Arc::new(Int64Array::from(vec![1i64; closed.len()]));
        ZSetBatch::new(batch, diff)
    }
}

/// Reads an event-time column as non-negative `i64`, mapping nulls to zero.
fn event_times(column: &ArrayRef) -> Result<Int64Array, EngineError> {
    let casted = cast(column.as_ref(), &DataType::Int64)?;
    let ints = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| EngineError::Infrastructure("int64 cast produced wrong type".to_string()))?;
    let values: Vec<i64> = ints.iter().map(|value| value.unwrap_or(0).max(0)).collect();
    Ok(Int64Array::from(values))
}

/// Decodes `arrow::row` key bytes back into column arrays for `converter`.
fn decode(converter: &RowConverter, rows: &[&OwnedRow]) -> Result<Vec<ArrayRef>, EngineError> {
    let parser = converter.parser();
    let parsed: Vec<Row<'_>> = rows.iter().map(|row| parser.parse(row.as_ref())).collect();
    Ok(converter.convert_rows(parsed)?)
}
