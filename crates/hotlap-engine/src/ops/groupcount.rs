use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, new_empty_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{Row, RowConverter};

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::converter_for;
use crate::zset::int64_diffs;

/// Stateful incremental `groupcount` over a keyed delta.
///
/// The reducer maintains `key -> count`, updating only the keys touched by the
/// incoming delta, so a push costs O(delta), not O(accumulated keyspace). Each
/// `apply` emits one `(key..., count)` row with a signed diff per key whose count
/// changed: a key that crosses to zero emits its old count with `-1`; a new key
/// emits its count with `+1`; a changed key emits the old count with `-1` and the
/// new count with `+1`.
pub struct GroupCount {
    key: Vec<usize>,
    schema: Option<SchemaRef>,
    converter: Option<RowConverter>,
    counts: HashMap<Vec<u8>, i64>,
    #[cfg(test)]
    work: u64,
}

impl GroupCount {
    /// Creates a reducer over the key columns named by `key`.
    pub fn new(key: &[usize]) -> Self {
        Self {
            key: key.to_vec(),
            schema: None,
            converter: None,
            counts: HashMap::new(),
            #[cfg(test)]
            work: 0,
        }
    }

    /// Applies one input delta and returns only the changed `(key, count)` rows.
    pub fn apply(&mut self, z: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
        self.ensure_schema(z)?;
        let constants = self.touched(z)?;
        #[cfg(test)]
        {
            self.work = constants.len() as u64;
        }
        let mut rows: Vec<(Vec<u8>, i64, i64)> = Vec::new();
        for (bytes, delta) in constants {
            if delta == 0 {
                continue;
            }
            let old = self.counts.get(&bytes).copied().unwrap_or(0);
            let new = old
                .checked_add(delta)
                .ok_or_else(|| EngineError::Infrastructure("group count overflow".to_string()))?;
            if new == 0 {
                self.counts.remove(&bytes);
            } else {
                self.counts.insert(bytes.clone(), new);
            }
            if old != 0 {
                rows.push((bytes.clone(), old, -1));
            }
            if new != 0 {
                rows.push((bytes, new, 1));
            }
        }
        self.materialize(&rows)
    }

    /// Number of keys touched by the last `apply` (test instrumentation only).
    #[cfg(test)]
    pub fn work(&self) -> u64 {
        self.work
    }

    /// Validates the key columns and learns the schema on first use.
    fn ensure_schema(&mut self, z: &ZSetBatch) -> Result<(), EngineError> {
        let schema = z.schema();
        let width = schema.fields().len();
        if self.key.is_empty() || self.key.iter().any(|&index| index >= width) {
            return Err(EngineError::Unsupported(
                "group key out of range".to_string(),
            ));
        }
        match &self.schema {
            Some(existing) if existing != &schema => Err(EngineError::Unsupported(
                "group input schema changed".to_string(),
            )),
            Some(_) => Ok(()),
            None => {
                self.converter = Some(converter_for(&schema, &self.key)?);
                self.schema = Some(schema);
                Ok(())
            }
        }
    }

    /// Sums the delta's diffs per key, returning one `(key bytes, delta)` each.
    fn touched(&self, z: &ZSetBatch) -> Result<Vec<(Vec<u8>, i64)>, EngineError> {
        let converter = self
            .converter
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("group converter missing".to_string()))?;
        let key_columns: Vec<ArrayRef> = self
            .key
            .iter()
            .map(|&index| z.batch.column(index).clone())
            .collect();
        let key_rows = converter.convert_columns(&key_columns)?;
        let diffs = int64_diffs(z.diff())?;

        let mut deltas: Vec<(Vec<u8>, i64)> = Vec::new();
        let mut positions: HashMap<Vec<u8>, usize> = HashMap::new();
        for index in 0..z.len() {
            let bytes = key_rows.row(index).as_ref().to_vec();
            let diff = diffs.value(index);
            match positions.get(&bytes) {
                Some(&slot) => {
                    deltas[slot].1 = deltas[slot].1.checked_add(diff).ok_or_else(|| {
                        EngineError::Infrastructure("group delta overflow".to_string())
                    })?;
                }
                None => {
                    positions.insert(bytes.clone(), deltas.len());
                    deltas.push((bytes, diff));
                }
            }
        }
        Ok(deltas)
    }

    /// Builds the `(key columns, count)` changelog rows with signed diffs.
    fn materialize(&self, rows: &[(Vec<u8>, i64, i64)]) -> Result<ZSetBatch, EngineError> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("group schema missing".to_string()))?;
        let mut fields: Vec<Field> = self.key.iter().map(|&i| schema.field(i).clone()).collect();
        fields.push(Field::new("count", DataType::Int64, false));
        let fields = Arc::new(Schema::new(fields));
        if rows.is_empty() {
            let mut columns: Vec<ArrayRef> = self
                .key
                .iter()
                .map(|&i| new_empty_array(schema.field(i).data_type()))
                .collect();
            columns.push(Arc::new(Int64Array::from(Vec::<i64>::new())));
            let batch = RecordBatch::try_new(fields, columns)?;
            return Ok(ZSetBatch::new(
                batch,
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            )?);
        }
        let converter = self
            .converter
            .as_ref()
            .ok_or_else(|| EngineError::Infrastructure("group converter missing".to_string()))?;
        let parser = converter.parser();
        let parsed: Vec<Row<'_>> = rows
            .iter()
            .map(|(bytes, _, _)| parser.parse(bytes))
            .collect();
        let mut columns = converter.convert_rows(parsed)?;
        columns.push(Arc::new(Int64Array::from(
            rows.iter().map(|(_, count, _)| *count).collect::<Vec<_>>(),
        )));
        let batch = RecordBatch::try_new(fields, columns)?;
        let diff: ArrayRef = Arc::new(Int64Array::from(
            rows.iter().map(|(_, _, diff)| *diff).collect::<Vec<_>>(),
        ));
        Ok(ZSetBatch::new(batch, diff)?)
    }
}

#[cfg(test)]
mod tests;
