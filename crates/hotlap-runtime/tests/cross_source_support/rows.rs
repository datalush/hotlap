//! Oracle recomputation, source schemas and result readers for the SQL oracle.
//!
//! The recompute helper is independent of the incremental engine: it folds the
//! raw input rows and multiplicities, so comparing a snapshot against it is
//! evidence, not a restatement of the join algorithm.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap::ZSetBatch;
use hotlap_connectors::source::SourceBatch;
use hotlap_runtime::QueryResult;

use crate::factory::CrossSourceFactory;

/// Source `left`: `(k, lv, _event_time)`, event-time at index 2.
pub fn schema_left() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("lv", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// Source `right`: `(k, rv, _event_time)`, event-time at index 2.
pub fn schema_right() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("rv", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

/// An Arrow array of `i64`, for building source batches.
pub fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

/// A split-0 append batch over `schema` with `columns`.
pub fn append_batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> SourceBatch {
    let rows = columns.first().map(|column| column.len()).unwrap_or(0);
    SourceBatch {
        batch: RecordBatch::try_new(schema, columns).unwrap(),
        base_offset: 0,
        next_offset: rows as i64,
        split: 0,
    }
}

/// Full recompute of `left JOIN right ON k` with insert/retract multiplicities.
pub fn recompute(left: &[(i64, i64, i64)], right: &[(i64, i64, i64)]) -> Vec<(i64, i64, i64, i64)> {
    let mut rows: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
    for &(lk, lv, ld) in left {
        for &(rk, rv, rd) in right {
            if lk == rk {
                *rows.entry((lk, lv, rv)).or_insert(0) += ld * rd;
            }
        }
    }
    rows.into_iter()
        .filter(|(_, diff)| *diff != 0)
        .map(|((k, l, r), diff)| (k, l, r, diff))
        .collect()
}

fn column(zset: &ZSetBatch, index: usize) -> Vec<i64> {
    let array = zset
        .batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..zset.len()).map(|row| array.value(row)).collect()
}

fn diff_column(zset: &ZSetBatch) -> Vec<i64> {
    let array = zset.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..zset.len()).map(|row| array.value(row)).collect()
}

/// A `(k, lv, rv)` MV snapshot as sorted `(k, lv, rv, diff)` tuples.
pub fn zset_tuples(zset: &ZSetBatch) -> Vec<(i64, i64, i64, i64)> {
    if zset.is_empty() {
        return Vec::new();
    }
    let (keys, left, right) = (column(zset, 0), column(zset, 1), column(zset, 2));
    let diffs = diff_column(zset);
    (0..zset.len())
        .map(|row| (keys[row], left[row], right[row], diffs[row]))
        .collect()
}

/// Push `batch` into the single split of source `name`.
pub fn send(factory: &CrossSourceFactory, name: &str, batch: SourceBatch) {
    factory.senders(name)[0]
        .send(Ok(batch))
        .expect("source channel open");
}

/// Poll until source `name` has acked at least `count` batches.
pub fn wait_commits(factory: &CrossSourceFactory, name: &str, count: usize) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if factory.source(name).commits().len() >= count {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

/// A result set as sorted `Vec<i64>` rows (the SQL surface drops multiplicities).
pub fn query_rows(result: QueryResult) -> Vec<Vec<i64>> {
    let QueryResult::Rows(batches) = result else {
        panic!("expected a result set");
    };
    let mut out = Vec::new();
    for batch in batches {
        let columns: Vec<&Int64Array> = batch
            .columns()
            .iter()
            .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            out.push(columns.iter().map(|column| column.value(row)).collect());
        }
    }
    out.sort_unstable();
    out
}
