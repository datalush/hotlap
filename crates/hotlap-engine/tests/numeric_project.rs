//! Projection over `Int32`/`Float64` columns.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::ZSetBatch;
use hotlap_engine::ops::project;

fn numeric_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int32, false),
        Field::new("f", DataType::Float64, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

/// Builds `(i32, f64, &str, diff)` rows over the numeric schema.
fn numeric_zset(rows: &[(i32, f64, &str, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.2).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|row| row.3).collect();
    let batch = RecordBatch::try_new(numeric_schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

#[test]
fn project_selects_int32_and_float64_columns() {
    let input = numeric_zset(&[(1, 1.5, "a", 1), (2, 2.5, "b", -1)]);
    let out = project(&input, &[1, 0]).unwrap();

    assert_eq!(out.schema().field(0).name(), "f");
    assert_eq!(out.schema().field(1).name(), "i");
    let floats = out
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!((floats.value(0), floats.value(1)), (1.5, 2.5));
    let ints = out
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!((ints.value(0), ints.value(1)), (1, 2));
}
