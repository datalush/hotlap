//! Inner-join on `Int32`/`Float64` key columns through the row converter.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::EngineCore;

fn two_column_schema(key: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", key, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

fn float_key_zset(rows: &[(f64, &str, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let batch = RecordBatch::try_new(two_column_schema(DataType::Float64), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn int_key_zset(rows: &[(i32, &str, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let batch = RecordBatch::try_new(two_column_schema(DataType::Int32), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

/// An empty engine with a single join view over `left_key`/`right_key`.
fn engine() -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        },
    )
    .unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core
}

fn strings(z: &ZSetBatch, column: usize) -> Vec<String> {
    let array = z
        .batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..z.len())
        .map(|index| array.value(index).to_string())
        .collect()
}

#[test]
fn join_matches_float64_keys_across_sides() {
    let mut core = engine();
    core.push(InputId(0), &float_key_zset(&[(1.5, "x", 1), (2.5, "y", 1)]))
        .unwrap();
    core.push(
        InputId(1),
        &float_key_zset(&[(1.5, "p", 1), (1.5, "q", -1), (3.5, "r", 1)]),
    )
    .unwrap();

    let out = core.snapshot(ViewId(0)).unwrap();
    let keys = out
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let (left, right) = (strings(&out, 1), strings(&out, 3));
    let mut rows: Vec<(f64, String, String)> = (0..out.len())
        .map(|i| (keys.value(i), left[i].clone(), right[i].clone()))
        .collect();
    rows.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(
        rows,
        vec![
            (1.5, "x".to_string(), "p".to_string()),
            (1.5, "x".to_string(), "q".to_string()),
        ]
    );
}

#[test]
fn join_matches_int32_keys_across_sides() {
    let mut core = engine();
    core.push(InputId(0), &int_key_zset(&[(1, "x", 1), (2, "y", 1)]))
        .unwrap();
    core.push(
        InputId(1),
        &int_key_zset(&[(1, "p", 1), (1, "q", -1), (3, "r", 1)]),
    )
    .unwrap();

    let out = core.snapshot(ViewId(0)).unwrap();
    let keys = out
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let (left, right) = (strings(&out, 1), strings(&out, 3));
    let mut rows: Vec<(i32, String, String)> = (0..out.len())
        .map(|i| (keys.value(i), left[i].clone(), right[i].clone()))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (1, "x".to_string(), "p".to_string()),
            (1, "x".to_string(), "q".to_string()),
        ]
    );
}
