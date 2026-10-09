use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap::ZSetBatch;

use super::*;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]))
}

fn zset(keys: &[i64], values: &[&str], diff: &[i64]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys.to_vec())),
        Arc::new(StringArray::from(values.to_vec())),
    ];
    let batch = RecordBatch::try_new(schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diff.to_vec()))).unwrap()
}

#[test]
fn zset_to_batch_keeps_nonzero_rows() {
    let batch = zset_to_batch(&schema(), &zset(&[1, 2], &["a", "b"], &[1, 0])).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
}

#[test]
fn empty_zset_yields_schema_only_batch() {
    let empty = ZSetBatch::empty(schema());
    let batch = zset_to_batch(&schema(), &empty).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 2);
}

#[test]
fn expands_positive_multiplicity_into_bag_rows() {
    // A weight of 3 means the row occurs three times (SQL bag, no DISTINCT).
    let batch = zset_to_batch(&schema(), &zset(&[7], &["x"], &[3])).unwrap();
    assert_eq!(batch.num_rows(), 3);
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for row in 0..3 {
        assert_eq!(keys.value(row), 7);
        assert_eq!(values.value(row), "x");
    }
}

#[test]
fn drops_zero_multiplicity_rows() {
    let batch = zset_to_batch(&schema(), &zset(&[1, 2], &["a", "b"], &[0, 2])).unwrap();
    assert_eq!(batch.num_rows(), 2);
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((keys.value(0), keys.value(1)), (2, 2));
}

#[test]
fn rejects_negative_multiplicity() {
    let error = zset_to_batch(&schema(), &zset(&[1], &["a"], &[-1])).unwrap_err();
    assert!(matches!(error, SqlError::Unsupported(_)), "got {error:?}");
}

#[test]
fn rejects_expansion_over_the_row_cap_before_allocating() {
    let error = zset_to_batch(
        &schema(),
        &zset(&[1], &["a"], &[(MAX_SNAPSHOT_ROWS as i64) + 1]),
    )
    .unwrap_err();
    assert!(matches!(error, SqlError::Unsupported(_)), "got {error:?}");
}

#[test]
fn rejects_overflowing_expansion() {
    let error = zset_to_batch(
        &schema(),
        &zset(&[1, 2], &["a", "b"], &[i64::MAX, i64::MAX]),
    )
    .unwrap_err();
    assert!(matches!(error, SqlError::Unsupported(_)), "got {error:?}");
}

#[test]
fn rejects_unsupported_type() {
    let bad = Arc::new(Schema::new(vec![Field::new("f", DataType::UInt64, true)]));
    assert!(ensure_kernel_types(&bad).is_err());
}

#[test]
fn accepts_int32_and_float64() {
    let schema = Schema::new(vec![
        Field::new("i", DataType::Int32, true),
        Field::new("f", DataType::Float64, true),
    ]);
    assert!(ensure_kernel_types(&schema).is_ok());
}
