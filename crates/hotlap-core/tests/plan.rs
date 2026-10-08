//! Tests for the plan IR's predicate evaluation over Arrow columns.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::{Predicate, Scalar};

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

/// Three rows over an `i32` column and an `f64` column, with a null each.
fn numeric_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int32, true),
        Field::new("f", DataType::Float64, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
        Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

fn values(mask: &BooleanArray) -> Vec<bool> {
    (0..mask.len()).map(|index| mask.value(index)).collect()
}

#[test]
fn eq_compares_integers() {
    let mask = Predicate::Eq(0, Scalar::I64(2)).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![false, true, false]);
}

#[test]
fn gt_compares_integers() {
    let mask = Predicate::Gt(0, 1).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![false, true, false]);
}

#[test]
fn eq_compares_strings() {
    let mask = Predicate::Eq(1, Scalar::Str("a".into()))
        .eval(&batch())
        .unwrap();
    assert_eq!(values(&mask), vec![true, false, true]);
}

#[test]
fn eq_null_matches_nulls() {
    let mask = Predicate::Eq(0, Scalar::Null).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![false, false, true]);
}

#[test]
fn out_of_range_column_is_unsupported() {
    assert!(Predicate::Gt(9, 0).eval(&batch()).is_err());
}

#[test]
fn eq_compares_int32() {
    let mask = Predicate::Eq(0, Scalar::I32(2))
        .eval(&numeric_batch())
        .unwrap();
    assert_eq!(values(&mask), vec![false, true, false]);
}

#[test]
fn gt_compares_int32() {
    let mask = Predicate::Gt(0, 1).eval(&numeric_batch()).unwrap();
    assert_eq!(values(&mask), vec![false, true, false]);
}

#[test]
fn eq_compares_float64() {
    let mask = Predicate::Eq(1, Scalar::F64(2.5))
        .eval(&numeric_batch())
        .unwrap();
    assert_eq!(values(&mask), vec![false, true, false]);
}

#[test]
fn gt_compares_float64_without_truncating() {
    // The integer bound must be compared as a float: 1.5 > 1 is true.
    let mask = Predicate::Gt(1, 1).eval(&numeric_batch()).unwrap();
    assert_eq!(values(&mask), vec![true, true, false]);
}
