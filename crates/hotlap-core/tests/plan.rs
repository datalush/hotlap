//! Tests for the plan IR's predicate evaluation over Arrow columns.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::plan::{CmpOp, Predicate, Scalar};

/// Three rows across every scalar type; `i` has a null in the middle.
fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int32, true),
        Field::new("f", DataType::Float64, true),
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
        Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0)])),
        Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])),
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

/// The mask as three-valued cells: `None` is SQL `NULL`/unknown.
fn values(mask: &BooleanArray) -> Vec<Option<bool>> {
    (0..mask.len())
        .map(|index| (!mask.is_null(index)).then(|| mask.value(index)))
        .collect()
}

fn cmp(op: CmpOp, col: usize, scalar: Scalar) -> Predicate {
    Predicate::Cmp { op, col, scalar }
}

#[test]
fn cmp_lt_excludes_nulls() {
    let mask = cmp(CmpOp::Lt, 0, Scalar::I32(2)).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(true), None, Some(false)]);
}

#[test]
fn cmp_le_excludes_nulls() {
    let mask = cmp(CmpOp::Le, 0, Scalar::I32(1)).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(true), None, Some(false)]);
}

#[test]
fn cmp_gt_excludes_nulls() {
    let mask = cmp(CmpOp::Gt, 0, Scalar::I32(1)).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(false), None, Some(true)]);
}

#[test]
fn cmp_ge_excludes_nulls() {
    let mask = cmp(CmpOp::Ge, 0, Scalar::I32(3)).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(false), None, Some(true)]);
}

#[test]
fn cmp_eq_excludes_nulls() {
    let mask = cmp(CmpOp::Eq, 0, Scalar::I32(1)).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(true), None, Some(false)]);
}

#[test]
fn cmp_ne_excludes_nulls() {
    let mask = cmp(CmpOp::Ne, 0, Scalar::I32(1)).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(false), None, Some(true)]);
}

#[test]
fn cmp_eq_compares_int64() {
    let mask = cmp(CmpOp::Eq, 2, Scalar::I64(2)).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(false), Some(true), None]);
}

#[test]
fn cmp_gt_compares_float64_without_truncating() {
    // 1.0 > 0.5 is true; the literal is not truncated to an integer.
    let mask = cmp(CmpOp::Gt, 1, Scalar::F64(0.5)).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(true), Some(true), Some(true)]);
}

#[test]
fn cmp_eq_compares_strings() {
    let mask = cmp(CmpOp::Eq, 3, Scalar::Str("a".into()))
        .eval(&batch())
        .unwrap();
    assert_eq!(values(&mask), vec![Some(true), Some(false), Some(true)]);
}

#[test]
fn cmp_eq_compares_booleans() {
    let mask = cmp(CmpOp::Eq, 4, Scalar::Bool(false))
        .eval(&batch())
        .unwrap();
    assert_eq!(values(&mask), vec![Some(false), None, Some(true)]);
}

#[test]
fn cmp_eq_null_is_always_null() {
    // SQL `col = NULL` is unknown even when the column itself is null.
    let mask = cmp(CmpOp::Eq, 0, Scalar::Null).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![None, None, None]);
}

#[test]
fn is_null_matches_nulls() {
    let mask = Predicate::IsNull(0).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(false), Some(true), Some(false)]);
}

#[test]
fn is_not_null_matches_non_nulls() {
    let mask = Predicate::IsNotNull(0).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(true), Some(false), Some(true)]);
}

#[test]
fn and_kleene_keeps_unknown() {
    // NULL AND TRUE -> NULL; TRUE AND FALSE -> FALSE.
    let left = cmp(CmpOp::Gt, 0, Scalar::I32(0));
    let right = cmp(CmpOp::Lt, 2, Scalar::I64(3));
    let mask = Predicate::And(Box::new(left), Box::new(right))
        .eval(&batch())
        .unwrap();
    assert_eq!(values(&mask), vec![Some(true), None, None]);
}

#[test]
fn or_kleene_takes_true_over_unknown() {
    // NULL OR TRUE -> TRUE.
    let left = cmp(CmpOp::Gt, 0, Scalar::I32(0));
    let right = cmp(CmpOp::Lt, 2, Scalar::I64(3));
    let mask = Predicate::Or(Box::new(left), Box::new(right))
        .eval(&batch())
        .unwrap();
    assert_eq!(values(&mask), vec![Some(true), Some(true), Some(true)]);
}

#[test]
fn not_of_unknown_stays_unknown() {
    let inner = cmp(CmpOp::Gt, 0, Scalar::I32(0));
    let mask = Predicate::Not(Box::new(inner)).eval(&batch()).unwrap();
    assert_eq!(values(&mask), vec![Some(false), None, Some(false)]);
}

#[test]
fn out_of_range_column_is_unsupported() {
    assert!(cmp(CmpOp::Gt, 9, Scalar::I64(0)).eval(&batch()).is_err());
    assert!(Predicate::IsNull(9).eval(&batch()).is_err());
}
