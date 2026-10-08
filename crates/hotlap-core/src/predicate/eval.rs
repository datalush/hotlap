//! Arrow kernels backing [`Predicate::eval`]: comparisons and literals.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, Scalar as ArrowScalar,
    StringArray,
};
use arrow::compute::{cast, kernels::cmp};
use arrow::datatypes::DataType;

use crate::error::CoreError;
use crate::predicate::{CmpOp, Scalar};

/// Applies `column <op> scalar`, casting the literal to the column's type.
///
/// Arrow's comparison kernels return null for null inputs, which is exactly
/// SQL's three-valued comparison. A null literal therefore yields an all-null
/// mask rather than matching every row.
pub(super) fn cmp(
    column: &ArrayRef,
    op: CmpOp,
    scalar: &Scalar,
) -> Result<BooleanArray, CoreError> {
    let literal = ArrowScalar::new(literal(scalar, column.data_type())?);
    let out = match op {
        CmpOp::Eq => cmp::eq(column, &literal),
        CmpOp::Ne => cmp::neq(column, &literal),
        CmpOp::Lt => cmp::lt(column, &literal),
        CmpOp::Le => cmp::lt_eq(column, &literal),
        CmpOp::Gt => cmp::gt(column, &literal),
        CmpOp::Ge => cmp::gt_eq(column, &literal),
    }?;
    Ok(out)
}

/// Builds a length-one array holding `value`, cast to `data_type`.
fn literal(value: &Scalar, data_type: &DataType) -> Result<ArrayRef, CoreError> {
    let base: ArrayRef = match value {
        Scalar::Null => return Ok(arrow::array::new_null_array(data_type, 1)),
        Scalar::I64(v) => Arc::new(Int64Array::from(vec![*v])),
        Scalar::I32(v) => Arc::new(Int32Array::from(vec![*v])),
        Scalar::F64(v) => Arc::new(Float64Array::from(vec![*v])),
        Scalar::Str(s) => Arc::new(StringArray::from(vec![s.as_str()])),
        Scalar::Bool(b) => Arc::new(BooleanArray::from(vec![*b])),
    };
    if base.data_type() == data_type {
        Ok(base)
    } else {
        Ok(cast(base.as_ref(), data_type)?)
    }
}
