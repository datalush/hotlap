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

/// Applies `column <op> scalar`, promoting both operands to a common type.
///
/// DataFusion leaves mixed-typed comparisons (an `Int64` column against a
/// `Float64` literal, say) uncast in the logical plan and resolves them during
/// physical planning by promoting both sides to a common supertype. Mirroring
/// that here avoids silently truncating a floating literal into an integer
/// column: `k > -1.5` compares as `Float64`, not as `k > -1`.
///
/// Arrow's comparison kernels return null for null inputs, which is exactly
/// SQL's three-valued comparison. A null literal therefore yields an all-null
/// mask rather than matching every row.
pub(super) fn cmp(
    column: &ArrayRef,
    op: CmpOp,
    scalar: &Scalar,
) -> Result<BooleanArray, CoreError> {
    let data_type = common_type(column.data_type(), scalar);
    let promoted;
    let column: &ArrayRef = if column.data_type() == &data_type {
        column
    } else {
        promoted = cast(column.as_ref(), &data_type)?;
        &promoted
    };
    let literal = ArrowScalar::new(literal(scalar, &data_type)?);
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

/// The common comparison type of a column and a literal.
///
/// Numeric operands widen to the wider of the two (`Int32` < `Int64` <
/// `Float64`), matching DataFusion's coercion. Everything else keeps the column
/// type and lets the literal cast fail if the pairing is incompatible; a null
/// literal likewise takes the column type since the result is null regardless.
fn common_type(column: &DataType, scalar: &Scalar) -> DataType {
    if let Scalar::Null = scalar {
        return column.clone();
    }
    match (numeric_rank(column), numeric_rank(&scalar_type(scalar))) {
        (Some(a), Some(b)) => match a.max(b) {
            3 => DataType::Float64,
            2 => DataType::Int64,
            _ => DataType::Int32,
        },
        _ => column.clone(),
    }
}

/// Widening rank of a numeric type: `Int32` < `Int64` < `Float64`.
fn numeric_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int32 => Some(1),
        DataType::Int64 => Some(2),
        DataType::Float64 => Some(3),
        _ => None,
    }
}

/// The Arrow type of a scalar literal.
fn scalar_type(scalar: &Scalar) -> DataType {
    match scalar {
        Scalar::Null => DataType::Null,
        Scalar::I32(_) => DataType::Int32,
        Scalar::I64(_) => DataType::Int64,
        Scalar::F64(_) => DataType::Float64,
        Scalar::Str(_) => DataType::Utf8,
        Scalar::Bool(_) => DataType::Boolean,
    }
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
