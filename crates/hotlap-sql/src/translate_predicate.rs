//! Translate a DataFusion filter expression into a kernel [`Predicate`].

use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::expr::BinaryExpr;
use datafusion::logical_expr::{Expr, Operator};
use hotlap::plan::{CmpOp, Predicate, Scalar};

use crate::error::SqlError;
use crate::translate_expr::column_index;

/// Translate a filter expression into an IR predicate.
///
/// Supports column/literal comparisons (`= <> < <= > >=`), `AND`/`OR`/`NOT`,
/// `IS NULL`/`IS NOT NULL` and the scalar literals the kernel can represent.
/// Everything else is rejected rather than silently dropped.
pub(crate) fn predicate(expr: &Expr, schema: &DFSchema) -> Result<Predicate, SqlError> {
    match expr {
        Expr::BinaryExpr(b) => binary(b, schema),
        Expr::Not(inner) => Ok(Predicate::Not(sub(inner, schema)?)),
        Expr::IsNull(inner) => Ok(Predicate::IsNull(column_index(inner, schema)?)),
        Expr::IsNotNull(inner) => Ok(Predicate::IsNotNull(column_index(inner, schema)?)),
        other => Err(unsupported("filter", other)),
    }
}

fn binary(b: &BinaryExpr, schema: &DFSchema) -> Result<Predicate, SqlError> {
    match b.op {
        Operator::And => Ok(Predicate::And(
            sub(&b.left, schema)?,
            sub(&b.right, schema)?,
        )),
        Operator::Or => Ok(Predicate::Or(sub(&b.left, schema)?, sub(&b.right, schema)?)),
        op => comparison(b, op, schema),
    }
}

/// Translates `column <op> literal`, accepting the literal on either side.
fn comparison(b: &BinaryExpr, op: Operator, schema: &DFSchema) -> Result<Predicate, SqlError> {
    let left_col = matches!(b.left.as_ref(), Expr::Column(_));
    let right_col = matches!(b.right.as_ref(), Expr::Column(_));
    let (col, lit, op) = match (left_col, right_col) {
        (true, false) => (&b.left, &b.right, cmp_op(op)?),
        // `literal < col` reverses the direction of the comparison.
        (false, true) => (&b.right, &b.left, flip(cmp_op(op)?)),
        _ => {
            return Err(SqlError::Unsupported(format!(
                "expected `column <op> literal`, got `{b}`"
            )));
        }
    };
    Ok(Predicate::Cmp {
        op,
        col: column_index(col, schema)?,
        scalar: scalar(lit)?,
    })
}

fn sub(expr: &Expr, schema: &DFSchema) -> Result<Box<Predicate>, SqlError> {
    Ok(Box::new(predicate(expr, schema)?))
}

fn cmp_op(op: Operator) -> Result<CmpOp, SqlError> {
    Ok(match op {
        Operator::Eq => CmpOp::Eq,
        Operator::NotEq => CmpOp::Ne,
        Operator::Lt => CmpOp::Lt,
        Operator::LtEq => CmpOp::Le,
        Operator::Gt => CmpOp::Gt,
        Operator::GtEq => CmpOp::Ge,
        other => {
            return Err(SqlError::Unsupported(format!(
                "unsupported predicate operator: {other:?}"
            )));
        }
    })
}

/// Mirrors a comparison so `literal < col` becomes `col > literal`.
fn flip(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
        CmpOp::Eq | CmpOp::Ne => op,
    }
}

/// Maps a literal expression to a kernel [`Scalar`].
pub(crate) fn scalar(expr: &Expr) -> Result<Scalar, SqlError> {
    let Expr::Literal(value, _) = expr else {
        return Err(unsupported("literal", expr));
    };
    Ok(match value {
        ScalarValue::Int64(Some(v)) => Scalar::I64(*v),
        ScalarValue::Int32(Some(v)) => Scalar::I32(*v),
        ScalarValue::Float64(Some(v)) => Scalar::F64(*v),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => Scalar::Str(v.clone()),
        ScalarValue::Boolean(Some(v)) => Scalar::Bool(*v),
        ScalarValue::Null
        | ScalarValue::Int64(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Float64(None) => Scalar::Null,
        other => {
            return Err(SqlError::Unsupported(format!(
                "unsupported literal `{other:?}`"
            )));
        }
    })
}

fn unsupported(what: &str, expr: &Expr) -> SqlError {
    SqlError::Unsupported(format!("unsupported {what}: `{expr}`"))
}
